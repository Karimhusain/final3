import requests
import time
import json
import asyncio
import aiohttp
import websockets
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

# --- KONFIGURASI ---
# Sesuaikan parameter ini sesuai kebutuhan Anda.
PAIR = 'BTCUSDT'
# ====================================================================
# VVVVVV SARAN PENYESUAIAN KONFIGURASI VVVVVV
STEP = 100# Pembulatan harga untuk pengelompokan orderbook. Semakin kecil, semakin detail.
DEPTH = 1000 # Maksimal limit orderbook yang bisa diambil dari Binance API REST untuk initial snapshot.
             # Untuk WebSocket, ini lebih ke batas tampilan awal saja.

# Ambang batas (thresholds) untuk deteksi wall berdasarkan kuantitas (dalam unit PAIR, misal BTC)
WALL_THRESHOLD_MAIN = 50  # Kuantitas minimum untuk dianggap 'WALL' utama (🟥🟥🟥)
WALL_THRESHOLD_MINOR = 10   # Kuantitas minimum untuk dianggap 'Minor Wall' (dibawah WALL_THRESHOLD_MAIN)

# Ambang batas untuk deteksi spoofing
SPOOF_DETECTION_THRESHOLD_PERCENT = 0.5 # Fluktuasi > 50% dari rata-rata kuantitas
SPOOF_QUANTITY_MIN = 5 # Kuantitas minimum agar order bisa dianggap "spoofable"

# Toleransi stabilitas: jika kuantitas berubah lebih dari X% dari rata-rata, dianggap tidak stabil (Fake Wall)
STABILITY_TOLERANCE_PERCENT = 0.2 # Toleransi 20% perubahan kuantitas agar dianggap stabil
# ^^^^^^ AKHIR SARAN PENYESUAIAN KONFIGURASI ^^^^^^
# ====================================================================

MAX_DISPLAY_LINES = 30 # Jumlah baris orderbook yang akan ditampilkan di Discord (per sisi)

# Durasi pelacakan dan interval snapshot tetap relevan untuk analisis history dari OrderBookManager
TRACK_INTERVAL = 3  # detik, interval pengambilan snapshot internal dari OrderBookManager untuk analisis history
TRACK_DURATION = 60 # detik, total durasi pelacakan sebelum penutupan candle 15m

# GANTI DENGAN URL WEBHOOK DISCORD ANDA YANG VALID!
DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/1385219307608346634/-1sAEFdJ6V5rqFqFc7DSBhpIgqNXymoQOxsGERka-cplGJkcacYqGWlTk44BddYamOOz'

# --- KONFIGURASI LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- URL API BINANCE ---
SPOT_REST_URL = f'https://api.binance.com/api/v3/depth?symbol={PAIR}&limit={DEPTH}'
FUTURES_REST_URL = f'https://fapi.binance.com/fapi/v1/depth?symbol={PAIR}&limit={DEPTH}'

# URL WebSocket Binance
SPOT_WS_URL = f'wss://stream.binance.com:9443/ws/{PAIR.lower()}@depth' # @depth untuk full depth stream
FUTURES_WS_URL = f'wss://fstream.binance.com/ws/{PAIR.lower()}@depth' # @depth untuk full depth stream

# --- FUNGSI PEMBANTU ---
def get_price_step(price):
    """Membulatkan harga ke langkah (step) terdekat untuk pengelompokan."""
    return math.floor(float(price) / STEP) * STEP

def _get_label_from_analysis(avg_qty, is_stable, is_spoof):
    """Menentukan label untuk orderbook berdasarkan kuantitas rata-rata, stabilitas, dan deteksi spoof."""
    if is_spoof:
        return '⚠️ SPOOFING?'
    elif avg_qty >= WALL_THRESHOLD_MAIN:
        return '🟥🟥🟥 WALL' if is_stable else '⚠️ Fake Wall'
    elif avg_qty >= WALL_THRESHOLD_MINOR:
        return '(Minor Wall)' if is_stable else '(Minor Fake Wall)'
    return ''

# --- KELAS ORDERBOOK MANAGER (WebSocket) ---
class OrderBookManager:
    def __init__(self, symbol, rest_url, ws_url, is_futures=False):
        self.symbol = symbol
        self.rest_url = rest_url
        self.ws_url = ws_url
        self.is_futures = is_futures
        self.bids = {}  # {price: quantity}
        self.asks = {}  # {price: quantity}
        self.last_update_id = -1
        self.msg_queue = asyncio.Queue()
        self.processing_task = None
        self.ws_task = None
        self.reconnect_delay = 1 # detik
        self.snapshot_lock = asyncio.Lock() # Untuk memastikan konsistensi saat mengambil snapshot

    async def _fetch_initial_snapshot(self):
        """Mengambil snapshot awal orderbook via REST API."""
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(self.rest_url, timeout=10) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    self.bids = {float(price): float(qty) for price, qty in data['bids']}
                    self.asks = {float(price): float(qty) for price, qty in data['asks']}
                    self.last_update_id = data['lastUpdateId']
                    logging.info(f"[{self.symbol}] Initial REST snapshot fetched. lastUpdateId: {self.last_update_id}")
                    return True
            except aiohttp.ClientError as e:
                logging.error(f"[{self.symbol}] Gagal mengambil snapshot REST: {e}")
                return False
            except Exception as e:
                logging.error(f"[{self.symbol}] Error saat mengambil snapshot REST: {e}", exc_info=True)
                return False

    async def _handle_websocket_message(self, message):
        """Menambahkan pesan WebSocket ke antrian untuk diproses."""
        await self.msg_queue.put(message)

    async def _process_messages(self):
        """Memproses pesan dari antrian untuk memperbarui orderbook."""
        while True:
            msg = await self.msg_queue.get()
            data = json.loads(msg)
            
            # Binance full depth stream (diffDepth) format
            # 'u' is finalUpdateId in stream, 'U' is firstUpdateId in stream
            # For futures, it's 'T' for transaction time, 'E' for event time
            
            final_update_id = data.get('u') or data.get('lastUpdateId') # For futures, lastUpdateId is in the data itself
            first_update_id = data.get('U') or final_update_id # For futures, firstUpdateId is usually not provided for diffs, use final_update_id

            if not final_update_id: # skip if no update ID found
                continue

            async with self.snapshot_lock:
                if self.last_update_id == -1:
                    # Menunggu snapshot awal selesai
                    await asyncio.sleep(0.01) # Small delay to prevent busy-waiting
                    continue

                # Cek urutan update ID
                if final_update_id <= self.last_update_id:
                    # logging.debug(f"[{self.symbol}] Skipping old update: {final_update_id} <= {self.last_update_id}")
                    continue

                # Pastikan update pertama setelah snapshot sesuai (firstUpdateId > lastUpdateId dari snapshot)
                # Atau jika itu update berikutnya, pastikan urutannya benar
                if first_update_id > self.last_update_id + 1 and self.last_update_id != -1:
                    logging.warning(f"[{self.symbol}] Out of sync or missed messages. Re-syncing orderbook. First: {first_update_id}, Last local: {self.last_update_id}")
                    await self.stop()
                    await self.start() # Re-sync by restarting

                # Update bids
                for price_str, qty_str in data['b']:
                    price = float(price_str)
                    qty = float(qty_str)
                    if qty == 0:
                        self.bids.pop(price, None)
                    else:
                        self.bids[price] = qty

                # Update asks
                for price_str, qty_str in data['a']:
                    price = float(price_str)
                    qty = float(qty_str)
                    if qty == 0:
                        self.asks.pop(price, None)
                    else:
                        self.asks[price] = qty

                self.last_update_id = final_update_id
            # logging.debug(f"[{self.symbol}] Orderbook updated to {self.last_update_id}")

    async def _websocket_connect(self):
        """Membangun dan menjaga koneksi WebSocket."""
        while True:
            try:
                logging.info(f"[{self.symbol}] Menghubungkan ke WebSocket: {self.ws_url}")
                async with websockets.connect(self.ws_url, ping_interval=30, ping_timeout=10) as ws:
                    logging.info(f"[{self.symbol}] WebSocket terhubung.")
                    # Setelah terhubung, reset last_update_id untuk memicu snapshot ulang
                    self.last_update_id = -1 
                    if not await self._fetch_initial_snapshot():
                        logging.error(f"[{self.symbol}] Gagal ambil snapshot awal. Menutup WS dan coba lagi.")
                        await ws.close()
                        await asyncio.sleep(self.reconnect_delay)
                        continue
                    
                    logging.info(f"[{self.symbol}] WebSocket & REST snapshot sinkron. Mulai menerima pesan.")
                    
                    while True:
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=60) # Timeout untuk mendeteksi koneksi mati
                            await self._handle_websocket_message(message)
                        except asyncio.TimeoutError:
                            logging.warning(f"[{self.symbol}] WebSocket recv timeout, koneksi mungkin stagnan. Coba ping.")
                            await ws.ping() # Kirim ping manual
                        except websockets.exceptions.ConnectionClosedOK:
                            logging.info(f"[{self.symbol}] WebSocket connection closed normally.")
                            break # Break loop to reconnect
                        except websockets.exceptions.ConnectionClosedError as e:
                            logging.error(f"[{self.symbol}] WebSocket connection closed with error: {e}")
                            break # Break loop to reconnect
                        except Exception as e:
                            logging.error(f"[{self.symbol}] Error saat menerima pesan WS: {e}", exc_info=True)
                            break # Break loop to reconnect

            except websockets.exceptions.WebSocketException as e:
                logging.error(f"[{self.symbol}] WebSocket connection failed: {e}. Retrying in {self.reconnect_delay}s...")
            except Exception as e:
                logging.error(f"[{self.symbol}] Error umum di _websocket_connect: {e}", exc_info=True)

            await asyncio.sleep(self.reconnect_delay)
            self.reconnect_delay = min(self.reconnect_delay * 2, 60) # Backoff delay, max 60s

    async def start(self):
        """Memulai manajemen orderbook."""
        if not self.ws_task:
            self.ws_task = asyncio.create_task(self._websocket_connect())
        if not self.processing_task:
            self.processing_task = asyncio.create_task(self._process_messages())

    async def stop(self):
        """Menghentikan manajemen orderbook."""
        if self.ws_task:
            self.ws_task.cancel()
            await asyncio.gather(self.ws_task, return_exceptions=True) # Ensure task is cancelled
            self.ws_task = None
        if self.processing_task:
            self.processing_task.cancel()
            await asyncio.gather(self.processing_task, return_exceptions=True) # Ensure task is cancelled
            self.processing_task = None
        self.bids = {}
        self.asks = {}
        self.last_update_id = -1
        # Clear the queue to prevent stale messages on reconnect
        while not self.msg_queue.empty():
            try:
                self.msg_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        logging.info(f"[{self.symbol}] OrderBookManager stopped.")


    async def get_current_orderbook(self):
        """Mengembalikan snapshot orderbook saat ini."""
        async with self.snapshot_lock:
            # Mengembalikan salinan untuk menghindari modifikasi saat iterasi
            bids_copy = {p: q for p, q in self.bids.items() if q > 0}
            asks_copy = {p: q for p, q in self.asks.items() if q > 0}
            return bids_copy, asks_copy

# --- FUNGSI ANALISIS & FORMATTING (Diperbarui untuk Volume Kumulatif) ---
async def track_and_analyze_orderbook_websocket(orderbook_manager):
    """
    Melacak dan mengumpulkan data orderbook dari WebSocket manager selama DURASI_PELACAKAN.
    Mengembalikan riwayat orderbook yang sudah dikelompokkan dan jumlah snapshot.
    """
    logging.info(f"[INFO] Melacak orderbook WebSocket untuk {orderbook_manager.symbol} selama {TRACK_DURATION} detik...")

    # Struktur: {price_step: {'buy': [qty1, qty2, ...], 'sell': [qty1, qty2, ...]}}
    orderbook_history = defaultdict(lambda: {'buy': deque(maxlen=20), 'sell': deque(maxlen=20)}) # Deque for efficiency
    end_time = time.time() + TRACK_DURATION
    snapshot_count = 0

    while time.time() < end_time:
        current_bids, current_asks = await orderbook_manager.get_current_orderbook()
        
        if not current_bids and not current_asks:
            logging.warning(f"[{orderbook_manager.symbol}] Tidak ada data orderbook dari manager pada snapshot ini. Menunggu...")
            await asyncio.sleep(TRACK_INTERVAL)
            continue
        
        snapshot_count += 1

        # Proses Bids (order beli)
        for bid_price, bid_qty in current_bids.items():
            price_step = get_price_step(bid_price)
            orderbook_history[price_step]['buy'].append(bid_qty)

        # Proses Asks (order jual)
        for ask_price, ask_qty in current_asks.items():
            price_step = get_price_step(ask_price)
            orderbook_history[price_step]['sell'].append(ask_qty)
        
        await asyncio.sleep(TRACK_INTERVAL)
    
    logging.info(f"[INFO] Pelacakan selesai untuk {orderbook_manager.symbol}. Total snapshot: {snapshot_count}")
    
    # Konversi deque ke list untuk konsistensi dengan analyze_full_orderbook
    final_history = {}
    for price_step, data in orderbook_history.items():
        final_history[price_step] = {'buy': list(data['buy']), 'sell': list(data['sell'])}
        
    return final_history, snapshot_count

def analyze_full_orderbook(orderbook_history, snapshot_count, is_futures=False):
    """
    Menganalisis riwayat orderbook dari WebSocket untuk mengidentifikasi wall, spoofing, imbalance,
    dan menghitung volume kumulatif.
    """
    analyzed_buy_levels = []
    analyzed_sell_levels = []
    total_buy_qty_overall = 0
    total_sell_qty_overall = 0

    # Mengurutkan harga secara numerik untuk pemrosesan yang benar
    sorted_prices = sorted(orderbook_history.keys())

    # Proses Bids (secara terbalik untuk kumulatif dari harga tertinggi ke terendah)
    cumulative_buy_qty = 0
    for price_step in reversed(sorted_prices):
        buys = orderbook_history[price_step].get('buy', [])
        if buys:
            avg_qty = sum(buys) / len(buys)
            is_stable = (len(buys) < 2) or all(abs(q - avg_qty) < avg_qty * STABILITY_TOLERANCE_PERCENT for q in buys)
            is_spoof = False
            if avg_qty >= SPOOF_QUANTITY_MIN and snapshot_count > 1:
                min_qty = min(buys)
                max_qty = max(buys)
                if avg_qty > 0 and (max_qty - min_qty) / avg_qty > SPOOF_DETECTION_THRESHOLD_PERCENT:
                    is_spoof = True
                elif all(q < SPOOF_QUANTITY_MIN * 0.1 for q in buys[:-1]) and buys[-1] >= SPOOF_QUANTITY_MIN:
                    is_spoof = True

            label = _get_label_from_analysis(avg_qty, is_stable, is_spoof)
            cumulative_buy_qty += avg_qty # Akumulasi volume
            analyzed_buy_levels.insert(0, {'price': price_step, 'qty': avg_qty, 'cumulative_qty': cumulative_buy_qty, 'label': label, 'is_wall_or_spoof': bool(label)})
            total_buy_qty_overall += avg_qty

    # Proses Asks (secara normal untuk kumulatif dari harga terendah ke tertinggi)
    cumulative_sell_qty = 0
    for price_step in sorted_prices:
        sells = orderbook_history[price_step].get('sell', [])
        if sells:
            avg_qty = sum(sells) / len(sells)
            is_stable = (len(sells) < 2) or all(abs(q - avg_qty) < avg_qty * STABILITY_TOLERANCE_PERCENT for q in sells)
            is_spoof = False
            if avg_qty >= SPOOF_QUANTITY_MIN and snapshot_count > 1:
                min_qty = min(sells)
                max_qty = max(sells)
                if avg_qty > 0 and (max_qty - min_qty) / avg_qty > SPOOF_DETECTION_THRESHOLD_PERCENT:
                    is_spoof = True
                elif all(q < SPOOF_QUANTITY_MIN * 0.1 for q in sells[:-1]) and sells[-1] >= SPOOF_QUANTITY_MIN:
                    is_spoof = True

            label = _get_label_from_analysis(avg_qty, is_stable, is_spoof)
            cumulative_sell_qty += avg_qty # Akumulasi volume
            analyzed_sell_levels.append({'price': price_step, 'qty': avg_qty, 'cumulative_qty': cumulative_sell_qty, 'label': label, 'is_wall_or_spoof': bool(label)})
            total_sell_qty_overall += avg_qty

    # Menentukan Imbalance global
    imbalance = 'BALANCED'
    total_relevant_buy_qty = sum(item['qty'] for item in analyzed_buy_levels if item['is_wall_or_spoof'])
    total_relevant_sell_qty = sum(item['qty'] for item in analyzed_sell_levels if item['is_wall_or_spoof'])

    imbalance_detail = ""
    if total_relevant_buy_qty > 0 or total_relevant_sell_qty > 0:
        imbalance_detail = f" (Total Buy Wall/Spoof: {total_relevant_buy_qty:,.2f} BTC vs Total Sell Wall/Spoof: {total_relevant_sell_qty:,.2f} BTC)"

    if total_buy_qty_overall > 0 and total_sell_qty_overall > 0:
        if total_buy_qty_overall / total_sell_qty_overall >= 1.2:
            imbalance = 'BUY DOMINANT'
        elif total_sell_qty_overall / total_buy_qty_overall >= 1.2:
            imbalance = 'SELL DOMINANT'
    elif total_buy_qty_overall > 0:
        imbalance = 'BUY DOMINANT (No significant sell volume)'
    elif total_sell_qty_overall > 0:
        imbalance = 'SELL DOMINANT (No significant buy volume)'
    else:
        imbalance = 'BALANCED (No significant orderbook activity detected)'
    
    imbalance += imbalance_detail


    return analyzed_buy_levels, analyzed_sell_levels, imbalance

def format_full_orderbook_output(levels, max_lines, side):
    """Memformat daftar level orderbook (termasuk non-wall) dengan volume kumulatif untuk output Discord."""
    if not levels:
        return f"Tidak ada data { 'beli' if side == 'buy' else 'jual' } orderbook yang cukup."

    # Urutkan berdasarkan harga: naik untuk jual, turun untuk beli (agar yang terdekat ke harga saat ini di atas)
    sorted_levels = sorted(levels, key=lambda x: x['price'], reverse=(side == 'buy'))
    output_lines = ["Price → Individual Qty | Cumulative Qty | Label"] # Header baru
    
    # Ambil hanya sejumlah baris yang ditentukan MAX_DISPLAY_LINES
    for item in sorted_levels[:max_lines]:
        label_text = f" {item['label']}" if item['label'] else ""
        output_lines.append(f"{item['price']:,.0f} → {item['qty']:,.2f} BTC | {item['cumulative_qty']:,.2f} BTC{label_text}")

    return "\n".join(output_lines)

def send_to_discord(content):
    """Mengirim pesan ke Discord Webhook."""
    payload = {"content": content}
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(payload),
                             headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        logging.info(f"[INFO] Pesan berhasil dikirim ke Discord. Status: {resp.status_code}")
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"[ERROR - Discord HTTP] Gagal mengirim ke Discord: {http_err}. Response: {http_err.response.text}")
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"[ERROR - Discord Connection] Gagal terhubung ke Discord: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f"[ERROR - Discord Timeout] Waktu habis saat mengirim ke Discord: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"[ERROR - Discord Request] Gagal mengirim ke Discord: {req_err}")
    except Exception as e:
        logging.error(f"[ERROR - Discord Unhandled] Error tak terduga saat mengirim ke Discord: {e}", exc_info=True)

def wait_until_pre_close():
    """Menunggu hingga 1 menit sebelum penutupan candle 15 menit berikutnya."""
    now_utc = datetime.utcnow()
    
    current_quarter = now_utc.minute // 15
    next_quarter_minute = (current_quarter + 1) * 15
    
    if next_quarter_minute == 60:
        next_candle_utc = (now_utc + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    else:
        next_candle_utc = now_utc.replace(minute=next_quarter_minute, second=0, microsecond=0)

    pre_close_time_utc = next_candle_utc - timedelta(minutes=1)

    logging.info(f"[INFO] Pelacakan berikutnya akan dimulai pada: {pre_close_time_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC (1 menit sebelum {next_candle_utc.strftime('%H:%M:%S')} UTC)")

    while True:
        now_utc = datetime.utcnow()
        if now_utc >= pre_close_time_utc:
            logging.info(f"[INFO] Waktu pelacakan dimulai: {datetime.now().strftime('%H:%M:%S')}")
            return next_candle_utc
        time.sleep(5)

# --- LOOP UTAMA ---
async def main():
    logging.info(f"=== {PAIR} ORDERBOOK WALL DETECTOR (SPOT + FUTURES) DIMULAI ===")

    wib_tz = timezone(timedelta(hours=7))

    # Inisialisasi OrderBookManager untuk SPOT dan FUTURES
    spot_ob_manager = OrderBookManager(PAIR + ' (Spot)', SPOT_REST_URL, SPOT_WS_URL)
    futures_ob_manager = OrderBookManager(PAIR + ' (Futures)', FUTURES_REST_URL, FUTURES_WS_URL, is_futures=True)

    # Mulai koneksi WebSocket (akan berjalan di background)
    await spot_ob_manager.start()
    await futures_ob_manager.start()

    # Beri waktu sebentar agar WebSocket terhubung dan mengambil snapshot awal
    logging.info("Memberi waktu 5 detik agar WebSocket terhubung dan sync initial snapshot...")
    await asyncio.sleep(5)

    while True:
        next_close_time_utc = wait_until_pre_close()

        # Gunakan data dari OrderBookManager yang real-time
        spot_history, spot_snapshot_count = await track_and_analyze_orderbook_websocket(spot_ob_manager)
        futures_history, futures_snapshot_count = await track_and_analyze_orderbook_websocket(futures_ob_manager)

        spot_buy, spot_sell, spot_imbalance = analyze_full_orderbook(spot_history, spot_snapshot_count)
        futures_buy, futures_sell, futures_imbalance = analyze_full_orderbook(futures_history, futures_snapshot_count, is_futures=True)

        close_time_wib = next_close_time_utc.astimezone(wib_tz)
        close_time_str = close_time_wib.strftime('%Y-%m-%d %H:%M WIB')

        content = f"""
📊 **{PAIR} — DETEKSI ORDERBOOK WALL** ⏰ Waktu: {close_time_str} (Penutupan Lilin 15m)  
Pelacakan: {TRACK_DURATION} detik (sebelum penutupan), Real-time via WebSocket  

---

### === ORDERBOOK SPOT ===

**__**[ BUY ORDERS — {MAX_DISPLAY_LINES} LEVEL TERDEKAT ]**__**
{format_full_orderbook_output(spot_buy, MAX_DISPLAY_LINES, 'buy')}

**__**[ SELL ORDERS — {MAX_DISPLAY_LINES} LEVEL TERDEKAT ]**__**
{format_full_orderbook_output(spot_sell, MAX_DISPLAY_LINES, 'sell')}

**IMBALANCE:** {spot_imbalance}

---

### === ORDERBOOK FUTURES ===

**__**[ BUY ORDERS — {MAX_DISPLAY_LINES} LEVEL TERDEKAT ]**__**
{format_full_orderbook_output(futures_buy, MAX_DISPLAY_LINES, 'buy')}

**__**[ SELL ORDERS — {MAX_DISPLAY_LINES} LEVEL TERDEKAT ]**__**
{format_full_orderbook_output(futures_sell, MAX_DISPLAY_LINES, 'sell')}

**IMBALANCE:** {futures_imbalance}
"""
        send_to_discord(content)
        logging.info(f"[INFO] Siklus deteksi selesai. Menunggu siklus berikutnya...\n")
        
        now_after_send = datetime.utcnow()
        next_tracking_start_utc = (next_close_time_utc + timedelta(minutes=15)) - timedelta(minutes=1)
        sleep_duration_seconds = (next_tracking_start_utc - now_after_send).total_seconds()

        if sleep_duration_seconds > 0:
            logging.info(f"[INFO] Tidur selama {int(sleep_duration_seconds)} detik sampai waktu pelacakan berikutnya.")
            await asyncio.sleep(sleep_duration_seconds)
        else:
            logging.warning("[WARNING] Waktu tidur negatif atau nol. Tidur 60 detik sebagai fallback.")
            await asyncio.sleep(60)

if __name__ == '__main__':
    if DISCORD_WEBHOOK_URL == 'https://discord.com/api/webhooks/YOUR_DISCORD_WEBHOOK_URL_HERE':
        logging.error("PENTING: Harap ganti 'https://discord.com/api/webhooks/YOUR_DISCORD_WEBHOOK_URL_HERE' dengan URL webhook Discord Anda yang valid di bagian KONFIGURASI!")
        exit()
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot dihentikan oleh pengguna.")
        # Lakukan cleanup jika diperlukan, misal stop OrderBookManager
    except Exception as e:
        logging.error(f"Error fatal di main loop: {e}", exc_info=True)

