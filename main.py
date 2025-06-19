import requests
import time
import json
import asyncio
import aiohttp
import websockets
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
import math

# --- KONFIGURASI ---
# Sesuaikan parameter ini sesuai kebutuhan Anda.
PAIR = 'BTCUSDT'
STEP = 10 # Pembulatan harga untuk pengelompokan orderbook. Semakin kecil, semakin detail.
DEPTH = 1000 # Maksimal limit orderbook yang bisa diambil dari Binance API REST untuk initial snapshot.

WALL_THRESHOLD_MAIN = 50
WALL_THRESHOLD_MINOR = 10

SPOOF_DETECTION_THRESHOLD_PERCENT = 0.5
SPOOF_QUANTITY_MIN = 5

STABILITY_TOLERANCE_PERCENT = 0.2

MAX_DISPLAY_LINES = 30

TRACK_INTERVAL = 3
TRACK_DURATION = 60

DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/1385219307608346634/-1sAEFdJ6V5rqFqFc7DSBhpIgqNXymoQOxsGERka-cplGJkcacYqGWlTk44BddYamOOz'

# --- KONFIGURASI LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- URL API BINANCE ---
SPOT_REST_URL = f'https://api.binance.com/api/v3/depth?symbol={PAIR}&limit={DEPTH}'
FUTURES_REST_URL = f'https://fapi.binance.com/fapi/v1/depth?symbol={PAIR}&limit={DEPTH}'

SPOT_WS_URL = f'wss://stream.binance.com:9443/ws/{PAIR.lower()}@depth'
FUTURES_WS_URL = f'wss://fstream.binance.com/ws/{PAIR.lower()}@depth'

# --- FUNGSI PEMBANTU ---
def get_price_step(price):
    return math.floor(float(price) / STEP) * STEP

def _get_label_from_analysis(avg_qty, is_stable, is_spoof):
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
        self.bids = {}
        self.asks = {}
        self.last_update_id = -1
        self.msg_queue = asyncio.Queue()
        self.processing_task = None
        self.ws_task = None
        self.reconnect_delay = 1
        self.snapshot_lock = asyncio.Lock()
        self.is_synced = False # Status baru untuk menunjukkan apakah orderbook sudah sinkron

    async def _fetch_initial_snapshot(self):
        async with aiohttp.ClientSession() as session:
            try:
                logging.info(f"[{self.symbol}] Mengambil snapshot REST awal...")
                async with session.get(self.rest_url, timeout=10) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    self.bids = {float(price): float(qty) for price, qty in data['bids']}
                    self.asks = {float(price): float(qty) for price, qty in data['asks']}
                    self.last_update_id = data['lastUpdateId']
                    logging.info(f"[{self.symbol}] Snapshot REST berhasil diambil. lastUpdateId: {self.last_update_id}. Jumlah bids: {len(self.bids)}, asks: {len(self.asks)}")
                    self.is_synced = True # Set status sinkron
                    return True
            except aiohttp.ClientError as e:
                logging.error(f"[{self.symbol}] Gagal mengambil snapshot REST: {e}")
                self.is_synced = False
                return False
            except Exception as e:
                logging.error(f"[{self.symbol}] Error saat mengambil snapshot REST: {e}", exc_info=True)
                self.is_synced = False
                return False

    async def _handle_websocket_message(self, message):
        await self.msg_queue.put(message)

    async def _process_messages(self):
        while True:
            try:
                msg = await self.msg_queue.get()
                data = json.loads(msg)
                
                final_update_id = data.get('u') or data.get('lastUpdateId')
                first_update_id = data.get('U') or final_update_id

                if not final_update_id:
                    continue

                async with self.snapshot_lock:
                    if not self.is_synced:
                        # Menunggu snapshot awal selesai dan disinkronkan
                        # logging.debug(f"[{self.symbol}] Menunggu sinkronisasi orderbook...")
                        await asyncio.sleep(0.001) # Hindari busy-waiting
                        continue

                    if final_update_id <= self.last_update_id:
                        # logging.debug(f"[{self.symbol}] Skipping old update: {final_update_id} <= {self.last_update_id}")
                        continue

                    # Perbaikan di sini: Cek urutan hanya jika ini bukan pesan pertama setelah sync awal
                    # Jika ini pesan pertama setelah sync, first_update_id harus > last_update_id
                    if first_update_id > self.last_update_id + 1:
                        logging.warning(f"[{self.symbol}] Out of sync or missed messages. Re-syncing orderbook. "
                                        f"First WS ID: {first_update_id}, Last local ID: {self.last_update_id}")
                        await self.stop()
                        await self.start() # Re-sync by restarting
                        continue # Skip processing this out-of-sync message

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
            except json.JSONDecodeError:
                logging.error(f"[{self.symbol}] Gagal mendecode JSON dari pesan WebSocket: {msg[:200]}...")
            except Exception as e:
                logging.error(f"[{self.symbol}] Error saat memproses pesan WebSocket: {e}", exc_info=True)


    async def _websocket_connect(self):
        while True:
            try:
                logging.info(f"[{self.symbol}] Menghubungkan ke WebSocket: {self.ws_url}")
                async with websockets.connect(self.ws_url, ping_interval=30, ping_timeout=10) as ws:
                    logging.info(f"[{self.symbol}] WebSocket terhubung.")
                    self.last_update_id = -1 # Reset ID untuk memicu snapshot ulang
                    self.is_synced = False # Reset status sync
                    
                    if not await self._fetch_initial_snapshot():
                        logging.error(f"[{self.symbol}] Gagal ambil snapshot awal. Menutup WS dan coba lagi.")
                        await ws.close()
                        await asyncio.sleep(self.reconnect_delay)
                        continue
                    
                    logging.info(f"[{self.symbol}] WebSocket & REST snapshot sinkron. Mulai menerima pesan.")
                    self.reconnect_delay = 1 # Reset delay setelah sukses

                    while True:
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=60)
                            await self._handle_websocket_message(message)
                        except asyncio.TimeoutError:
                            logging.warning(f"[{self.symbol}] WebSocket recv timeout, koneksi mungkin stagnan. Coba ping.")
                            await ws.ping()
                        except websockets.exceptions.ConnectionClosedOK:
                            logging.info(f"[{self.symbol}] WebSocket connection closed normally.")
                            break
                        except websockets.exceptions.ConnectionClosedError as e:
                            logging.error(f"[{self.symbol}] WebSocket connection closed with error: {e}")
                            break
                        except Exception as e:
                            logging.error(f"[{self.symbol}] Error saat menerima pesan WS: {e}", exc_info=True)
                            break

            except websockets.exceptions.WebSocketException as e:
                logging.error(f"[{self.symbol}] WebSocket connection failed: {e}. Retrying in {self.reconnect_delay}s...")
            except Exception as e:
                logging.error(f"[{self.symbol}] Error umum di _websocket_connect: {e}", exc_info=True)

            await asyncio.sleep(self.reconnect_delay)
            self.reconnect_delay = min(self.reconnect_delay * 2, 60)

    async def start(self):
        if not self.ws_task:
            self.ws_task = asyncio.create_task(self._websocket_connect())
        if not self.processing_task:
            self.processing_task = asyncio.create_task(self._process_messages())
        logging.info(f"[{self.symbol}] OrderBookManager tasks started.")


    async def stop(self):
        if self.ws_task:
            self.ws_task.cancel()
            await asyncio.gather(self.ws_task, return_exceptions=True)
            self.ws_task = None
        if self.processing_task:
            self.processing_task.cancel()
            await asyncio.gather(self.processing_task, return_exceptions=True)
            self.processing_task = None
        self.bids = {}
        self.asks = {}
        self.last_update_id = -1
        self.is_synced = False
        while not self.msg_queue.empty():
            try:
                self.msg_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        logging.info(f"[{self.symbol}] OrderBookManager stopped.")

    async def get_current_orderbook(self):
        async with self.snapshot_lock:
            # Mengembalikan salinan untuk menghindari modifikasi saat iterasi
            bids_copy = {p: q for p, q in self.bids.items() if q > 0}
            asks_copy = {p: q for p, q in self.asks.items() if q > 0}
            return bids_copy, asks_copy

# --- FUNGSI ANALISIS & FORMATTING (Diperbarui untuk Volume Kumulatif) ---
async def track_and_analyze_orderbook_websocket(orderbook_manager):
    logging.info(f"[INFO] Melacak orderbook WebSocket untuk {orderbook_manager.symbol} selama {TRACK_DURATION} detik...")

    orderbook_history = defaultdict(lambda: {'buy': deque(maxlen=int(TRACK_DURATION/TRACK_INTERVAL)) if TRACK_INTERVAL > 0 else deque(maxlen=1),
                                             'sell': deque(maxlen=int(TRACK_DURATION/TRACK_INTERVAL)) if TRACK_INTERVAL > 0 else deque(maxlen=1)})
    end_time = time.time() + TRACK_DURATION
    snapshot_count = 0

    while time.time() < end_time:
        current_bids, current_asks = await orderbook_manager.get_current_orderbook()
        
        # Penanganan jika OrderBookManager belum sinkron atau tidak ada data
        if not orderbook_manager.is_synced:
            logging.warning(f"[{orderbook_manager.symbol}] OrderBookManager belum sinkron. Menunda pelacakan snapshot...")
            await asyncio.sleep(TRACK_INTERVAL)
            continue
        
        if not current_bids and not current_asks:
            logging.warning(f"[{orderbook_manager.symbol}] Tidak ada data orderbook dari manager pada snapshot ini. Melanjutkan...")
            # Tetap tambahkan snapshot untuk menghitung waktu, walaupun datanya kosong
            # atau bisa skip snapshot_count jika tidak mau hitung snapshot kosong
            await asyncio.sleep(TRACK_INTERVAL)
            continue
        
        snapshot_count += 1

        for bid_price, bid_qty in current_bids.items():
            price_step = get_price_step(bid_price)
            orderbook_history[price_step]['buy'].append(bid_qty)

        for ask_price, ask_qty in current_asks.items():
            price_step = get_price_step(ask_price)
            orderbook_history[price_step]['sell'].append(ask_qty)
        
        await asyncio.sleep(TRACK_INTERVAL)
    
    logging.info(f"[INFO] Pelacakan selesai untuk {orderbook_manager.symbol}. Total snapshot: {snapshot_count}")
    
    final_history = {}
    for price_step, data in orderbook_history.items():
        # Pastikan kita hanya mengambil data yang ada untuk analisis
        final_history[price_step] = {
            'buy': list(data['buy']) if data['buy'] else [],
            'sell': list(data['sell']) if data['sell'] else []
        }
        
    return final_history, snapshot_count

def analyze_full_orderbook(orderbook_history, snapshot_count, is_futures=False):
    analyzed_buy_levels = []
    analyzed_sell_levels = []
    total_buy_qty_overall = 0
    total_sell_qty_overall = 0

    if not orderbook_history:
        logging.warning("[ANALYSIS] orderbook_history kosong, tidak ada analisis yang dilakukan.")
        return [], [], "BALANCED (No data to analyze)"

    sorted_prices = sorted(orderbook_history.keys())

    # Proses Bids
    cumulative_buy_qty = 0
    # Iterasi dari harga tertinggi ke terendah untuk sisi beli
    for price_step in sorted(sorted_prices, reverse=True): 
        buys = orderbook_history[price_step].get('buy', [])
        if buys:
            # Perbaiki pembagian nol jika buys kosong
            avg_qty = sum(buys) / len(buys) if buys else 0
            
            # Pastikan ada cukup data snapshot untuk stabilitas/spoofing
            is_stable = True
            is_spoof = False

            if len(buys) >= 1: # Setidaknya ada 1 snapshot
                # Cek stabilitas hanya jika ada 2 atau lebih snapshot
                if len(buys) >= 2:
                    is_stable = all(abs(q - avg_qty) < avg_qty * STABILITY_TOLERANCE_PERCENT for q in buys)
                else: # Jika hanya 1 snapshot, dianggap stabil karena tidak ada fluktuasi
                    is_stable = True 

                # Cek spoofing hanya jika ada 2 atau lebih snapshot dan memenuhi min qty
                if avg_qty >= SPOOF_QUANTITY_MIN and len(buys) >= 2:
                    min_qty = min(buys)
                    max_qty = max(buys)
                    if avg_qty > 0 and (max_qty - min_qty) / avg_qty > SPOOF_DETECTION_THRESHOLD_PERCENT:
                        is_spoof = True
                    # Logika ini butuh minimal 2 snapshot, jika hanya 1, is_spoof tetap False
                    elif all(q < SPOOF_QUANTITY_MIN * 0.1 for q in list(buys)[:-1]) and list(buys)[-1] >= SPOOF_QUANTITY_MIN: # konversi ke list untuk indexing
                        is_spoof = True
            
            label = _get_label_from_analysis(avg_qty, is_stable, is_spoof)
            cumulative_buy_qty += avg_qty
            analyzed_buy_levels.append({'price': price_step, 'qty': avg_qty, 'cumulative_qty': cumulative_buy_qty, 'label': label, 'is_wall_or_spoof': bool(label)})
            total_buy_qty_overall += avg_qty
    
    # Sortir ulang hasil buy_levels agar dari harga tertinggi ke terendah
    analyzed_buy_levels.sort(key=lambda x: x['price'], reverse=True)


    # Proses Asks
    cumulative_sell_qty = 0
    # Iterasi dari harga terendah ke tertinggi untuk sisi jual
    for price_step in sorted_prices:
        sells = orderbook_history[price_step].get('sell', [])
        if sells:
            # Perbaiki pembagian nol jika sells kosong
            avg_qty = sum(sells) / len(sells) if sells else 0

            is_stable = True
            is_spoof = False

            if len(sells) >= 1: # Setidaknya ada 1 snapshot
                if len(sells) >= 2:
                    is_stable = all(abs(q - avg_qty) < avg_qty * STABILITY_TOLERANCE_PERCENT for q in sells)
                else:
                    is_stable = True

                if avg_qty >= SPOOF_QUANTITY_MIN and len(sells) >= 2:
                    min_qty = min(sells)
                    max_qty = max(sells)
                    if avg_qty > 0 and (max_qty - min_qty) / avg_qty > SPOOF_DETECTION_THRESHOLD_PERCENT:
                        is_spoof = True
                    elif all(q < SPOOF_QUANTITY_MIN * 0.1 for q in list(sells)[:-1]) and list(sells)[-1] >= SPOOF_QUANTITY_MIN:
                        is_spoof = True
            
            label = _get_label_from_analysis(avg_qty, is_stable, is_spoof)
            cumulative_sell_qty += avg_qty
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
    if not levels:
        return f"Tidak ada data { 'beli' if side == 'buy' else 'jual' } orderbook yang cukup."

    sorted_levels = sorted(levels, key=lambda x: x['price'], reverse=(side == 'buy'))
    output_lines = ["Price → Individual Qty | Cumulative Qty | Label"]
    
    for item in sorted_levels[:max_lines]:
        label_text = f" {item['label']}" if item['label'] else ""
        output_lines.append(f"{item['price']:,.0f} → {item['qty']:,.2f} BTC | {item['cumulative_qty']:,.2f} BTC{label_text}")

    return "\n".join(output_lines)

def send_to_discord(content):
    payload = {"content": content}
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(payload),
                             headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        logging.info(f"[INFO] Pesan berhasil dikirim ke Discord. Status: {resp.status_code}")
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"[ERROR - Discord HTTP] Gagal mengirim ke Discord: {http_err}. Response: {http_err.response.text}")
        if http_err.response.status_code == 400:
            logging.error("Pesan terlalu panjang atau ada masalah format. Coba kurangi MAX_DISPLAY_LINES atau periksa karakter khusus.")
        elif http_err.response.status_code == 401 or http_err.response.status_code == 403:
            logging.error("URL Webhook Discord tidak valid atau tidak memiliki izin yang benar.")
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"[ERROR - Discord Connection] Gagal terhubung ke Discord: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f"[ERROR - Discord Timeout] Waktu habis saat mengirim ke Discord: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"[ERROR - Discord Request] Gagal mengirim ke Discord: {req_err}")
    except Exception as e:
        logging.error(f"[ERROR - Discord Unhandled] Error tak terduga saat mengirim ke Discord: {e}", exc_info=True)

def wait_until_pre_close():
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

    spot_ob_manager = OrderBookManager(PAIR + ' (Spot)', SPOT_REST_URL, SPOT_WS_URL)
    futures_ob_manager = OrderBookManager(PAIR + ' (Futures)', FUTURES_REST_URL, FUTURES_WS_URL, is_futures=True)

    await spot_ob_manager.start()
    await futures_ob_manager.start()

    logging.info("Memberi waktu 10 detik agar WebSocket terhubung dan sync initial snapshot...")
    await asyncio.sleep(10) # Beri waktu lebih lama untuk koneksi awal dan sinkronisasi

    while True:
        try:
            next_close_time_utc = wait_until_pre_close()

            # Pastikan OrderBookManager sudah sinkron sebelum melacak
            if not spot_ob_manager.is_synced:
                logging.warning("[MAIN] Spot OrderBookManager belum sinkron. Menunggu...")
                await spot_ob_manager.start() # Coba start ulang jika belum sinkron
                await asyncio.sleep(5)
                continue # Langsung ke siklus berikutnya

            if not futures_ob_manager.is_synced:
                logging.warning("[MAIN] Futures OrderBookManager belum sinkron. Menunggu...")
                await futures_ob_manager.start() # Coba start ulang jika belum sinkron
                await asyncio.sleep(5)
                continue # Langsung ke siklus berikutnya


            spot_history, spot_snapshot_count = await track_and_analyze_orderbook_websocket(spot_ob_manager)
            futures_history, futures_snapshot_count = await track_and_analyze_orderbook_websocket(futures_ob_manager)

            # Validasi data yang terkumpul sebelum analisis
            if not spot_history:
                logging.warning("[MAIN] Tidak ada data riwayat spot yang terkumpul. Melewatkan analisis spot.")
                spot_buy, spot_sell, spot_imbalance = [], [], "BALANCED (No data collected)"
            else:
                spot_buy, spot_sell, spot_imbalance = analyze_full_orderbook(spot_history, spot_snapshot_count)
            
            if not futures_history:
                logging.warning("[MAIN] Tidak ada data riwayat futures yang terkumpul. Melewatkan analisis futures.")
                futures_buy, futures_sell, futures_imbalance = [], [], "BALANCED (No data collected)"
            else:
                futures_buy, futures_sell, futures_imbalance = analyze_full_orderbook(futures_history, futures_snapshot_count, is_futures=True)

            close_time_wib = next_close_time_utc.astimezone(wib_tz)
            close_time_str = close_time_wib.strftime('%Y-%m-%d %H:%M WIB')

            # Log ringkasan sebelum mengirim ke Discord
            logging.info(f"[MAIN] Siap mengirim output untuk {PAIR} pada {close_time_str}.")
            logging.info(f"[MAIN] Spot Buy Levels: {len(spot_buy)}, Sell Levels: {len(spot_sell)}, Imbalance: {spot_imbalance}")
            logging.info(f"[MAIN] Futures Buy Levels: {len(futures_buy)}, Sell Levels: {len(futures_sell)}, Imbalance: {futures_imbalance}")


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
        
        except Exception as e:
            logging.error(f"[ERROR - Main Loop] Error tak terduga dalam siklus utama: {e}", exc_info=True)
            # Coba hentikan dan mulai ulang OrderBookManager jika terjadi error serius
            logging.info("Mencoba me-restart OrderBookManager untuk pemulihan...")
            await spot_ob_manager.stop()
            await futures_ob_manager.stop()
            await asyncio.sleep(5) # Beri waktu untuk cleanup
            await spot_ob_manager.start()
            await futures_ob_manager.start()
            await asyncio.sleep(10) # Beri waktu untuk sinkronisasi ulang
            # Lanjutkan loop, berharap error tidak berulang


if __name__ == '__main__':
    if DISCORD_WEBHOOK_URL == 'https://discord.com/api/webhooks/YOUR_DISCORD_WEBHOOK_URL_HERE':
        logging.error("PENTING: Harap ganti 'https://discord.com/api/webhooks/YOUR_DISCORD_WEBHOOK_URL_HERE' dengan URL webhook Discord Anda yang valid di bagian KONFIGURASI!")
        exit()
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot dihentikan oleh pengguna. Melakukan cleanup...")
        # Perlu memastikan OrderBookManager dihentikan dengan benar saat KeyboardInterrupt
        # Ini akan dilakukan oleh akhirnya loop main, atau bisa ditambahkan di sini secara eksplisit
        # Untuk kasus sederhana ini, tidak masalah jika proses langsung keluar
    except Exception as e:
        logging.error(f"Error fatal di luar main loop: {e}", exc_info=True)

