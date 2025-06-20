import requests
import time
import json
import asyncio
import aiohttp
import websockets
import logging
import numpy as np  # Tambahkan import numpy
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
import math
import sys

# --- KONFIGURASI ---
PAIR = 'BTCUSDT'
STEP = 100 # Tetap 100 sesuai yang Anda inginkan
DEPTH = 1000

# !!! PENTING: NILAI-NILAI INI PERLU DISESUAIKAN KARENA SEKARANG ADALAH TOTAL QTY DALAM STEP !!!
# Anda harus mengamati output pertama kali setelah perubahan ini
# dan sesuaikan kembali nilai-nilai ini agar sesuai dengan yang Anda anggap "Wall" atau "Spoof"
WALL_THRESHOLD_MAIN = 200   # Contoh: mungkin 50 BTC total dalam 100 poin harga adalah "Main Wall"
WALL_THRESHOLD_MINOR = 100  # Contoh: mungkin 20 BTC total dalam 100 poin harga adalah "Minor Wall"

SPOOF_DETECTION_THRESHOLD_PERCENT = 0.5
SPOOF_QUANTITY_MIN = 100    # Contoh: hanya cek spoofing untuk total kuantitas step >= 10 BTC

STABILITY_TOLERANCE_PERCENT = 0.2

MAX_DISPLAY_LINES = 5 # Ditingkatkan agar bisa melihat lebih banyak 'wall'

TRACK_INTERVAL = 3
TRACK_DURATION = 60

DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/1385663406790545570/d4LV5YNiMeH1VOqbUL_ZwUTK4iJ_dL6Xrz1PJx2gmfVkP1xJzYkcFqz0MxSdQQlMFJgy'

# --- KONFIGURASI LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- URL API BINANCE ---
SPOT_REST_URL = f'https://api.binance.com/api/v3/depth?symbol={PAIR}&limit={DEPTH}'
FUTURES_REST_URL = f'https://fapi.binance.com/fapi/v1/depth?symbol={PAIR}&limit={DEPTH}'

SPOT_WS_URL = f'wss://stream.binance.com:9443/ws/{PAIR.lower()}@depth'
FUTURES_WS_URL = f'wss://fstream.binance.com/ws/{PAIR.lower()}@depth'

# --- SET RECURSION LIMIT ---
sys.setrecursionlimit(2000) 

# --- FUNGSI PEMBANTU ---
def get_price_step(price):
    return math.floor(float(price) / STEP) * STEP

# Mengubah fungsi untuk menerima 'side' sebagai parameter
def _get_label_from_analysis(avg_qty, is_stable, is_spoof, side):
    label = ""
    
    # Prioritaskan label SPOOFING jika terdeteksi
    if is_spoof and avg_qty >= SPOOF_QUANTITY_MIN: # Pastikan kuantitas juga memenuhi SPOOF_QUANTITY_MIN
        label = '⚠️ SPOOFING?'
    elif avg_qty >= WALL_THRESHOLD_MAIN:
        label = '🟥🟥🟥 WALL' if is_stable else '⚠️ Fake Wall'
    elif avg_qty >= WALL_THRESHOLD_MINOR:
        label = '(Minor Wall)' if is_stable else '(Minor Fake Wall)'
    
    return label

# --- KELAS ORDERBOOK MANAGER (WebSocket) ---
class OrderBookManager:
    def __init__(self, symbol, rest_url, ws_url, session, is_futures=False):
        self.symbol = symbol
        self.rest_url = rest_url
        self.ws_url = ws_url
        self.session = session # Re-use aiohttp session
        self.is_futures = is_futures
        # Mengubah struktur bids/asks untuk menyimpan order individual di setiap step
        # Format: {price_step: {order_price: qty, order_price: qty, ...}}
        self.bids_by_step = defaultdict(dict)
        self.asks_by_step = defaultdict(dict)
        self.last_update_id = -1
        self.msg_queue = asyncio.Queue()
        self.processing_task = None
        self.ws_task = None
        self.reconnect_delay = 1
        self.snapshot_lock = asyncio.Lock()
        self.is_synced = False
        self._closing = False 
        self._process_messages_running = False 
        self.initial_sync_grace_period_end_time = 0 # Timestamp for grace period

    async def _fetch_initial_snapshot(self):
        try:
            logging.info(f"[{self.symbol}] Mengambil snapshot REST awal...")
            async with self.session.get(self.rest_url, timeout=10) as resp:
                resp.raise_for_status()
                data = await resp.json()
                
                # Menginisialisasi bids_by_step dan asks_by_step dari snapshot
                self.bids_by_step.clear()
                self.asks_by_step.clear()

                for price_str, qty_str in data['bids']:
                    price = float(price_str)
                    qty = float(qty_str)
                    price_step = get_price_step(price)
                    self.bids_by_step[price_step][price] = qty # Simpan harga asli di dalam step

                for price_str, qty_str in data['asks']:
                    price = float(price_str)
                    qty = float(qty_str)
                    price_step = get_price_step(price)
                    self.asks_by_step[price_step][price] = qty # Simpan harga asli di dalam step
                    
                self.last_update_id = data['lastUpdateId']
                logging.info(f"[{self.symbol}] Snapshot REST berhasil diambil. lastUpdateId: {self.last_update_id}. Jumlah bids_by_step: {len(self.bids_by_step)}, asks_by_step: {len(self.asks_by_step)}")
                self.is_synced = True
                self.initial_sync_grace_period_end_time = time.time() + 5 
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
        if self.msg_queue.qsize() > 5000: 
            logging.warning(f"[{self.symbol}] Message queue penuh ({self.msg_queue.qsize()}), membuang pesan lama.")
            try: 
                for _ in range(self.msg_queue.qsize() // 2): 
                    self.msg_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        await self.msg_queue.put(message)

    async def _process_messages(self):
        self._process_messages_running = True
        try:
            while not self._closing:
                try:
                    msg = await asyncio.wait_for(self.msg_queue.get(), timeout=1.0) 
                    data = json.loads(msg)
                    
                    final_update_id = data.get('u') or data.get('lastUpdateId')
                    first_update_id = data.get('U') or final_update_id

                    if not final_update_id:
                        continue

                    async with self.snapshot_lock:
                        if not self.is_synced:
                            await asyncio.sleep(0.001) 
                            continue

                        if final_update_id <= self.last_update_id:
                            continue

                        if time.time() < self.initial_sync_grace_period_end_time:
                            pass 
                        elif first_update_id > self.last_update_id + 1 and self.last_update_id != -1:
                            logging.warning(f"[{self.symbol}] Out of sync or missed messages. "
                                            f"First WS ID: {first_update_id}, Last local ID: {self.last_update_id}. Signaling for re-sync.")
                            self.is_synced = False 
                            while not self.msg_queue.empty():
                                try: self.msg_queue.get_nowait()
                                except asyncio.QueueEmpty: break
                            continue 

                        # Update orderbook by price step
                        for price_str, qty_str in data['b']:
                            price = float(price_str)
                            qty = float(qty_str)
                            price_step = get_price_step(price)
                            if qty == 0:
                                self.bids_by_step[price_step].pop(price, None)
                                if not self.bids_by_step[price_step]: # Hapus step jika kosong
                                    self.bids_by_step.pop(price_step, None)
                            else:
                                self.bids_by_step[price_step][price] = qty

                        for price_str, qty_str in data['a']:
                            price = float(price_str)
                            qty = float(qty_str)
                            price_step = get_price_step(price)
                            if qty == 0:
                                self.asks_by_step[price_step].pop(price, None)
                                if not self.asks_by_step[price_step]: # Hapus step jika kosong
                                    self.asks_by_step.pop(price_step, None)
                            else:
                                self.asks_by_step[price_step][price] = qty

                        self.last_update_id = final_update_id
                        self.initial_sync_grace_period_end_time = time.time() + 1 

                except asyncio.TimeoutError:
                    if self._closing: break
                    continue 
                except asyncio.CancelledError:
                    logging.info(f"[{self.symbol}] _process_messages task cancelled.")
                    break
                except json.JSONDecodeError:
                    logging.error(f"[{self.symbol}] Gagal mendecode JSON dari pesan WebSocket: {msg[:200]}...")
                except Exception as e:
                    logging.error(f"[{self.symbol}] Error saat memproses pesan WebSocket: {e}", exc_info=True)
        finally:
            self._process_messages_running = False

    async def _websocket_connect(self):
        while not self._closing:
            try:
                logging.info(f"[{self.symbol}] Menghubungkan ke WebSocket: {self.ws_url}")
                async with websockets.connect(self.ws_url, ping_interval=30, ping_timeout=10) as ws:
                    logging.info(f"[{self.symbol}] WebSocket terhubung.")
                    
                    self.last_update_id = -1
                    self.is_synced = False 
                    self.initial_sync_grace_period_end_time = 0 

                    while not self.msg_queue.empty():
                        try: self.msg_queue.get_nowait()
                        except asyncio.QueueEmpty: break
                    
                    if not await self._fetch_initial_snapshot():
                        logging.error(f"[{self.symbol}] Gagal ambil snapshot awal. Menutup WS dan coba lagi.")
                        await ws.close()
                        await asyncio.sleep(self.reconnect_delay)
                        continue
                    
                    logging.info(f"[{self.symbol}] WebSocket & REST snapshot sinkron. Mulai menerima pesan.")
                    self.reconnect_delay = 1 

                    while not self._closing and self.is_synced: 
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=30) 
                            await self._handle_websocket_message(message)
                        except asyncio.TimeoutError:
                            logging.warning(f"[{self.symbol}] WebSocket recv timeout, koneksi mungkin stagnan. Memaksa ping.")
                            try:
                                await ws.ping()
                            except websockets.exceptions.ConnectionClosed:
                                logging.error(f"[{self.symbol}] Ping gagal, koneksi sudah terputus.")
                                break
                        except websockets.exceptions.ConnectionClosedOK:
                            logging.info(f"[{self.symbol}] WebSocket connection closed normally.")
                            break
                        except websockets.exceptions.ConnectionClosedError as e:
                            logging.error(f"[{self.symbol}] WebSocket connection closed with error: {e}")
                            break
                        except Exception as e:
                            logging.error(f"[{self.symbol}] Error saat menerima pesan WS: {e}", exc_info=True)
                            break
                if not self._closing:
                    logging.info(f"[{self.symbol}] WebSocket connection lost. Reconnecting...")

            except asyncio.CancelledError:
                logging.info(f"[{self.symbol}] _websocket_connect task cancelled.")
                break 
            except websockets.exceptions.WebSocketException as e:
                logging.error(f"[{self.symbol}] WebSocket connection failed: {e}. Retrying in {self.reconnect_delay}s...")
            except Exception as e:
                logging.error(f"[{self.symbol}] Error umum di _websocket_connect: {e}", exc_info=True)

            if not self._closing:
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, 60)

    async def get_current_orderbook(self):
        async with self.snapshot_lock:
            # Mengembalikan salinan deep dari defaultdict agar tidak ada modifikasi eksternal
            current_bids = {price_step: dict(orders) for price_step, orders in self.bids_by_step.items()}
            current_asks = {price_step: dict(orders) for price_step, orders in self.asks_by_step.items()}
            return current_bids, current_asks

    async def start(self):
        if self.ws_task and not self.ws_task.done():
            logging.info(f"[{self.symbol}] WS task already running, skipping start.")
            return
        if self.processing_task and not self.processing_task.done():
            logging.info(f"[{self.symbol}] Processing task already running, skipping start.")
            return
        
        self._closing = False
        self.ws_task = asyncio.create_task(self._websocket_connect())
        self.processing_task = asyncio.create_task(self._process_messages())
        logging.info(f"[{self.symbol}] OrderBookManager tasks started.")

    async def stop(self):
        if self._closing:
            logging.info(f"[{self.symbol}] OrderBookManager already in closing process.")
            return

        self._closing = True

        logging.info(f"[{self.symbol}] Stopping OrderBookManager tasks...")
        tasks_to_cancel = []
        if self.ws_task:
            if not self.ws_task.done():
                self.ws_task.cancel()
                tasks_to_cancel.append(self.ws_task)
            self.ws_task = None
        if self.processing_task:
            if not self.processing_task.done():
                self.processing_task.cancel()
                tasks_to_cancel.append(self.processing_task)
            self.processing_task = None
        
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True) 

        self.bids_by_step.clear()
        self.asks_by_step.clear()
        self.last_update_id = -1
        self.is_synced = False
        self.initial_sync_grace_period_end_time = 0 
        while not self.msg_queue.empty():
            try:
                self.msg_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        
        self._closing = False
        logging.info(f"[{self.symbol}] OrderBookManager stopped.")

# --- FUNGSI ANALISIS & FORMATTING ---
async def track_and_analyze_orderbook_websocket(orderbook_manager):
    logging.info(f"[INFO] Melacak orderbook WebSocket untuk {orderbook_manager.symbol} selama {TRACK_DURATION} detik...")

    effective_maxlen = max(1, int(TRACK_DURATION / TRACK_INTERVAL)) if TRACK_INTERVAL > 0 else 1

    # Mengubah struktur orderbook_history untuk menyimpan total_qty_in_step per snapshot
    orderbook_history = defaultdict(lambda: {'buy': deque(maxlen=effective_maxlen),
                                             'sell': deque(maxlen=effective_maxlen)})
    end_time = time.time() + TRACK_DURATION
    snapshot_count = 0

    while time.time() < end_time:
        if not orderbook_manager.is_synced or orderbook_manager._closing:
            logging.warning(f"[{orderbook_manager.symbol}] OrderBookManager belum sinkron atau sedang menutup. Menunda pelacakan snapshot...")
            await asyncio.sleep(TRACK_INTERVAL)
            continue
            
        current_bids_by_step, current_asks_by_step = await orderbook_manager.get_current_orderbook()
        
        if not current_bids_by_step and not current_asks_by_step:
            logging.warning(f"[{orderbook_manager.symbol}] Tidak ada data orderbook dari manager pada snapshot ini. Melanjutkan...")
            await asyncio.sleep(TRACK_INTERVAL)
            continue
        
        snapshot_count += 1

        # Kumpulkan total kuantitas untuk setiap price_step di snapshot ini
        for price_step, orders_in_step in current_bids_by_step.items():
            if orders_in_step:
                total_qty_in_this_step = sum(orders_in_step.values())
                orderbook_history[price_step]['buy'].append(total_qty_in_this_step)
        
        for price_step, orders_in_step in current_asks_by_step.items():
            if orders_in_step:
                total_qty_in_this_step = sum(orders_in_step.values())
                orderbook_history[price_step]['sell'].append(total_qty_in_this_step)
        
        await asyncio.sleep(TRACK_INTERVAL)
    
    logging.info(f"[INFO] Pelacakan selesai untuk {orderbook_manager.symbol}. Total snapshot: {snapshot_count}")
    
    final_history = {}
    for price_step, data in orderbook_history.items():
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

    all_price_steps = sorted(orderbook_history.keys())

    # Analisis BUY ORDERS
    temp_buy_levels = [] 
    for price_step in sorted(all_price_steps, reverse=True): # Urutkan dari harga tertinggi ke terendah untuk BUY
        history_of_total_buys_in_step = orderbook_history[price_step].get('buy', [])
        
        avg_qty = np.mean(history_of_total_buys_in_step) if history_of_total_buys_in_step else 0
            
        is_stable = True
        is_spoof = False

        if len(history_of_total_buys_in_step) >= 1: 
            if len(history_of_total_buys_in_step) >= 2:
                # Cek stabilitas berdasarkan fluktuasi total kuantitas
                min_qty_in_history = np.min(history_of_total_buys_in_step)
                max_qty_in_history = np.max(history_of_total_buys_in_step)
                if avg_qty > 0 and (max_qty_in_history - min_qty_in_history) / avg_qty > STABILITY_TOLERANCE_PERCENT:
                    is_stable = False
            else: 
                is_stable = True # Tidak bisa cek stabilitas dengan hanya 1 data

            # Logika spoofing (berbasis total kuantitas per step)
            if avg_qty >= SPOOF_QUANTITY_MIN and not is_stable:
                # Jika total kuantitas step cukup besar dan tidak stabil, bisa jadi indikasi spoofing.
                # Ini adalah interpretasi yang berbeda dari spoofing tradisional (muncul/hilang order besar).
                is_spoof = True
        
        if avg_qty > 0:
            label = _get_label_from_analysis(avg_qty, is_stable, is_spoof, 'buy') # Meneruskan 'buy'
            temp_buy_levels.append({'price': price_step, 'qty': avg_qty, 'label': label, 'is_wall_or_spoof': bool(label)})
            total_buy_qty_overall += avg_qty
    
    # Sortir dan hitung kumulatif
    temp_buy_levels.sort(key=lambda x: x['price'], reverse=True)
    cumulative_buy_qty_current = 0
    for item in temp_buy_levels:
        cumulative_buy_qty_current += item['qty']
        analyzed_buy_levels.append({**item, 'cumulative_qty': cumulative_buy_qty_current})


    # Analisis SELL ORDERS
    temp_sell_levels = []
    for price_step in sorted(all_price_steps, reverse=False): # Urutkan dari harga terendah ke tertinggi untuk SELL
        history_of_total_sells_in_step = orderbook_history[price_step].get('sell', [])
        
        avg_qty = np.mean(history_of_total_sells_in_step) if history_of_total_sells_in_step else 0

        is_stable = True
        is_spoof = False

        if len(history_of_total_sells_in_step) >= 1:
            if len(history_of_total_sells_in_step) >= 2:
                min_qty_in_history = np.min(history_of_total_sells_in_step)
                max_qty_in_history = np.max(history_of_total_sells_in_step)
                if avg_qty > 0 and (max_qty_in_history - min_qty_in_history) / avg_qty > STABILITY_TOLERANCE_PERCENT:
                    is_stable = False
            else:
                is_stable = True

            if avg_qty >= SPOOF_QUANTITY_MIN and not is_stable:
                is_spoof = True
        
        if avg_qty > 0:
            label = _get_label_from_analysis(avg_qty, is_stable, is_spoof, 'sell') # Meneruskan 'sell'
            temp_sell_levels.append({'price': price_step, 'qty': avg_qty, 'label': label, 'is_wall_or_spoof': bool(label)})
            total_sell_qty_overall += avg_qty
    
    # Sortir dan hitung kumulatif
    temp_sell_levels.sort(key=lambda x: x['price'], reverse=False)
    cumulative_sell_qty_current = 0
    for item in temp_sell_levels:
        cumulative_sell_qty_current += item['qty']
        analyzed_sell_levels.append({**item, 'cumulative_qty': cumulative_sell_qty_current})

    imbalance = 'BALANCED'
    total_relevant_buy_qty = sum(item['qty'] for item in analyzed_buy_levels if item['is_wall_or_spoof'])
    total_relevant_sell_qty = sum(item['qty'] for item in analyzed_sell_levels if item['is_wall_or_spoof'])

    imbalance_detail = ""
    if total_relevant_buy_qty > 0 or total_relevant_sell_qty > 0:
        imbalance_detail = f" (Total Buy Wall/Spoof: {total_relevant_buy_qty:,.2f} BTC vs Total Sell Wall/Spoof: {total_relevant_sell_qty:,.2f} BTC)"

    # Logika Imbalance ini berdasarkan 'Total Buy Wall/Spoof' dan 'Total Sell Wall/Spoof'
    # Bukan berdasarkan total_buy_qty_overall dan total_sell_qty_overall yang mencakup semua qty
    # Jika Anda ingin imbalance mencakup semua qty, ganti variabelnya.
    if total_relevant_buy_qty > 0 and total_relevant_sell_qty > 0:
        if total_relevant_buy_qty / total_relevant_sell_qty >= 1.2:
            imbalance = 'BUY DOMINANT'
        elif total_relevant_sell_qty / total_relevant_buy_qty >= 1.2:
            imbalance = 'SELL DOMINANT'
    elif total_relevant_buy_qty > 0:
        imbalance = 'BUY DOMINANT (No significant sell wall/spoof)'
    elif total_relevant_sell_qty > 0:
        imbalance = 'SELL DOMINANT (No significant buy wall/spoof)'
    else:
        imbalance = 'BALANCED (No significant wall/spoof activity detected)'
    
    # Menambahkan detail Imbalance (total_relevant_buy_qty vs total_relevant_sell_qty)
    imbalance += imbalance_detail

    return analyzed_buy_levels, analyzed_sell_levels, imbalance

def format_full_orderbook_output(levels, max_lines, side):
    if not levels:
        return f"Tidak ada data { 'beli' if side == 'buy' else 'jual' } orderbook yang cukup."

    output_lines = ["Price → Individual Qty | Cumulative Qty | Label"]
    
    # Urutkan level sebelum menampilkan agar konsisten dengan tampilan Discord
    if side == 'buy':
        display_levels = sorted(levels, key=lambda x: x['price'], reverse=True)
    else: # side == 'sell'
        display_levels = sorted(levels, key=lambda x: x['price'], reverse=False)

    display_count = 0
    for item in display_levels:
        if item['qty'] > 0: # Hanya tampilkan jika kuantitas > 0
            label_text = f" {item['label']}" if item['label'] else ""
            output_lines.append(f"{item['price']:,.0f} → {item['qty']:,.2f} BTC | {item['cumulative_qty']:,.2f} BTC{label_text}")
            display_count += 1
            if display_count >= max_lines: # Batasi jumlah baris yang ditampilkan
                break

    if display_count == 0 and len(output_lines) == 1:
        return f"Tidak ada data { 'beli' if side == 'buy' else 'jual' } orderbook yang cukup signifikan untuk ditampilkan."

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

def get_next_candle_close_time():
    now_utc = datetime.utcnow()
    
    current_quarter = now_utc.minute // 15
    next_quarter_minute = (current_quarter + 1) * 15
    
    if next_quarter_minute == 60:
        next_candle_utc = (now_utc + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    else:
        next_candle_utc = now_utc.replace(minute=next_quarter_minute, second=0, microsecond=0)
    
    return next_candle_utc 

async def wait_until_pre_close(next_candle_close_utc):
    pre_close_time_utc = next_candle_close_utc - timedelta(minutes=1)

    logging.info(f"[INFO] Pelacakan berikutnya akan dimulai pada: {pre_close_time_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC (1 menit sebelum {next_candle_close_utc.strftime('%H:%M:%S')} UTC)")

    while True:
        now_utc = datetime.utcnow()
        if now_utc >= pre_close_time_utc:
            logging.info(f"[INFO] Waktu pelacakan dimulai: {datetime.now().strftime('%H:%M:%S')}")
            return next_candle_close_utc
        await asyncio.sleep(5) 

async def perform_analysis_and_send_report(spot_ob_manager, futures_ob_manager, report_time_utc, report_label):
    wib_tz = timezone(timedelta(hours=7)) 
    
    managers = {'Spot': spot_ob_manager, 'Futures': futures_ob_manager}
    for name, manager in managers.items():
        max_retries = 3
        for attempt in range(max_retries):
            if manager.is_synced and not manager._closing:
                break
            logging.warning(f"[REPORT - {report_label}] {name} OrderBookManager belum sinkron atau sedang menutup (Percobaan {attempt+1}/{max_retries}). Mencoba memulai ulang...")
            await manager.stop() 
            await asyncio.sleep(3) 
            await manager.start()
            await asyncio.sleep(7) 
        else: 
            logging.error(f"[REPORT - {report_label}] {name} OrderBookManager gagal sinkron setelah {max_retries} percobaan. Tidak dapat membuat laporan.")
            return 
    
    if spot_ob_manager._closing or futures_ob_manager._closing:
        logging.warning(f"[REPORT - {report_label}] Salah satu OrderBookManager sedang dalam proses penutupan setelah retry. Melewatkan pembuatan laporan.")
        return 

    logging.info(f"[REPORT - {report_label}] Memulai pelacakan dan analisis untuk laporan.")
    
    spot_history, spot_snapshot_count = await track_and_analyze_orderbook_websocket(spot_ob_manager)
    futures_history, futures_snapshot_count = await track_and_analyze_orderbook_websocket(futures_ob_manager)

    if not spot_history or spot_snapshot_count == 0:
        logging.warning(f"[REPORT - {report_label}] Tidak ada data riwayat spot yang terkumpul atau snapshot kosong. Melewatkan analisis spot.")
        spot_buy, spot_sell, spot_imbalance = [], [], "BALANCED (No data collected)"
    else:
        # Menambahkan parameter is_futures di sini
        spot_buy, spot_sell, spot_imbalance = analyze_full_orderbook(spot_history, spot_snapshot_count, is_futures=False)
    
    if not futures_history or futures_snapshot_count == 0:
        logging.warning(f"[REPORT - {report_label}] Tidak ada data riwayat futures yang terkumpul atau snapshot kosong. Melewatkan analisis futures.")
        futures_buy, futures_sell, futures_imbalance = [], [], "BALANCED (No data collected)"
    else:
        # Menambahkan parameter is_futures di sini
        futures_buy, futures_sell, futures_imbalance = analyze_full_orderbook(futures_history, futures_snapshot_count, is_futures=True)

    report_time_wib = report_time_utc.astimezone(wib_tz)
    report_time_str = report_time_wib.strftime('%Y-%m-%d %H:%M WIB')

    logging.info(f"[REPORT - {report_label}] Siap mengirim output untuk {PAIR} pada {report_time_str}.")
    logging.info(f"[REPORT - {report_label}] Spot Buy Levels: {len(spot_buy)}, Sell Levels: {len(spot_sell)}, Imbalance: {spot_imbalance}")
    logging.info(f"[REPORT - {report_label}] Futures Buy Levels: {len(futures_buy)}, Sell Levels: {len(futures_sell)}, Imbalance: {futures_imbalance}")

    content = f"""
📊 **{PAIR} — DETEKSI ORDERBOOK WALL** ⏰ Waktu: {report_time_str} ({report_label})  
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
    logging.info(f"[REPORT - {report_label}] Laporan selesai dikirim.")

# --- LOOP UTAMA ---
async def main():
    logging.info(f"=== {PAIR} ORDERBOOK WALL DETECTOR (SPOT + FUTURES) DIMULAI ===")

    global aiohttp_session
    aiohttp_session = aiohttp.ClientSession()

    global spot_ob_manager, futures_ob_manager 
    spot_ob_manager = OrderBookManager(PAIR + ' (Spot)', SPOT_REST_URL, SPOT_WS_URL, aiohttp_session)
    futures_ob_manager = OrderBookManager(PAIR + ' (Futures)', FUTURES_REST_URL, FUTURES_WS_URL, aiohttp_session, is_futures=True)

    await spot_ob_manager.start()
    await futures_ob_manager.start()

    logging.info("Memberi waktu 10 detik agar WebSocket terhubung dan sync initial snapshot...")
    await asyncio.sleep(10) 

    logging.info("[MAIN] Melakukan pengiriman laporan awal dengan data terkini...")
    wib_tz_for_now = timezone(timedelta(hours=7))
    await perform_analysis_and_send_report(spot_ob_manager, futures_ob_manager, datetime.now(wib_tz_for_now), "Laporan Awal (Data Terkini)")
    logging.info("[MAIN] Laporan awal selesai dikirim. Melanjutkan ke siklus terjadwal.")

    while True:
        try:
            for name, manager in {'Spot': spot_ob_manager, 'Futures': futures_ob_manager}.items():
                # Pastikan manager saat ini yang digunakan, bukan global variable yang mungkin di-reassign
                current_manager = spot_ob_manager if name == 'Spot' else futures_ob_manager
                if not current_manager.is_synced:
                    logging.warning(f"[MAIN LOOP] {name} OrderBookManager terdeteksi UNSYNCED. Memaksa restart penuh...")
                    await current_manager.stop()
                    # Re-create the manager object to ensure a clean state
                    if name == 'Spot':
                        spot_ob_manager = OrderBookManager(PAIR + ' (Spot)', SPOT_REST_URL, SPOT_WS_URL, aiohttp_session)
                    else: # Futures
                        futures_ob_manager = OrderBookManager(PAIR + ' (Futures)', FUTURES_REST_URL, FUTURES_WS_URL, aiohttp_session, is_futures=True)
                    await (spot_ob_manager if name == 'Spot' else futures_ob_manager).start() # Start the newly created/updated manager
                    await asyncio.sleep(10) 
                    # Re-check sync status after restart
                    if not (spot_ob_manager if name == 'Spot' else futures_ob_manager).is_synced: 
                        logging.error(f"[MAIN LOOP] {name} OrderBookManager GAGAL sinkron setelah restart penuh. Akan mencoba lagi di siklus berikutnya.")
                        continue 

            next_close_time_utc = get_next_candle_close_time()
            await wait_until_pre_close(next_close_time_utc)
            
            await perform_analysis_and_send_report(spot_ob_manager, futures_ob_manager, next_close_time_utc, "Penutupan Lilin 15m")
            
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
        
        except RecursionError as re: 
            logging.critical(f"[ERROR - Main Loop FATAL] RecursionError terdeteksi: {re}. Bot akan mencoba restart total.")
            try:
                if aiohttp_session and not aiohttp_session.closed:
                    await aiohttp_session.close()
                if spot_ob_manager: await spot_ob_manager.stop()
                if futures_ob_manager: await futures_ob_manager.stop()
                await asyncio.sleep(15) 
                
                aiohttp_session = aiohttp.ClientSession()
                spot_ob_manager = OrderBookManager(PAIR + ' (Spot)', SPOT_REST_URL, SPOT_WS_URL, aiohttp_session)
                futures_ob_manager = OrderBookManager(PAIR + ' (Futures)', FUTURES_REST_URL, FUTURES_WS_URL, aiohttp_session, is_futures=True)
                await spot_ob_manager.start()
                await futures_ob_manager.start()
                await asyncio.sleep(15) 
                logging.info("[MAIN] Bot berhasil restart setelah RecursionError.")
            except Exception as restart_e:
                logging.error(f"[ERROR - Main Loop Restart FATAL] Gagal me-restart bot setelah RecursionError: {restart_e}", exc_info=True)
                logging.error("Bot mungkin perlu dihentikan dan dijalankan ulang secara manual.")
                await asyncio.sleep(120) 
        except Exception as e:
            logging.error(f"[ERROR - Main Loop] Error tak terduga dalam siklus utama: {e}", exc_info=True)
            logging.info("Mencoba me-restart OrderBookManager untuk pemulihan...")
            
            try:
                if spot_ob_manager: await spot_ob_manager.stop()
                if futures_ob_manager: await futures_ob_manager.stop()
                await asyncio.sleep(5)
                
                spot_ob_manager = OrderBookManager(PAIR + ' (Spot)', SPOT_REST_URL, SPOT_WS_URL, aiohttp_session)
                futures_ob_manager = OrderBookManager(PAIR + ' (Futures)', FUTURES_REST_URL, FUTURES_WS_URL, aiohttp_session, is_futures=True)
                await spot_ob_manager.start()
                await futures_ob_manager.start()
                await asyncio.sleep(10)
            except Exception as restart_e:
                logging.error(f"[ERROR - Main Loop Restart] Gagal me-restart OrderBookManager: {restart_e}", exc_info=True)
                await asyncio.sleep(60)

# Global references for cleanup_on_exit
spot_ob_manager = None
futures_ob_manager = None
aiohttp_session = None 

if __name__ == '__main__':
    if DISCORD_WEBHOOK_URL == 'https://discord.com/api/webhooks/YOUR_DISCORD_WEBHOOK_URL_HERE':
        logging.error("PENTING: Harap ganti 'https://discord.com/api/webhooks/YOUR_DISCORD_WEBHOOK_URL_HERE' dengan URL webhook Discord Anda yang valid di bagian KONFIGURASI!")
        exit()
    
    async def cleanup_on_exit_global():
        if spot_ob_manager:
            await spot_ob_manager.stop()
        if futures_ob_manager:
            await futures_ob_manager.stop()
        if aiohttp_session and not aiohttp_session.closed:
            await aiohttp_session.close() 
        logging.info("Global managers and aiohttp session cleanup completed.")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot dihentikan oleh pengguna (KeyboardInterrupt). Melakukan cleanup...")
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                # Schedule the cleanup task and wait for it to complete or for a short duration
                loop.create_task(cleanup_on_exit_global())
                loop.run_until_complete(asyncio.sleep(3)) # Give some time for cleanup to run
            else:
                asyncio.run(cleanup_on_exit_global())
        except RuntimeError: 
             asyncio.run(cleanup_on_exit_global()) # Fallback for already closed loop
        except Exception as cleanup_e:
            logging.error(f"Error during cleanup_on_exit: {cleanup_e}", exc_info=True)

    except Exception as e:
        logging.error(f"Error fatal di luar main loop: {e}", exc_info=True)
