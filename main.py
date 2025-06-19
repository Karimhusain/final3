import requests
import time
import json
from datetime import datetime, timedelta, timezone
import math
import asyncio
import aiohttp
import logging

# --- KONFIGURASI ---
# Sesuaikan parameter ini sesuai kebutuhan Anda.
# PENTING: Ganti URL Discord Webhook dengan milik Anda!
PAIR = 'BTCUSDT'
STEP = 100 # Pembulatan harga untuk pengelompokan orderbook (misal: 29000, 29100). Sesuaikan dengan tick size pair!
DEPTH = 1000 # Maksimal limit orderbook yang bisa diambil dari Binance (disarankan 1000 untuk data lebih lengkap)

# Ambang batas (thresholds) untuk deteksi wall berdasarkan kuantitas (dalam unit PAIR, misal BTC)
WALL_THRESHOLD_MAIN = 1000 # Kuantitas minimum untuk dianggap 'WALL' utama (🟥🟥🟥)
WALL_THRESHOLD_MINOR = 100  # Kuantitas minimum untuk dianggap 'Minor Wall' (dibawah WALL_THRESHOLD_MAIN)

# Ambang batas untuk deteksi spoofing (misal: order besar muncul dan hilang dengan cepat)
# Jika kuantitas berfluktuasi lebih dari X% dari rata-rata (dan qty >= SPOOF_QUANTITY_MIN), dianggap 'spoofing'
SPOOF_DETECTION_THRESHOLD_PERCENT = 0.5 # Fluktuasi > 50% dari rata-rata kuantitas
SPOOF_QUANTITY_MIN = 50 # Kuantitas minimum agar order bisa dianggap "spoofable"

# Toleransi stabilitas: jika kuantitas berubah lebih dari X% dari rata-rata, dianggap tidak stabil (Fake Wall)
STABILITY_TOLERANCE_PERCENT = 0.2 # Toleransi 20% perubahan kuantitas agar dianggap stabil

# Jumlah baris orderbook yang akan ditampilkan di Discord (termasuk non-wall)
MAX_DISPLAY_LINES = 30 # Contoh: tampilkan 15 baris terdekat dari harga saat ini (per sisi)

TRACK_INTERVAL = 3  # detik, interval pengambilan snapshot orderbook selama durasi pelacakan
TRACK_DURATION = 60 # detik, total durasi pelacakan sebelum penutupan candle 15m

# GANTI DENGAN URL WEBHOOK DISCORD ANDA YANG VALID!
DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/1385219307608346634/-1sAEFdJ6V5rqFqFc7DSBhpIgqNXymoQOxsGERka-cplGJkcacYqGWlTk44BddYamOOz' 

# --- KONFIGURASI LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- URL API BINANCE ---
SPOT_URL = f'https://api.binance.com/api/v3/depth?symbol={PAIR}&limit={DEPTH}'
FUTURES_URL = f'https://fapi.binance.com/fapi/v1/depth?symbol={PAIR}&limit={DEPTH}'

# --- FUNGSI PEMBANTU ---
def get_price_step(price):
    """Membulatkan harga ke langkah (step) terdekat untuk pengelompokan."""
    # Memastikan pembulatan selalu ke bawah agar sesuai dengan "step" harga
    return math.floor(float(price) / STEP) * STEP

def _get_label_from_analysis(avg_qty, is_stable, is_spoof):
    """Menentukan label untuk orderbook berdasarkan kuantitas rata-rata, stabilitas, dan deteksi spoof."""
    # Prioritas deteksi: Spoofing > Main Wall > Minor Wall
    if is_spoof:
        return '⚠️ SPOOFING?' 
    elif avg_qty >= WALL_THRESHOLD_MAIN:
        return '🟥🟥🟥 WALL' if is_stable else '⚠️ Fake Wall'
    elif avg_qty >= WALL_THRESHOLD_MINOR:
        return '(Minor Wall)' if is_stable else '(Minor Fake Wall)'
    return '' # Tidak ada label jika bukan wall

# --- FUNGSI UTAMA ---
async def fetch_orderbook(session, url):
    """Mengambil data orderbook dari URL yang diberikan secara async."""
    try:
        async with session.get(url, timeout=10) as resp: # Tingkatkan timeout untuk keandalan API
            resp.raise_for_status() # Akan memunculkan error untuk respons HTTP 4xx/5xx
            data = await resp.json()
            return data.get('bids', []), data.get('asks', [])
    except aiohttp.ClientError as e:
        # Menangkap error spesifik dari aiohttp (koneksi, DNS, dll)
        logging.error(f'[ERROR - API Call] Gagal mengambil orderbook dari {url}: {e}')
        return [], []
    except asyncio.TimeoutError:
        # Menangkap error jika permintaan melebihi batas waktu
        logging.error(f'[ERROR - Timeout] Waktu habis saat mengambil orderbook dari {url}')
        return [], []
    except json.JSONDecodeError:
        # Menangkap error jika respons bukan JSON yang valid
        response_text = "N/A"
        try:
            # Mencoba membaca teks respons jika tersedia untuk debugging
            response_text = await resp.text() if 'resp' in locals() and resp.status != 200 else "N/A"
        except Exception:
            pass # Abaikan jika gagal membaca teks respons
        logging.error(f'[ERROR - JSON Decode] Gagal mendecode JSON dari {url}. Respon: {response_text[:200]}...') # Batasi log respon
        return [], []
    except Exception as e:
        # Menangkap error tak terduga lainnya
        logging.error(f'[ERROR - Unhandled] Error tak terduga saat mengambil orderbook dari {url}: {e}', exc_info=True) # exc_info=True untuk traceback lengkap
        return [], []

async def track_all_orderbook(orderbook_url):
    """Melacak dan mengumpulkan data orderbook secara keseluruhan selama DURASI_PELACAKAN."""
    logging.info(f"[INFO] Melacak seluruh orderbook selama {TRACK_DURATION} detik dari {orderbook_url}...")

    # Struktur: {price_step: {'buy': [qty1, qty2, ...], 'sell': [qty1, qty2, ...]}}
    orderbook_history = {}
    end_time = time.time() + TRACK_DURATION
    snapshot_count = 0

    async with aiohttp.ClientSession() as session:
        while time.time() < end_time:
            bids, asks = await fetch_orderbook(session, orderbook_url)
            
            # Hanya proses jika data berhasil diambil
            if bids or asks:
                snapshot_count += 1
                current_snapshot = {}

                # Proses Bids (order beli)
                for bid_price_str, bid_qty_str in bids:
                    try:
                        price_step = get_price_step(bid_price_str)
                        qty = float(bid_qty_str)
                        current_snapshot.setdefault(price_step, {})['buy'] = qty
                    except ValueError:
                        logging.warning(f"Skipping invalid bid data: price={bid_price_str}, qty={bid_qty_str}")

                # Proses Asks (order jual)
                for ask_price_str, ask_qty_str in asks:
                    try:
                        price_step = get_price_step(ask_price_str)
                        qty = float(ask_qty_str)
                        current_snapshot.setdefault(price_step, {})['sell'] = qty
                    except ValueError:
                        logging.warning(f"Skipping invalid ask data: price={ask_price_str}, qty={ask_qty_str}")

                # Perbarui riwayat orderbook
                for price_step, side_data in current_snapshot.items():
                    orderbook_history.setdefault(price_step, {'buy': [], 'sell': []})
                    if 'buy' in side_data:
                        orderbook_history[price_step]['buy'].append(side_data['buy'])
                    if 'sell' in side_data:
                        orderbook_history[price_step]['sell'].append(side_data['sell'])
            else:
                logging.warning(f"[WARNING] Tidak ada data orderbook yang diterima dari {orderbook_url} pada snapshot ini.")

            await asyncio.sleep(TRACK_INTERVAL)
    
    logging.info(f"[INFO] Pelacakan selesai untuk {orderbook_url}. Total snapshot: {snapshot_count}")
    return orderbook_history, snapshot_count # Mengembalikan juga jumlah snapshot

def analyze_full_orderbook(orderbook_history, snapshot_count):
    """Menganalisis riwayat orderbook untuk mengidentifikasi wall, minor wall, spoofing, dan ketidakseimbangan."""
    analyzed_buy_levels = []
    analyzed_sell_levels = []
    total_buy_qty_overall = 0
    total_sell_qty_overall = 0

    # Sortir berdasarkan price_step untuk konsistensi
    for price_step in sorted(orderbook_history.keys()):
        buys = orderbook_history[price_step].get('buy', [])
        sells = orderbook_history[price_step].get('sell', [])

        # Analisis Beli
        if buys:
            avg_qty = sum(buys) / len(buys)
            
            # Deteksi stabilitas: kuantitas tidak banyak berubah (dalam STABILITY_TOLERANCE_PERCENT dari rata-rata)
            # Pastikan ada cukup data untuk menghitung stabilitas
            is_stable = (len(buys) < 2) or all(abs(q - avg_qty) < avg_qty * STABILITY_TOLERANCE_PERCENT for q in buys)
            
            # Deteksi spoofing: jika kuantitas berfluktuasi drastis (hanya jika ada >1 snapshot)
            is_spoof = False
            if avg_qty >= SPOOF_QUANTITY_MIN and snapshot_count > 1:
                min_qty = min(buys)
                max_qty = max(buys)
                if avg_qty > 0 and (max_qty - min_qty) / avg_qty > SPOOF_DETECTION_THRESHOLD_PERCENT:
                    is_spoof = True
                # Jika semua snapshot adalah 0 atau mendekati 0, dan kemudian muncul sangat besar, ini mungkin spoofing
                elif all(q < SPOOF_QUANTITY_MIN * 0.1 for q in buys[:-1]) and buys[-1] >= SPOOF_QUANTITY_MIN:
                    is_spoof = True


            label = _get_label_from_analysis(avg_qty, is_stable, is_spoof)
            analyzed_buy_levels.append({'price': price_step, 'qty': avg_qty, 'label': label, 'is_wall_or_spoof': bool(label)})
            total_buy_qty_overall += avg_qty

        # Analisis Jual
        if sells:
            avg_qty = sum(sells) / len(sells)

            # Deteksi stabilitas
            is_stable = (len(sells) < 2) or all(abs(q - avg_qty) < avg_qty * STABILITY_TOLERANCE_PERCENT for q in sells)

            # Deteksi spoofing
            is_spoof = False
            if avg_qty >= SPOOF_QUANTITY_MIN and snapshot_count > 1:
                min_qty = min(sells)
                max_qty = max(sells)
                if avg_qty > 0 and (max_qty - min_qty) / avg_qty > SPOOF_DETECTION_THRESHOLD_PERCENT:
                    is_spoof = True
                elif all(q < SPOOF_QUANTITY_MIN * 0.1 for q in sells[:-1]) and sells[-1] >= SPOOF_QUANTITY_MIN:
                    is_spoof = True


            label = _get_label_from_analysis(avg_qty, is_stable, is_spoof)
            analyzed_sell_levels.append({'price': price_step, 'qty': avg_qty, 'label': label, 'is_wall_or_spoof': bool(label)})
            total_sell_qty_overall += avg_qty

    # Menentukan Imbalance global
    # Gunakan ambang batas perbedaan persentase untuk menentukan dominasi
    imbalance = 'BALANCED'
    if total_buy_qty_overall > 0 and total_sell_qty_overall > 0:
        if total_buy_qty_overall / total_sell_qty_overall >= 1.2: # Beli 20% lebih dominan
            imbalance = 'BUY DOMINANT'
        elif total_sell_qty_overall / total_buy_qty_overall >= 1.2: # Jual 20% lebih dominan
            imbalance = 'SELL DOMINANT'
    elif total_buy_qty_overall > 0:
        imbalance = 'BUY DOMINANT (No significant sell volume)'
    elif total_sell_qty_overall > 0:
        imbalance = 'SELL DOMINANT (No significant buy volume)'
    else:
        imbalance = 'BALANCED (No significant orderbook activity detected)'


    return analyzed_buy_levels, analyzed_sell_levels, imbalance

def format_full_orderbook_output(levels, max_lines, side):
    """Memformat daftar level orderbook (termasuk non-wall) untuk output Discord."""
    if not levels:
        return f"Tidak ada data { 'beli' if side == 'buy' else 'jual' } orderbook yang cukup dalam durasi pelacakan."

    # Urutkan berdasarkan harga: naik untuk jual (terdekat dari harga saat ini), turun untuk beli (terdekat)
    sorted_levels = sorted(levels, key=lambda x: x['price'], reverse=(side == 'buy'))
    output_lines = []

    # Ambil hanya sejumlah baris yang ditentukan MAX_DISPLAY_LINES
    for item in sorted_levels[:max_lines]:
        label_text = f" {item['label']}" if item['label'] else ""
        # Format kuantitas BTC ke 2 angka desimal
        output_lines.append(f"{item['price']:,.0f} → {item['qty']:,.2f} BTC{label_text}")

    return "\n".join(output_lines)

def send_to_discord(content):
    """Mengirim pesan ke Discord Webhook."""
    payload = {"content": content}
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(payload),
                             headers={"Content-Type": "application/json"})
        resp.raise_for_status() # Tangani status error HTTP (misal 400, 404, 500)
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

    while True:
        next_close_time_utc = wait_until_pre_close()

        # Jalankan pelacakan SPOT dan FUTURES secara konkuren (bersamaan)
        # track_all_orderbook kini mengembalikan history DAN jumlah snapshot
        spot_history, spot_snapshot_count = await track_all_orderbook(SPOT_URL)
        futures_history, futures_snapshot_count = await track_all_orderbook(FUTURES_URL)

        # Analisis data yang terkumpul dari kedua orderbook, berikan juga jumlah snapshot untuk akurasi spoofing
        spot_buy, spot_sell, spot_imbalance = analyze_full_orderbook(spot_history, spot_snapshot_count)
        futures_buy, futures_sell, futures_imbalance = analyze_full_orderbook(futures_history, futures_snapshot_count)

        close_time_wib = next_close_time_utc.astimezone(wib_tz)
        close_time_str = close_time_wib.strftime('%Y-%m-%d %H:%M WIB')

        content = f"""
📊 **{PAIR} — DETEKSI ORDERBOOK WALL** ⏰ Waktu: {close_time_str} (Penutupan Lilin 15m)  
Pelacakan: {TRACK_DURATION} detik (sebelum penutupan), Interval {TRACK_INTERVAL} detik  

---

### === ORDERBOOK SPOT ===

**__**[ BUY ORDERS — {MAX_DISPLAY_LINES} LEVEL TERDEKAT ]**__** {format_full_orderbook_output(spot_buy, MAX_DISPLAY_LINES, 'buy')}

**__**[ SELL ORDERS — {MAX_DISPLAY_LINES} LEVEL TERDEKAT ]**__** {format_full_orderbook_output(spot_sell, MAX_DISPLAY_LINES, 'sell')}

**IMBALANCE:** {spot_imbalance}

---

### === ORDERBOOK FUTURES ===

**__**[ BUY ORDERS — {MAX_DISPLAY_LINES} LEVEL TERDEKAT ]**__** {format_full_orderbook_output(futures_buy, MAX_DISPLAY_LINES, 'buy')}

**__**[ SELL ORDERS — {MAX_DISPLAY_LINES} LEVEL TERDEKAT ]**__** {format_full_orderbook_output(futures_sell, MAX_DISPLAY_LINES, 'sell')}

**IMBALANCE:** {futures_imbalance}
"""
        send_to_discord(content)
        logging.info(f"[INFO] Siklus deteksi selesai. Menunggu siklus berikutnya...\n")
        
        now_after_send = datetime.utcnow()
        # Hitung waktu untuk tidur sampai 1 menit sebelum candle berikutnya dimulai
        next_tracking_start_utc = (next_close_time_utc + timedelta(minutes=15)) - timedelta(minutes=1)
        sleep_duration_seconds = (next_tracking_start_utc - now_after_send).total_seconds()

        if sleep_duration_seconds > 0:
            logging.info(f"[INFO] Tidur selama {int(sleep_duration_seconds)} detik sampai waktu pelacakan berikutnya.")
            await asyncio.sleep(sleep_duration_seconds)
        else:
            logging.warning("[WARNING] Waktu tidur negatif atau nol. Tidur 60 detik sebagai fallback.")
            await asyncio.sleep(60)

if __name__ == '__main__':
    # Validasi URL Webhook Discord. Jangan sampai lupa ganti!
    if DISCORD_WEBHOOK_URL == 'https://discord.com/api/webhooks/YOUR_DISCORD_WEBHOOK_URL_HERE':
        logging.error("PENTING: Harap ganti 'https://discord.com/api/webhooks/YOUR_DISCORD_WEBHOOK_URL_HERE' dengan URL webhook Discord Anda yang valid di bagian KONFIGURASI!")
        exit()
    
    asyncio.run(main())
