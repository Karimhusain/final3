import requests
import time
import json
from datetime import datetime, timedelta, timezone # Tambah timezone
import math
import asyncio # Untuk operasi async
import aiohttp # Untuk permintaan HTTP async
import logging # Untuk logging yang lebih baik

# ===== KONFIGURASI =====
# Pastikan nilai-nilai ini sesuai dengan kebutuhan Anda.
# Perhatian: Ganti URL Discord Webhook dengan milik Anda!
PAIR = 'BTCUSDT'
STEP = 100
DEPTH = 200 # Maksimal 1000 di Binance
WALL_THRESHOLD_MAIN = 1000 # Contoh: 1000 BTC dianggap "WALL" utama
WALL_THRESHOLD_MINOR = 100 # Contoh: 100 BTC dianggap "wall" minor/potensial
MAX_BUY_LINES = 10
MAX_SELL_LINES = 10
TRACK_INTERVAL = 3  # detik, interval pengambilan data selama durasi pelacakan
TRACK_DURATION = 60 # detik, durasi total pelacakan sebelum penutupan candle
DISCORD_WEBHOOK_URL = 'https://discord.com/api/webhooks/1385219307608346634/-1sAEFdJ6V5rqFqFc7DSBhpIgqNXymoQOxsGERka-cplGJkcacYqGWlTk44BddYamOOz' # <<< GANTI INI!

# ===== KONFIGURASI LOGGING =====
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ===== URL API BINANCE =====
SPOT_URL = f'https://api.binance.com/api/v3/depth?symbol={PAIR}&limit={DEPTH}'
FUTURES_URL = f'https://fapi.binance.com/fapi/v1/depth?symbol={PAIR}&limit={DEPTH}'

# ===== FUNGSI PEMBANTU =====
def get_price_step(price):
    """Membulatkan harga ke langkah (step) terdekat untuk pengelompokan."""
    return math.floor(float(price) / STEP) * STEP

def _get_wall_label(avg_qty, is_stable):
    """Menentukan label untuk wall berdasarkan kuantitas rata-rata dan stabilitasnya."""
    if avg_qty >= WALL_THRESHOLD_MAIN:
        return '🟥🟥🟥 WALL' if is_stable else '⚠️ Fake Wall'
    elif is_stable:
        return '(Minor Wall)'
    else:
        return '(Minor Fake Wall)'

# ===== FUNGSI UTAMA =====
async def fetch_orderbook(session, url):
    """Mengambil data orderbook dari URL yang diberikan secara async."""
    try:
        async with session.get(url, timeout=5) as resp:
            resp.raise_for_status() # Akan memunculkan error untuk respons HTTP 4xx/5xx
            data = await resp.json()
            return data.get('bids', []), data.get('asks', [])
    except aiohttp.ClientError as e:
        logging.error(f'[{datetime.now().strftime("%H:%M:%S")}] [ERROR] Gagal mengambil orderbook dari {url}: {e}')
        return [], []
    except asyncio.TimeoutError:
        logging.error(f'[{datetime.now().strftime("%H:%M:%S")}] [ERROR] Waktu habis saat mengambil orderbook dari {url}')
        return [], []
    except json.JSONDecodeError:
        response_text = await resp.text() if 'resp' in locals() else "N/A"
        logging.error(f'[{datetime.now().strftime("%H:%M:%S")}] [ERROR] Gagal mendecode JSON dari {url}. Respon: {response_text}')
        return [], []
    except Exception as e:
        logging.error(f'[{datetime.now().strftime("%H:%M:%S")}] [ERROR] Error tak terduga saat mengambil orderbook dari {url}: {e}')
        return [], []

async def track_walls(orderbook_url):
    """Melacak dan mengumpulkan data wall selama DURASI_PELACAKAN."""
    logging.info(f"[{datetime.now().strftime('%H:%M:%S')}] [INFO] Melacak wall selama {TRACK_DURATION} detik ... {orderbook_url}")

    wall_history = {}
    end_time = time.time() + TRACK_DURATION

    async with aiohttp.ClientSession() as session:
        while time.time() < end_time:
            bids, asks = await fetch_orderbook(session, orderbook_url)
            current_snapshot = {}

            # Bids (order beli)
            for bid in bids:
                price_step = get_price_step(bid[0])
                qty = float(bid[1])
                if qty >= WALL_THRESHOLD_MINOR:
                    current_snapshot.setdefault(price_step, {'buy': 0})
                    current_snapshot[price_step]['buy'] = qty

            # Asks (order jual)
            for ask in asks:
                price_step = get_price_step(ask[0])
                qty = float(ask[1])
                if qty >= WALL_THRESHOLD_MINOR:
                    current_snapshot.setdefault(price_step, {'sell': 0})
                    current_snapshot[price_step]['sell'] = qty

            # Perbarui riwayat wall
            for price_step, side_data in current_snapshot.items():
                wall_history.setdefault(price_step, {'buy': [], 'sell': []})
                if 'buy' in side_data:
                    wall_history[price_step]['buy'].append(side_data['buy'])
                if 'sell' in side_data:
                    wall_history[price_step]['sell'].append(side_data['sell'])

            await asyncio.sleep(TRACK_INTERVAL)

    return wall_history

def analyze_walls(wall_history):
    """Menganalisis riwayat wall untuk mengidentifikasi wall utama dan minor."""
    buy_levels, sell_levels = [], []
    total_buy, total_sell = 0, 0

    for price_step in sorted(wall_history.keys()):
        buys = wall_history[price_step]['buy']
        sells = wall_history[price_step]['sell']

        if buys:
            avg_qty = sum(buys) / len(buys)
            is_stable = all(abs(q - avg_qty) < avg_qty * 0.2 for q in buys)
            label = _get_wall_label(avg_qty, is_stable)
            buy_levels.append((price_step, avg_qty, label))
            total_buy += avg_qty

        if sells:
            avg_qty = sum(sells) / len(sells)
            is_stable = all(abs(q - avg_qty) < avg_qty * 0.2 for q in sells)
            label = _get_wall_label(avg_qty, is_stable)
            sell_levels.append((price_step, avg_qty, label))
            total_sell += avg_qty

    if total_buy > total_sell:
        imbalance = 'BUY DOMINANT'
    elif total_sell > total_buy:
        imbalance = 'SELL DOMINANT'
    else:
        imbalance = 'BALANCED' # Tambahkan status balanced

    return buy_levels, sell_levels, imbalance

def format_output(levels, max_lines, side):
    """Memformat daftar level wall untuk output Discord."""
    if not levels:
        return f"Tidak ada wall { 'beli' if side == 'buy' else 'jual' } yang kuat."

    # Urutkan berdasarkan harga, naik untuk jual, turun untuk beli (terdekat)
    sorted_levels = sorted(levels, key=lambda x: x[0], reverse=(side == 'buy'))
    output_lines = []

    for lvl in sorted_levels[:max_lines]:
        output_lines.append(f"{lvl[0]:,.0f} → {lvl[1]:,.2f} BTC {lvl[2]}")

    return "\n".join(output_lines)

def send_to_discord(content):
    """Mengirim pesan ke Discord Webhook."""
    payload = {"content": content}
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(payload),
                             headers={"Content-Type": "application/json"})
        if resp.status_code in (200, 204):
            logging.info(f"[{datetime.now().strftime('%H:%M:%S')}] [INFO] Pesan berhasil dikirim ke Discord.")
        else:
            logging.error(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] Discord webhook error: {resp.status_code} {resp.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] Gagal mengirim ke Discord: {e}")
    except Exception as e:
        logging.error(f"[{datetime.now().strftime('%H:%M:%S')}] [ERROR] Error tak terduga saat mengirim ke Discord: {e}")

def wait_until_pre_close():
    """Menunggu hingga 1 menit sebelum penutupan candle 15 menit berikutnya."""
    now_utc = datetime.utcnow()
    
    # Hitung menit candle 15m berikutnya (00, 15, 30, 45)
    current_quarter = now_utc.minute // 15
    next_quarter_minute = (current_quarter + 1) * 15
    
    if next_quarter_minute == 60: # Jika hasilnya 60, berarti jam berikutnya
        next_candle_utc = (now_utc + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    else:
        next_candle_utc = now_utc.replace(minute=next_quarter_minute, second=0, microsecond=0)

    pre_close_time_utc = next_candle_utc - timedelta(minutes=1)

    logging.info(f"[{datetime.now().strftime('%H:%M:%S')}] [INFO] Pelacakan berikutnya pada: {pre_close_time_utc.strftime('%H:%M:%S')} UTC (1 menit sebelum {next_candle_utc.strftime('%H:%M:%S')} UTC)")

    while True:
        now_utc = datetime.utcnow()
        if now_utc >= pre_close_time_utc:
            return next_candle_utc
        time.sleep(5) # Cek setiap 5 detik

# ===== LOOP UTAMA =====
async def main():
    logging.info(f"=== {PAIR} WALL TRACKER (SPOT + FUTURES) DIMULAI ===")

    # Atur zona waktu WIB untuk output
    wib_tz = timezone(timedelta(hours=7))

    while True:
        next_close_time_utc = wait_until_pre_close()

        # Jalankan pelacakan SPOT dan FUTURES secara konkuren
        spot_task = track_walls(SPOT_URL)
        futures_task = track_walls(FUTURES_URL)

        spot_history, futures_history = await asyncio.gather(spot_task, futures_task)

        # Analisis data yang terkumpul
        spot_buy, spot_sell, spot_imbalance = analyze_walls(spot_history)
        futures_buy, futures_sell, futures_imbalance = analyze_walls(futures_history)

        # Konversi waktu penutupan ke WIB untuk output Discord
        close_time_wib = next_close_time_utc.astimezone(wib_tz)
        close_time_str = close_time_wib.strftime('%Y-%m-%d %H:%M WIB')

        content = f"""
📊 **{PAIR} — DETEKSI ORDERBOOK WALL** ⏰ Waktu: {close_time_str} (Penutupan Lilin 15m)  
Pelacakan: {TRACK_DURATION} detik (sebelum penutupan), Interval {TRACK_INTERVAL} detik  

---

### === ORDERBOOK SPOT ===

**__**[ BUY WALLS — {MAX_BUY_LINES} LEVEL TERDEKAT ]**__** {format_output(spot_buy, MAX_BUY_LINES, 'buy')}

**__**[ SELL WALLS — {MAX_SELL_LINES} LEVEL TERDEKAT ]**__** {format_output(spot_sell, MAX_SELL_LINES, 'sell')}

**IMBALANCE:** {spot_imbalance}

---

### === ORDERBOOK FUTURES ===

**__**[ BUY WALLS — {MAX_BUY_LINES} LEVEL TERDEKAT ]**__** {format_output(futures_buy, MAX_BUY_LINES, 'buy')}

**__**[ SELL WALLS — {MAX_SELL_LINES} LEVEL TERDEKAT ]**__** {format_output(futures_sell, MAX_SELL_LINES, 'sell')}

**IMBALANCE:** {futures_imbalance}
"""
        send_to_discord(content)
        logging.info(f"[{datetime.now().strftime('%H:%M:%S')}] [INFO] Tidur hingga siklus berikutnya...\n")
        # Beri jeda 60 detik setelah mengirim pesan agar tidak langsung cek lagi
        # atau sesuaikan agar tepat menunggu sampai awal candle 15m berikutnya jika ingin lebih presisi
        now_after_send = datetime.utcnow()
        sleep_duration = (next_close_time_utc + timedelta(minutes=14)) - now_after_send
        if sleep_duration.total_seconds() > 0:
            await asyncio.sleep(sleep_duration.total_seconds())
        else:
            await asyncio.sleep(60) # Fallback jika sudah melewati waktunya

if __name__ == '__main__':
    # Pastikan Anda telah mengganti placeholder webhook!
    if DISCORD_WEBHOOK_URL == 'https://discord.com/api/webhooks/YOUR_DISCORD_WEBHOOK_URL_HERE':
        logging.error("PENTING: Harap ganti 'YOUR_DISCORD_WEBHOOK_URL_HERE' dengan URL webhook Discord Anda yang valid!")
        exit()
    
    # Jalankan fungsi main async
    asyncio.run(main())

