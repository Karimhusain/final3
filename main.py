import asyncio
import aiohttp
import datetime
import numpy as np
import pytz
import os
import logging
import time

# --- Konfigurasi Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("orderbook_monitor.log"),
                        logging.StreamHandler()
                    ])

# --- KONFIGURASI ---
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1382392547594342521/nzmNqCuwQzlgeq5PKh6pj4YdXCNd10iQFOlGPEvrpmI8qu20pUs9W3NJdl-185Y2rRf9"

PRICE_INTERVAL = 500  # Interval pengelompokan harga
TOP_LEVELS = 10       # Jumlah level teratas yang ditampilkan
CANDLE_INTERVAL_MINUTES = 15 # Interval candle Binance untuk sinkronisasi pengiriman

EXCHANGES = {
    'binance': {
        'url': 'https://api.binance.com/api/v3/depth',
        'params': {'symbol': 'BTCUSDT', 'limit': 100},
        'spot': True,
        'min_delay_ms': 50
    },
    'bitget': {
        'url': 'https://api.bitget.com/api/spot/v1/market/depth',
        'params': {'symbol': 'BTCUSDT_SPBL', 'limit': 100},
        'spot': True,
        'min_delay_ms': 50
    },
    'bitget_futures': {
        'url': 'https://api.bitget.com/api/mix/v1/market/depth',
        'params': {'symbol': 'BTCUSDT_UMCBL', 'limit': 100},
        'spot': False,
        'min_delay_ms': 50
    },
    'okx': {
        'url': 'https://www.okx.com/api/v5/market/books',
        'params': {'instId': 'BTC-USDT', 'sz': 100},
        'spot': True,
        'min_delay_ms': 100
    },
    'coinbase': {
        'url': 'https://api.exchange.coinbase.com/products/BTC-USDT/book',
        'params': {'level': 2},
        'spot': True,
        'min_delay_ms': 100
    },
    'bybit': {
        'url': 'https://api.bybit.com/v5/market/orderbook',
        'params': {'category': 'spot', 'symbol': 'BTCUSDT'},
        'spot': True,
        'min_delay_ms': 50
    }
}

last_request_time = {name: 0 for name in EXCHANGES}


# --- FUNGSI BANTUAN (HELPERS) ---
def group_price(price: float) -> int:
    """Membulatkan harga ke interval harga terdekat yang ditentukan oleh PRICE_INTERVAL."""
    return int(round(price / PRICE_INTERVAL) * PRICE_INTERVAL)

def format_volume(vol: float) -> str:
    """Memformat volume BTC ke string dengan 2 angka desimal."""
    return f"{vol:.2f} BTC"

def utc_now() -> str:
    """Mengembalikan waktu UTC saat ini dalam format string (YYYY-MM-DD HH:MM:SS UTC)."""
    return datetime.datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

def calculate_next_candle_close_time(interval_minutes: int) -> datetime.datetime:
    """
    Menghitung waktu penutupan candle berikutnya berdasarkan interval tertentu.
    Akan selalu mengembalikan waktu yang akan datang.
    """
    now_utc = datetime.datetime.now(pytz.UTC)
    
    # Hitung total menit sejak awal hari (UTC)
    total_minutes_today = now_utc.hour * 60 + now_utc.minute
    
    # Cari menit terdekat yang merupakan kelipatan dari interval_minutes
    minutes_to_next_interval = interval_minutes - (total_minutes_today % interval_minutes)
    
    # Tambahkan menit tersebut ke waktu sekarang
    next_close_time = now_utc + datetime.timedelta(minutes=minutes_to_next_interval)
    
    # Normalisasi ke menit genap (detik dan mikrodetik menjadi 0)
    next_close_time = next_close_time.replace(second=0, microsecond=0)
    
    logging.info(f"Waktu UTC saat ini: {now_utc.strftime('%H:%M:%S')}. Penutupan candle {interval_minutes} menit berikutnya pada: {next_close_time.strftime('%H:%M:%S UTC')}")
    return next_close_time


# --- FUNGSI PENGAMBIL DATA (FETCHERS) ---
async def fetch_orderbook(session: aiohttp.ClientSession, name: str, config: dict, retries: int = 3) -> tuple:
    """
    Mengambil data order book dari API bursa tertentu dengan penanganan retry dan rate limit.
    """
    min_delay = config.get('min_delay_ms', 0) / 1000

    for attempt in range(retries):
        current_time_monotonic = time.monotonic() # Ganti nama variabel agar tidak bentrok
        elapsed_time = current_time_monotonic - last_request_time[name]
        if elapsed_time < min_delay:
            wait_time = min_delay - elapsed_time
            logging.debug(f"Menunggu {wait_time:.2f} detik untuk memenuhi min_delay pada {name}.")
            await asyncio.sleep(wait_time)
        
        last_request_time[name] = time.monotonic()

        try:
            async with session.get(config['url'], params=config['params'], timeout=10) as resp:
                if resp.status == 429:
                    retry_after = resp.headers.get('Retry-After')
                    sleep_duration = int(retry_after) if retry_after else (2 ** attempt) * 5
                    logging.warning(f"Percobaan {attempt + 1}/{retries}: Terkena rate limit dari {name} (status 429). Menunggu {sleep_duration} detik.")
                    await asyncio.sleep(sleep_duration)
                    continue
                
                resp.raise_for_status()
                data = await resp.json()

                asks = []
                bids = []

                if name == 'okx':
                    if data and 'data' in data and data['data']:
                        asks = [[float(x[0]), float(x[1])] for x in data['data'][0].get('asks', [])]
                        bids = [[float(x[0]), float(x[1])] for x in data['data'][0].get('bids', [])]
                elif name == 'coinbase':
                    asks = [[float(x[0]), float(x[1])] for x in data.get('asks', [])]
                    bids = [[float(x[0]), float(x[1])] for x in data.get('bids', [])]
                elif name == 'bybit':
                    if data and isinstance(data, dict) and 'result' in data and isinstance(data['result'], dict):
                        asks = [[float(x['price']), float(x['size'])] for x in data['result'].get('a', []) if isinstance(x, dict) and 'price' in x and 'size' in x]
                        bids = [[float(x['price']), float(x['size'])] for x in data['result'].get('b', []) if isinstance(x, dict) and 'price' in x and 'size' in x]
                    else:
                        logging.warning(f"Respon Bybit tidak memiliki format 'result' yang diharapkan: {data}")
                        return name, config['spot'], [], []
                else: # Untuk Binance, Bitget, dll. yang formatnya mirip [[price, volume], ...]
                    asks = [[float(x[0]), float(x[1])] for x in data.get('asks', [])]
                    bids = [[float(x[0]), float(x[1])] for x in data.get('bids', [])]
                
                logging.info(f"Berhasil mengambil data dari {name}.")
                return name, config['spot'], asks, bids

        except aiohttp.ClientResponseError as e:
            logging.warning(f"Percobaan {attempt + 1}/{retries}: Kesalahan HTTP dari {name} (Status: {e.status}): {e}. Respons: {await e.response.text() if e.response else 'N/A'}")
            sleep_duration = (2 ** attempt) * 2
            await asyncio.sleep(sleep_duration)
        except aiohttp.ClientError as e:
            logging.warning(f"Percobaan {attempt + 1}/{retries}: Kesalahan klien (jaringan/timeout) saat mengambil {name}: {e}")
            sleep_duration = (2 ** attempt) * 2
            await asyncio.sleep(sleep_duration)
        except Exception as e:
            logging.error(f"Percobaan {attempt + 1}/{retries}: Kesalahan tidak terduga saat mengambil {name}: {e}", exc_info=True)
            sleep_duration = (2 ** attempt) * 5
            await asyncio.sleep(sleep_duration)
    
    logging.error(f"Gagal mengambil data dari {name} setelah {retries} percobaan.")
    return name, config['spot'], [], []

# --- FUNGSI PEMROSESAN DATA (PROCESSING) ---
def aggregate_orderbooks(all_books: list) -> tuple:
    """
    Menggabungkan data order book dari berbagai bursa dan mengelompokkan
    kuantitas berdasarkan interval harga yang ditentukan (PRICE_INTERVAL).
    Sekarang menyimpan kuantitas per bursa untuk deteksi wall.
    """
    # Key: (level, side), Value: {'total_spot': float, 'total_futures': float, 'exchanges': {exchange_name: quantity}}
    grouped = {}
    
    for name, spot, asks, bids in all_books:
        if not asks and not bids:
            logging.warning(f"Tidak ada data asks/bids dari {name}, dilewati agregasi.")
            continue

        for side, book in [('ask', asks), ('bid', bids)]:
            for price, qty in book:
                if price <= 0 or qty <= 0:
                    logging.debug(f"Mengabaikan entri orderbook tidak valid: Price={price}, Qty={qty} dari {name}")
                    continue
                
                level = group_price(price)
                key = (level, side)
                
                if key not in grouped:
                    grouped[key] = {'total_spot': 0.0, 'total_futures': 0.0, 'exchanges': {}}
                
                # Tambahkan kuantitas ke total spot/futures
                if spot:
                    grouped[key]['total_spot'] += float(qty)
                else:
                    grouped[key]['total_futures'] += float(qty)
                
                # Simpan kuantitas per bursa untuk deteksi wall
                exchange_type_key = f"{name} {'(Spot)' if spot else '(Futures)'}"
                if exchange_type_key not in grouped[key]['exchanges']:
                    grouped[key]['exchanges'][exchange_type_key] = 0.0
                grouped[key]['exchanges'][exchange_type_key] += float(qty)
                
    logging.info(f"Berhasil menggabungkan {len(all_books)} order book, menghasilkan {len(grouped)} level harga yang dikelompokkan.")
    return grouped


def detect_wall(grouped: dict) -> list:
    """
    Mendeteksi 'order wall' (akumulasi kuantitas besar) berdasarkan deviasi standar dari kuantitas.
    Kuantitas yang melebihi rata-rata + 2 * deviasi standar dianggap sebagai wall.
    Menyertakan informasi bursa yang dominan.
    """
    all_quantities = [data['total_spot'] + data['total_futures'] for data in grouped.values()]
    if not all_quantities:
        logging.info("Tidak ada kuantitas terdeteksi untuk analisis wall.")
        return []
    
    avg = np.mean(all_quantities)
    std = np.std(all_quantities)
    # Ambang batas untuk deteksi wall: rata-rata + 2 * deviasi standar
    # Anda bisa menyesuaikan faktor 2 ini (misal 1.5, 3) untuk sensitivitas yang berbeda
    threshold = avg + 2 * std
    
    walls = []
    for (p, s), data in grouped.items():
        total_qty = data['total_spot'] + data['total_futures']
        if total_qty > threshold:
            # Temukan bursa dengan kontribusi kuantitas terbesar di level ini
            top_exchanges = sorted(data['exchanges'].items(), key=lambda item: item[1], reverse=True)
            
            exchange_info = ""
            if top_exchanges:
                # Ambil bursa yang menyumbang minimal 20% dari total kuantitas wall, atau bursa teratas jika tidak ada yang mencapai 20%
                significant_exchanges = [
                    f"{name.replace(' (Spot)', '').replace(' (Futures)', '')} {'(Spot)' if '(Spot)' in name else '(Futures)' if '(Futures)' in name else ''}"
                    for name, qty in top_exchanges if qty / total_qty >= 0.2
                ]
                if not significant_exchanges and top_exchanges:
                    significant_exchanges = [
                        f"{top_exchanges[0][0].replace(' (Spot)', '').replace(' (Futures)', '')} {'(Spot)' if '(Spot)' in top_exchanges[0][0] else '(Futures)' if '(Futures)' in top_exchanges[0][0] else ''}"
                    ]

                if significant_exchanges:
                    exchange_info = f" ({', '.join(significant_exchanges)})"

            walls.append((p, s, total_qty, exchange_info))
    
    if walls:
        logging.info(f"Deteksi {len(walls)} order wall dengan ambang batas {threshold:.2f} BTC.")
    else:
        logging.info("Tidak ada order wall signifikan terdeteksi.")
    return walls

def compute_imbalance(grouped: dict) -> tuple:
    """
    Menghitung ketidakseimbangan order book antara total kuantitas bid dan ask.
    Imbalance = (Total Bid - Total Ask) / (Total Bid + Total Ask)
    """
    bid_total = sum(data['total_spot'] + data['total_futures'] for k, data in grouped.items() if k[1] == 'bid')
    ask_total = sum(data['total_spot'] + data['total_futures'] for k, data in grouped.items() if k[1] == 'ask')
    
    if bid_total + ask_total == 0:
        logging.warning("Total kuantitas bid dan ask adalah nol, ketidakseimbangan tidak dapat dihitung.")
        return 0.0, bid_total, ask_total
    
    imbalance_score = (bid_total - ask_total) / (bid_total + ask_total)
    logging.info(f"Ketidakseimbangan order book: {imbalance_score:+.2f} (Bid: {bid_total:.2f}, Ask: {ask_total:.2f})")
    return imbalance_score, bid_total, ask_total

# --- FUNGSI KIRIM PESAN DISCORD ---
async def send_to_discord(content: str):
    """
    Mengirim pesan teks ke Discord Webhook.
    """
    if not DISCORD_WEBHOOK_URL or DISCORD_WEBHOOK_URL == "YOUR_WEBHOOK_URL_HERE":
        logging.error("DISCORD_WEBHOOK_URL belum dikonfigurasi. Pesan tidak terkirim.")
        return

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(DISCORD_WEBHOOK_URL, json={"content": content}) as resp:
                resp.raise_for_status()
                logging.info("Pesan berhasil dikirim ke Discord.")
        except aiohttp.ClientResponseError as e:
            logging.error(f"Gagal mengirim pesan ke Discord: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"Respons Discord: {e.response.status} - {await e.response.text()}")
        except Exception as e:
            logging.error(f"Kesalahan tidak terduga saat mengirim ke Discord: {e}", exc_info=True)

def format_output(current_spot_price: float, current_futures_price: float, grouped: dict, walls: list, imbalance: tuple) -> str:
    """
    Memformat ringkasan order book menjadi string yang mudah dibaca untuk pesan Discord.
    Akan menggabungkan kuantitas spot dan futures menjadi satu kolom 'Total Qty'.
    """
    lines = [
        f"📊 **BTC/USDT Order Book Snapshot** (Grouped by `${PRICE_INTERVAL}`)",
        f"🕒 {utc_now()}\n",
        f"💰 **Harga Terkini (Bid Tertinggi Rata-rata):**",
        f"**Spot** : `${current_spot_price:.2f}`",
        f"**Futures**: `${current_futures_price:.2f}`  (`{'+' if (current_futures_price - current_spot_price) > 0 else ''}${(current_futures_price - current_spot_price):.2f}` Premium)\n",
        f"📦 **ASK (Jual) - Harga | Total BTC Qty**"
    ]

    ask_levels_sorted = sorted(set(p for (p, s) in grouped if s == 'ask'))
    for level in ask_levels_sorted[:TOP_LEVELS]:
        data = grouped.get((level, 'ask'), {'total_spot': 0, 'total_futures': 0})
        total_qty = data['total_spot'] + data['total_futures'] # Summing spot and futures quantities
        lines.append(f"`{level:<10}` `{format_volume(total_qty):<12}`")

    lines.append("\n📥 **BID (Beli) - Harga | Total BTC Qty**")
    bid_levels_sorted = sorted(set(p for (p, s) in grouped if s == 'bid'), reverse=True)
    for level in bid_levels_sorted[:TOP_LEVELS]:
        data = grouped.get((level, 'bid'), {'total_spot': 0, 'total_futures': 0})
        total_qty = data['total_spot'] + data['total_futures'] # Summing spot and futures quantities
        lines.append(f"`{level:<10}` `{format_volume(total_qty):<12}`")

    if walls:
        lines.append("\n🧱 **Order Walls Terdeteksi:**")
        for p, s, total_qty, exchange_info in walls:
            lines.append(f"{'🔻' if s == 'ask' else '🔺'} {s.capitalize()} Wall di `${p}`{exchange_info} → `{total_qty:.2f} BTC`")
    else:
        lines.append("\n_Tidak ada order wall signifikan terdeteksi._")

    imbalance_score, bid_qty, ask_qty = imbalance
    imbalance_emoji = '⚪ Netral'
    if imbalance_score > 0.1:
        imbalance_emoji = '🟢 Pembelian Dominan'
    elif imbalance_score < -0.1:
        imbalance_emoji = '🔴 Penjualan Dominan'

    lines.append("\n📊 **Ketidakseimbangan Order Book** (Top Levels):")
    lines.append(f"`{imbalance_emoji}` (`{imbalance_score:+.2f}`)")
    lines.append(f"Total Kuantitas Bid : `{bid_qty:.2f} BTC`")
    lines.append(f"Total Kuantitas Ask : `{ask_qty:.2f} BTC`")

    lines.append("\n_Catatan: Data kuantitas (Qty) ini adalah snapshot dari order book yang terbuka, bukan volume perdagangan historis._")

    return "\n".join(lines)

# --- FUNGSI UTAMA (MAIN LOOP) ---
async def main_loop():
    """
    Fungsi utama yang menjalankan siklus pemantauan, pengambilan, pemrosesan,
    dan pengiriman data order book.
    """
    if not EXCHANGES:
        logging.critical("Konfigurasi bursa kosong. Skrip tidak dapat berjalan.")
        return

    logging.info("Memulai pemantau order book BTC/USDT multi-bursa.")
    while True:
        # Hitung waktu penutupan candle berikutnya
        next_send_time = calculate_next_candle_close_time(CANDLE_INTERVAL_MINUTES)
        
        # Hitung durasi tidur hingga waktu pengiriman berikutnya
        now_utc = datetime.datetime.now(pytz.UTC)
        sleep_duration_initial = (next_send_time - now_utc).total_seconds()
        
        if sleep_duration_initial > 0:
            logging.info(f"Menunggu {sleep_duration_initial:.2f} detik hingga penutupan candle 15 menit berikutnya ({next_send_time.strftime('%Y-%m-%d %H:%M:%S UTC')}).")
            await asyncio.sleep(sleep_duration_initial)
        else:
            # Jika skrip dimulai tepat setelah waktu penutupan atau agak terlambat,
            # langsung lanjutkan atau tunggu sebentar ke siklus berikutnya.
            logging.info("Waktu penutupan candle 15 menit telah berlalu atau segera tiba. Melanjutkan ke pengambilan data.")
            # Beri jeda kecil untuk memastikan candle benar-benar "tutup" dan API mungkin diperbarui
            await asyncio.sleep(5) # Jeda 5 detik setelah waktu target

        start_fetch_time = datetime.datetime.now()
        logging.info(f"Memulai siklus pengambilan data baru pada {start_fetch_time.strftime('%Y-%m-%d %H:%M:%S')}. Ini akan disinkronkan dengan penutupan candle 15 menit Binance.")
        
        all_books_data = []
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_orderbook(session, name, config) for name, config in EXCHANGES.items()]
            all_books_data = await asyncio.gather(*tasks)

        valid_books = [book for book in all_books_data if book[2] or book[3]]

        if not valid_books:
            logging.error("Tidak ada data order book yang valid berhasil diambil dari bursa manapun. Mencoba lagi di siklus berikutnya.")
            # Jika gagal, tetap coba lagi pada penutupan candle berikutnya.
            continue

        # Inisialisasi harga default jika tidak ada data yang ditemukan
        current_spot_price = 0.0
        current_futures_price = 0.0

        spot_prices_fetched = []
        futures_prices_fetched = []

        # Kumpulkan harga bid tertinggi dari setiap bursa yang valid
        for name, is_spot, asks, bids in valid_books:
            if bids: # Pastikan ada bid
                # Ambil bid tertinggi (harga terbaik pembeli) sebagai representasi harga saat ini
                if is_spot:
                    spot_prices_fetched.append(bids[0][0])
                else:
                    futures_prices_fetched.append(bids[0][0])

        if spot_prices_fetched:
            current_spot_price = sum(spot_prices_fetched) / len(spot_prices_fetched)
        else:
            logging.warning("Harga spot tidak dapat ditentukan (mungkin semua bursa spot gagal diambil).")

        if futures_prices_fetched:
            current_futures_price = sum(futures_prices_fetched) / len(futures_prices_fetched)
        else:
            # Jika tidak ada harga futures yang berhasil diambil, gunakan harga spot sebagai fallback
            current_futures_price = current_spot_price if current_spot_price != 0 else 0.0
            if current_futures_price == 0:
                logging.warning("Harga futures tidak dapat ditentukan (mungkin semua bursa futures gagal diambil dan harga spot juga tidak ada).")


        grouped_order_book = aggregate_orderbooks(valid_books)
        
        walls_detected = detect_wall(grouped_order_book)
        imbalance_result = compute_imbalance(grouped_order_book)

        message_content = format_output(current_spot_price, current_futures_price, grouped_order_book, walls_detected, imbalance_result)
        await send_to_discord(message_content)

        end_fetch_time = datetime.datetime.now()
        fetch_duration = (end_fetch_time - start_fetch_time).total_seconds()
        logging.info(f"Pengambilan dan pengolahan data selesai dalam {fetch_duration:.2f} detik.")
        
        # Siklus akan kembali ke awal untuk menghitung waktu tidur berikutnya
        # sehingga selalu sinkron dengan penutupan candle
        
# --- JALANKAN SKRIP ---
if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt: # Baris 439
        logging.info("Skrip dihentikan secara manual (Ctrl+C).")
    except Exception as e:
        logging.critical(f"Kesalahan fatal pada main loop: {e}", exc_info=True)

