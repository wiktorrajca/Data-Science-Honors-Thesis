import psycopg2
import datetime
import sys
import signal
from multiprocessing import Pool, cpu_count

# === CONFIGURATION ===
DB_NAME = "procurement"
DB_USER = "wiktorrajca"
DB_PASSWORD = "pussinboots2"
DB_HOST = "localhost"
DB_PORT = "5432"

TED_BATCH_SIZE = 500 
ALLOWED_COUNTRIES = [
    'PL', 'FR', 'DE', 'RO', 'UK', 'ES', 'IT', 'CZ', 'SI', 'BG', 'LT', 'SE', 'LV', 'NL', 'HU', 'BE',
    'HR', 'DK', 'GR', 'FI', 'AT', 'NO', 'SK', 'PT', 'EE', 'IE', 'CH', 'MK', 'CY', 'LU', 'MT', 'US',
    'IS', 'RE', 'GP', 'CA', 'MQ', 'CN', 'LI', 'RU', 'TR', 'IN', 'AW', 'AF', 'GF', 'IL', 'RS', 'JP',
    'AM', 'AE', 'AU', 'KR', 'UA', 'ZA', 'SG', 'HK', 'SM', 'MC', 'YT', 'MD']

# === Graceful shutdown handler ===
def signal_handler(sig, frame):
    print("\n🛑 Caught interrupt signal. Shutting down gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def get_country_codes():
    return ALLOWED_COUNTRIES

def fetch_ted_rows_by_country(country_code):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()
    cur.execute("""
        SELECT ted_id, processed_name, win_country_code, win_town
        FROM ted
        WHERE SUBSTRING(win_country_code FROM 1 FOR 2) = %s
        ORDER BY ted_id;
    """, (country_code,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def log_timing(cur, ted_id, duration):
    cur.execute("""
        INSERT INTO ted_match_debug (ted_id, duration_ms, timestamp)
        VALUES (%s, %s, now());
    """, (ted_id, duration))

def log_country_progress(cur, country_code, batch_count):
    cur.execute("""
        INSERT INTO ted_match_progress (country_code, processed_rows)
        VALUES (%s, %s)
        ON CONFLICT (country_code) DO UPDATE
        SET processed_rows = ted_match_progress.processed_rows + EXCLUDED.processed_rows;
    """, (country_code, batch_count))

def get_total_progress():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        cur.execute("SELECT SUM(processed_rows) FROM ted_match_progress;")
        total = cur.fetchone()[0] or 0
        cur.close()
        conn.close()
        return total
    except:
        return 0

def process_country(country_code):
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        cur.execute("SET pg_trgm.similarity_threshold = 0.8;")

        ted_rows = fetch_ted_rows_by_country(country_code)
        total_inserted = 0
        total_batches = len(ted_rows) // TED_BATCH_SIZE + (1 if len(ted_rows) % TED_BATCH_SIZE != 0 else 0)

        ted_rows_processed = 0
        total_ted_rows = len(ted_rows)

        for i in range(0, len(ted_rows), TED_BATCH_SIZE):
            batch = ted_rows[i:i+TED_BATCH_SIZE]

            for ted_id, t_name, win_country, win_town in batch:
                if not t_name:
                    continue

                start_time = datetime.datetime.now()
                matches = []

                cur.execute("""
                    SELECT bvdidnumber, processed_name
                    FROM orbis_names
                    WHERE processed_name %% %s
                      AND SUBSTRING(bvdidnumber FROM 1 FOR 2) = %s;
                """, (t_name, country_code))
                matches = cur.fetchall()

                if not matches:
                    cur.execute("""
                        SELECT bvdidnumber, processed_name
                        FROM orbis_names
                        WHERE processed_name %% %s;
                    """, (t_name,))
                    matches = cur.fetchall()

                for bvdid, o_name in matches:
                    cur.execute("""
                        INSERT INTO all_fuzzy_matches (
                            ted_id, bvdidnumber, sim_score,
                            ted_country, orbis_country, country_match, type, win_town
                        )
                        VALUES (%s, %s, similarity(%s, %s), %s, %s, %s, %s, %s);
                    """, (
                        ted_id, bvdid,
                        t_name, o_name,
                        win_country[:2] if win_country else None,
                        bvdid[:2],
                        (win_country[:2] == bvdid[:2]) if win_country else False,
                        'Unknown', win_town
                    ))
                    total_inserted += 1

                duration = (datetime.datetime.now() - start_time).total_seconds() * 1000
                log_timing(cur, ted_id, duration)
                ted_rows_processed += 1

            conn.commit()
            cur.execute("INSERT INTO match_log (batch_prefix, inserted_rows, status) VALUES (%s, %s, 'done');", (country_code + f"_batch_{i}", total_inserted))
            log_country_progress(cur, country_code, len(batch))
            conn.commit()

            batch_index = i // TED_BATCH_SIZE
            if batch_index % 10 == 0:
                overall_progress = get_total_progress()
                print(f"[{datetime.datetime.now()}] ✅ Country {country_code}: completed batch {batch_index + 1}/{total_batches}, processed {ted_rows_processed}/{total_ted_rows} TED rows, {total_batches - batch_index - 1} batches remaining. Total across countries: {overall_progress} rows processed.")

        print(f"[{datetime.datetime.now()}] ✅ Country {country_code}: {total_inserted} rows inserted, all {total_ted_rows} TED rows processed.")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"[{country_code}] ❌ Error: {e}")

if __name__ == "__main__":
    country_codes = get_country_codes()
    num_workers = min(cpu_count(), 10)
    with Pool(num_workers) as pool:
        pool.map(process_country, country_codes)

    print("\nAll TED rows processed.")
