#!/usr/bin/env python3
import time
import signal
import sys
import argparse
from multiprocessing import Process
import psycopg2
from psycopg2.extras import DictCursor

# ── CONFIG ────────────────────────────────────────────────────────────────────────
DSN          = "dbname=procurement user=wiktorrajca password=pussinboots2 host=localhost port=5432"
BATCH_SIZE   = 500
LEV_DIST     = 3
TARGET_TABLE = "polish_fuzzy_matches"
MAX_LEV_LEN  = 255
# ─────────────────────────────────────────────────────────────────────────────────

# global flag used by each worker
keep_running = True

def handle_sigint(signum, frame):
    global keep_running
    keep_running = False

def worker(worker_idx, num_workers, resume_from):
    global keep_running
    keep_running = True
    signal.signal(signal.SIGINT, handle_sigint)

    # — Writer connection: to get resume point, EXPLAIN, MATCH & INSERT —
    conn_w      = psycopg2.connect(DSN)
    cur_start   = conn_w.cursor()
    cur_explain = conn_w.cursor()
    cur_match   = conn_w.cursor()
    cur_ins     = conn_w.cursor()

    # Determine where to resume
    if resume_from is not None:
        last_processed = resume_from
        print(f"[W{worker_idx}] Forced resume from TED ID > {last_processed}")
    else:
        cur_start.execute(f"""
            SELECT COALESCE(MAX(ted_id), 0)
            FROM {TARGET_TABLE}
            WHERE (ted_id %% %s) = %s;
        """, (num_workers, worker_idx))
        last_processed = cur_start.fetchone()[0]
        print(f"[W{worker_idx}] Resuming from TED ID > {last_processed}")

    # Ensure fuzzystrmatch is available
    cur_match.execute("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;")
    conn_w.commit()

    # — Reader connection: streaming cursor (never commit here) —
    conn_r  = psycopg2.connect(DSN)
    cur_ted = conn_r.cursor(name=f"stream_{worker_idx}", cursor_factory=DictCursor)
    cur_ted.itersize = BATCH_SIZE
    cur_ted.execute("""
        SELECT ted_id, processed_name, win_country_code, win_town
        FROM polish_ted
        WHERE (ted_id %% %s) = %s
          AND ted_id > %s
        ORDER BY ted_id
    """, (num_workers, worker_idx, last_processed))

    first     = True
    processed = 0

    try:
        while keep_running:
            batch = cur_ted.fetchmany(BATCH_SIZE)
            if not batch:
                break

            for row in batch:
                ted_id   = row["ted_id"]
                raw      = row["processed_name"] or ""
                name     = raw[:MAX_LEV_LEN]
                ctry     = (row["win_country_code"] or "")[:2]
                pref2    = name[:2].upper()
                win_town = row["win_town"] or ""

                # First‐row EXPLAIN by worker 0 only
                if worker_idx == 0 and first:
                    print(f"\n[W0] 🔍 EXPLAIN ANALYZE for TED {ted_id} ({ctry}/{pref2}, ≤{LEV_DIST} edits):\n")
                    cur_explain.execute("""
EXPLAIN (ANALYZE, BUFFERS)
SELECT 1
FROM orbis_names
WHERE substring(bvdidnumber FROM 1 FOR 2)         = %s
  AND upper(substring(processed_name FROM 1 FOR 2)) = %s
  AND levenshtein_less_equal(
        LEFT(processed_name, %s),
        %s,
        %s
      ) <= %s
LIMIT 1;
""", (ctry, pref2, MAX_LEV_LEN, name, LEV_DIST, LEV_DIST))
                    for plan in cur_explain:
                        print(plan[0])
                    print(f"[W0] ✅ Plan validated—continuing full run.\n")
                    first = False

                t0 = time.perf_counter()
                cur_match.execute("""
SELECT 
  bvdidnumber,
  levenshtein_less_equal(
    LEFT(processed_name, %s),
    %s,
    %s
  ) AS dist
FROM orbis_names
WHERE substring(bvdidnumber FROM 1 FOR 2)         = %s
  AND upper(substring(processed_name FROM 1 FOR 2)) = %s
  AND levenshtein_less_equal(
        LEFT(processed_name, %s),
        %s,
        %s
      ) <= %s;
""", (
    MAX_LEV_LEN, name, LEV_DIST,
    ctry,        pref2,
    MAX_LEV_LEN, name, LEV_DIST, LEV_DIST
))
                matches = cur_match.fetchall()

                for bvdid, dist in matches:
                    cur_ins.execute(f"""
INSERT INTO {TARGET_TABLE} (
  ted_id, bvdidnumber, sim_score,
  ted_country, orbis_country,
  country_match, type, win_town
) VALUES (%s, %s, %s, %s, %s, %s, 'Unknown', %s);
""", (
    ted_id, bvdid, dist,
    ctry,   bvdid[:2], True, win_town
))
                conn_w.commit()

                elapsed = (time.perf_counter() - t0) * 1000
                print(f"[W{worker_idx}] TED {ted_id}: {len(matches)} matches in {elapsed:.1f} ms")
                processed += 1

                if not keep_running:
                    break

    except Exception as e:
        print(f"[W{worker_idx}] ERROR on TED {ted_id}: {e}", file=sys.stderr)
    finally:
        # Clean up
        cur_ted.close(); conn_r.close()
        cur_start.close(); cur_explain.close()
        cur_match.close(); cur_ins.close(); conn_w.close()
        print(f"[W{worker_idx}] ✅ Exiting—processed {processed} rows.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-workers", type=int, default=1,
                        help="Total parallel workers")
    parser.add_argument("--resume-from", type=int, default=None,
                        help="If set, skip all ted_id ≤ this value")
    args = parser.parse_args()

    procs = []
    for i in range(args.num_workers):
        p = Process(target=worker, args=(i, args.num_workers, args.resume_from))
        p.start()
        procs.append(p)

    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        print("🛑 Main caught Ctrl+C, terminating workers…", file=sys.stderr)
        for p in procs:
            p.terminate()
        for p in procs:
            p.join()

if __name__ == "__main__":
    main()