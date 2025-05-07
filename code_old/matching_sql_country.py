#!/usr/bin/env python3
import time
import signal
import psycopg2
from psycopg2.extras import DictCursor

# ── CONFIG ────────────────────────────────────────────────────────────────────────
DSN          = "dbname=procurement user=wiktorrajca password=pussinboots2 host=localhost port=5432"
BATCH_SIZE   = 500
LEV_DIST     = 3
TARGET_TABLE = "polish_fuzzy_matches"
MAX_LEV_LEN  = 255
# ─────────────────────────────────────────────────────────────────────────────────

keep_running = True

def handle_sigint(signum, frame):
    global keep_running
    print("\n⚠️  Caught Ctrl+C; will finish this batch then exit…")
    keep_running = False

def main():
    global keep_running
    signal.signal(signal.SIGINT, handle_sigint)

    # 1) Reading connection (streaming cursor—never commit here)
    conn_r = psycopg2.connect(DSN)
    cur_ted = conn_r.cursor(name="polish_ted_stream", cursor_factory=DictCursor)
    cur_ted.itersize = BATCH_SIZE
    cur_ted.execute("""
        SELECT ted_id, processed_name, win_country_code, win_town
        FROM polish_ted
        ORDER BY ted_id
    """)

    # 2) Writing connection (for EXPLAIN, MATCH and INSERT; we commit here)
    conn_w = psycopg2.connect(DSN)
    cur_explain = conn_w.cursor()
    cur_match   = conn_w.cursor()
    cur_ins     = conn_w.cursor()

    # enable the fuzzystrmatch extension if not present
    cur_match.execute("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;")
    conn_w.commit()

    total = 0
    first = True

    while keep_running:
        batch = cur_ted.fetchmany(BATCH_SIZE)
        if not batch:
            break

        for row in batch:
            ted_id  = row["ted_id"]
            raw     = row["processed_name"] or ""
            name    = raw[:MAX_LEV_LEN]             # truncate at 255
            ctry    = (row["win_country_code"] or "PL")[:2]
            pref2   = name[:2].upper()
            win_town= row["win_town"] or ""

            # ---- on the very first TED row, run EXPLAIN ANALYZE ----
            if first:
                print(f"\n🔍 EXPLAIN ANALYZE for TED {ted_id} ({ctry}/{pref2}, ≤{LEV_DIST} edits):\n")
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
                for plan_row in cur_explain.fetchall():
                    print(plan_row[0])
                print("\n✅ Plan looks good. Proceeding with full run.\n")
                first = False

            # ---- now do the real matching ----
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

            # insert all matches
            for bvdid, dist in matches:
                cur_ins.execute(f"""
INSERT INTO {TARGET_TABLE} (
  ted_id, bvdidnumber, sim_score,
  ted_country, orbis_country,
  country_match, type, win_town
) VALUES (%s, %s, %s, %s, %s, %s, 'Unknown', %s);
""", (
    ted_id,
    bvdid,
    dist,
    ctry,
    bvdid[:2],
    True,
    win_town
))
            conn_w.commit()
            elapsed_ms = (time.perf_counter() - t0) * 1000
            print(f"[TED {ted_id}] {len(matches)} matches in {elapsed_ms:.1f} ms")

            total += 1
            if not keep_running:
                break

    # cleanup
    cur_ted.close()
    conn_r.close()
    cur_explain.close()
    cur_match.close()
    cur_ins.close()
    conn_w.close()

    print(f"\n✅ Finished. Processed {total} Polish TED rows.")
    print(f"🔍 All matches are in `{TARGET_TABLE}`.")

if __name__ == "__main__":
    main()