#!/usr/bin/env python3
import os
import math
import json
import time
import random
import logging
import signal

from multiprocessing import Pool
import psycopg2
import psycopg2.extras
import psycopg2.errorcodes
import requests
from psycopg2.extras import execute_values

# ---------- CONFIGURATION ----------
DB_PARAMS = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", "5432"),
    "dbname":   os.getenv("DB_NAME", "procurement"),
    "user":     os.getenv("DB_USER", "wiktorrajca"),
    "password": os.getenv("DB_PASS", "pussinboots2"),
}
YENTE_URL      = "http://localhost:8000/match/default"
TABLE_NAME     = "polish_ted_matched"
BATCH_SIZE     = 100
NUM_WORKERS    = 10
MAX_RETRIES    = 3

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s: %(message)s")

# Graceful shutdown
stop_requested = False
def handle_sigint(signum, frame):
    global stop_requested
    stop_requested = True
    print("\nSIGINT received; finishing current work before exiting…")

signal.signal(signal.SIGINT, handle_sigint)

# ---------- UTILITIES ----------
def is_valid(v):
    if v is None:
        return False
    if isinstance(v, str) and v.strip().lower() in ("", "nan", "unknown", "-"):
        return False
    if isinstance(v, float) and math.isnan(v):
        return False
    return True

def build_entity_payload(row):
    """
    Build a Yente payload for a given row, falling back to identifiers-only
    if name or other fields are missing/invalid.
    """
    nid = str(row["win_nationalid"])
    props = {}

    # name
    name = row.get("win_name")
    if is_valid(name):
        props["name"] = [name.strip()]
    else:
        logging.warning(f"Missing/invalid name for ID {nid}; omitting name")

    # jurisdiction (first two letters of win_country_code)
    country = row.get("win_country_code")
    if is_valid(country) and country.strip() != "-":
        props["jurisdiction"] = [country.strip()[:2].upper()]

    # postalCode
    postal = row.get("win_postal_code")
    if is_valid(postal):
        props["postalCode"] = [postal.strip()]

    # town
    town = row.get("win_town")
    if is_valid(town):
        props["town"] = [town.strip()]

    # always include the identifier
    props.setdefault("identifiers", []).append({"source": "DB", "value": nid})

    return {"schema": "Company", "properties": props}

def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

# ---------- MATCHER (parallel) ----------
def match_batch(batch_ids):
    # fetch rows for this batch, ignoring any null IDs
    conn = psycopg2.connect(**DB_PARAMS)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    placeholders = ",".join(["%s"] * len(batch_ids))
    cur.execute(f"""
        SELECT win_nationalid, win_name, win_postal_code, win_town, win_country_code
          FROM {TABLE_NAME}
         WHERE win_nationalid IN ({placeholders})
           AND win_nationalid IS NOT NULL
    """, batch_ids)
    rows = cur.fetchall()
    conn.close()

    # build the query payloads, guaranteed one per ID
    queries = {}
    for row in rows:
        nid = str(row["win_nationalid"])
        queries[nid] = build_entity_payload(row)

    # call Yente
    try:
        resp = requests.post(YENTE_URL, json={"queries": queries}, timeout=30)
        resp.raise_for_status()
        raw = resp.json().get("responses", {})
    except Exception:
        logging.exception(f"Yente HTTP error for batch starting with IDs {batch_ids[:3]}…")
        raw = {}

    # ensure every ID appears in output
    out = {}
    for nid in queries:
        out[nid] = raw.get(nid, {"results": []})
    return out

# ---------- BULK WRITER (single-threaded) ----------
def write_results(results):
    if not results:
        return

    values = []
    for sid, res in results.items():
        hits = res.get("results", [])
        if hits:
            top = hits[0]
            mf, sc, me = True, top.get("score"), json.dumps(top.get("entity", {}))
        else:
            mf, sc, me = False, None, None
        values.append((mf, sc, me, sid))

    conn = psycopg2.connect(**DB_PARAMS)
    cur  = conn.cursor()
    query = f"""
    UPDATE {TABLE_NAME} AS t
    SET
      match_found    = v.match_found,
      score          = v.score::double precision,
      matched_entity = v.matched_entity::jsonb
    FROM (
      VALUES %s
    ) AS v(match_found, score, matched_entity, win_nationalid)
    WHERE t.win_nationalid = v.win_nationalid;
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            execute_values(cur, query, values, template=None, page_size=100)
            conn.commit()
            break
        except psycopg2.Error as e:
            if e.pgcode == psycopg2.errorcodes.DEADLOCK_DETECTED and attempt < MAX_RETRIES:
                logging.warning("Deadlock during bulk update, retry %d/%d", attempt, MAX_RETRIES)
                conn.rollback()
                time.sleep(random.uniform(0.1, 0.5))
                continue
            else:
                conn.rollback()
                raise
    conn.close()

# ---------- MAIN SCREENING FUNCTION ----------
def screen_entities():
    global stop_requested

    # 1) ensure screening columns exist
    conn = psycopg2.connect(**DB_PARAMS)
    cur  = conn.cursor()
    cur.execute(f"""
      ALTER TABLE {TABLE_NAME}
        ADD COLUMN IF NOT EXISTS match_found    BOOLEAN,
        ADD COLUMN IF NOT EXISTS score          DOUBLE PRECISION,
        ADD COLUMN IF NOT EXISTS matched_entity JSONB;
    """)
    conn.commit()

    # 2) fetch unscreened, non-null IDs
    cur.execute(f"""
      SELECT win_nationalid
        FROM {TABLE_NAME}
       WHERE match_found IS NULL
         AND win_nationalid IS NOT NULL
       ORDER BY win_nationalid;
    """)
    raw_ids = [r[0] for r in cur.fetchall()]
    conn.close()

    # drop any unexpected nulls (should be none)
    if any(i is None for i in raw_ids):
        count_null = sum(1 for i in raw_ids if i is None)
        logging.warning(f"Dropping {count_null} null win_nationalid rows before batching")
    ids = [i for i in raw_ids if i is not None]

    total = len(ids)
    if total == 0:
        print("No unscreened entities.")
        return

    batches = list(chunks(ids, BATCH_SIZE))
    print(f"{total} entities → {len(batches)} batches of {BATCH_SIZE} using {NUM_WORKERS} workers.")

    pool = Pool(processes=NUM_WORKERS)
    try:
        for idx, results in enumerate(pool.imap_unordered(match_batch, batches), start=1):
            if stop_requested:
                break
            write_results(results)
            if idx == 1 or idx % 50 == 0:
                print(f"Processed {idx}/{len(batches)} batches.")
    except KeyboardInterrupt:
        stop_requested = True
    finally:
        pool.terminate()
        pool.join()
        print("Completed." if not stop_requested else "Interrupted by user.")

if __name__ == "__main__":
    screen_entities()