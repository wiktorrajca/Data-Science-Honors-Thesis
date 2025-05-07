#!/usr/bin/env python3
import os
import sys
import json
import math
import logging
import argparse
import pandas as pd
import networkx as nx
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# show only warnings+ by default
logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s: %(message)s')

DATASET_SCOPES = ["sanctions", "peps", "regulatory", "crime"]

def is_valid(v):
    if v is None:
        return False
    if isinstance(v, str):
        vv = v.strip().lower()
        if vv in ("", "nan", "unknown", "-"):
            return False
    if isinstance(v, float) and math.isnan(v):
        return False
    return True

def build_entity_payload(nid, data):
    name = data.get("the_name")
    if not is_valid(name):
        return None

    t = data.get("type", "").strip().lower()
    if t == "company":
        schema = "Company"
    elif t == "person":
        schema = "Person"
    else:
        return None

    props = {"name": [name.strip()]}
    if schema == "Company" and is_valid(data.get("the_identifier")):
        props["registrationNumber"] = [data["the_identifier"].strip()]
    if is_valid(data.get("the_country")):
        c = data["the_country"].strip()
        props["jurisdiction" if schema == "Company" else "nationality"] = [c]
    if is_valid(data.get("the_postal_code")):
        props["postalCode"] = [data["the_postal_code"].strip()]
    if is_valid(data.get("the_town")):
        props["town"] = [data["the_town"].strip()]

    props.setdefault("identifiers", []).append({"source": "GraphID", "value": str(nid)})
    return {"schema": schema, "properties": props}

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def _match_batch(scope, batch, all_queries, yente_base):
    """
    Thread-friendly batch matcher.
    Returns list of (nid, record_dict).
    """
    url = f"{yente_base.rstrip('/')}/match/{scope}"
    payload = {"queries": {nid: all_queries[nid] for nid in batch}}
    try:
        r = requests.post(url, json=payload, timeout=60)
        r.raise_for_status()
    except Exception as e:
        logging.error("Scope %s batch failed: %s", scope, e)
        return [
            (nid, {
                "match_found": False,
                "dataset": scope
            })
            for nid in batch
        ]

    out = []
    reps = r.json().get("responses", {})
    for nid, res in reps.items():
        hits = res.get("results", [])
        if hits:
            top = hits[0]
            entity = top.get("entity", {})

            matched_name = entity.get("name") or \
                           entity.get("properties", {}).get("name", [None])[0] or \
                           entity.get("id")

            rec = {
                "match_found":   True,
                "matched_name":  matched_name,
                "matched_entity": entity,       # full dict for debugging
                "dataset":       scope,
                "score":         top.get("score")
            }
        else:
            rec = {
                "match_found": False,
                "dataset":     scope
            }
        out.append((nid, rec))
    return out

def screen_graph_multi_threads(
    graph: nx.DiGraph,
    yente_base: str,
    batch_size: int,
    max_workers: int,
    checkpoint_file: str,
    checkpoint_frequency: int
) -> nx.DiGraph:
    """
    Multi-threaded screening with CSV checkpointing & resume support.
    """
    # Load or initialize checkpoint
    if os.path.exists(checkpoint_file):
        df_exist = pd.read_csv(checkpoint_file, dtype=str)
        skip_pairs = set(zip(df_exist['node_id'], df_exist['dataset']))
    else:
        pd.DataFrame(
            columns=['node_id','dataset','match_found','matched_name','score','record_json']
        ).to_csv(checkpoint_file, index=False)
        skip_pairs = set()

    # Build all queries
    all_q = {}
    for nid, data in graph.nodes(data=True):
        p = build_entity_payload(nid, data)
        if p:
            all_q[nid] = p
    if not all_q:
        logging.error("No valid nodes to screen.")
        return graph

    nodes = list(all_q.keys())

    # Prepare pending batches
    batches = []
    for scope in DATASET_SCOPES:
        pending = [nid for nid in nodes if (nid, scope) not in skip_pairs]
        for b in chunks(pending, batch_size):
            batches.append((scope, b))

    total_batches = len(batches)
    print(f"{len(nodes)} nodes → {total_batches} pending batches across {len(DATASET_SCOPES)} scopes.")

    if max_workers is None:
        max_workers = os.cpu_count() or 4

    # Execute & checkpoint
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        future_map = {
            exe.submit(_match_batch, scope, batch, all_q, yente_base): (scope, batch)
            for scope, batch in batches
        }

        completed = 0
        buffer = []

        for fut in as_completed(future_map):
            completed += 1
            scope, batch = future_map[fut]
            try:
                results = fut.result()
            except Exception as e:
                logging.error("Batch %s/%s failed: %s", scope, batch, e)
                results = [(nid, {"match_found": False, "dataset": scope}) for nid in batch]

            for nid, rec in results:
                buffer.append({
                    'node_id':      nid,
                    'dataset':      rec.get('dataset', scope),
                    'match_found':  rec.get('match_found', False),
                    'matched_name': rec.get('matched_name', ''),
                    'score':        rec.get('score', None),
                    'record_json':  json.dumps(rec)
                })

            # flush buffer to CSV periodically or at end
            if checkpoint_frequency > 0 and (completed % checkpoint_frequency == 0 or completed == total_batches):
                pd.DataFrame(buffer).to_csv(
                    checkpoint_file,
                    mode='a',
                    index=False,
                    header=False
                )
                buffer.clear()
                print(f"  → checkpointed after {completed} batches")

            # progress log
            if completed == 1 or completed % 100 == 0 or completed == total_batches:
                print(f"  completed {completed}/{total_batches} batches")

    return graph

def main():
    parser = argparse.ArgumentParser(description="Screen a NetworkX DiGraph via Yente with checkpointing")
    parser.add_argument("graph_file", help="Input graph in GraphML format")
    parser.add_argument("--yente-base", default="http://localhost:8000",
                        help="Base URL for Yente matching service")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of nodes per batch")
    parser.add_argument("--max-workers", type=int, default=None, help="Number of threads to use")
    parser.add_argument("--checkpoint-file", default="screening_checkpoint.csv",
                        help="CSV file to save checkpointed results")
    parser.add_argument("--checkpoint-frequency", type=int, default=100,
                        help="How many batches between CSV writes")
    args = parser.parse_args()

    # load your graph
    if not os.path.isfile(args.graph_file):
        print(f"Graph file not found: {args.graph_file}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading graph from {args.graph_file}...")
    G = nx.read_graphml(args.graph_file)
    print(f"Graph has {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")

    screened = screen_graph_multi_threads(
        G,
        yente_base=args.yente_base,
        batch_size=args.batch_size,
        max_workers=args.max_workers,
        checkpoint_file=args.checkpoint_file,
        checkpoint_frequency=args.checkpoint_frequency
    )

    print("Screening completed. Checkpoint saved to:", os.path.abspath(args.checkpoint_file))

if __name__ == "__main__":
    main()