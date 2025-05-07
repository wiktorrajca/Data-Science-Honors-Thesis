#!/usr/bin/env python3
"""
Main pipeline script:
  1) Match base CSV against additional CSVs via matcher.py
  2) Load and filter merged results via cleaning_matcher.py
  3) Build and initialize graph via graph_utilis.py
  4) Expand graph via load_and_match_all
  5) Screen the graph via screening.py and attach screening results
  6) Compute shadiness pipeline via shadiness_max_only.py
  7) Compute metrics via metrics.py and verify
  8) Save outputs at each stage

Usage:
    python main.py \
      --base-file data/raw/base.csv \
      --additional-dir data/raw/additional \
      --matched-dir data/processed/matched \
      --country FR \
      --shareholders-file data/raw/shareholders.csv \
      --subsidiaries-file data/raw/subsidiaries.csv \
      --basic-shareholders-file data/raw/basic_shareholders.csv \
      --output-dir data/processed \
      --checkpoint screening.csv \
      --yente-base http://localhost:8000
"""
import os
import sys
import glob
import argparse
import pandas as pd
import networkx as nx

# matcher
from matcher import merge_tables_on_processed_names
# cleaning and merging
from cleaning_matcher import load_and_filter_csvs
# graph utilities
from graph_utilis import (
    load_or_initialize_graph,
    add_procurement_winners,
    load_and_match_all,
    clean_node_attributes,
    save_graph as save_graph_util,
    attach_screening_results
)
# screening
from screening import screen_graph_multi_threads
# shadiness pipeline
from shadiness_max_only import run_shadiness_pipeline
# metrics
from metrics import (
    compute_shadiness,
    compute_expected_shadiness,
    compute_urgency_from_winner_values,
    verify_shadiness_computed_and_range,
    verify_expected_shadiness_computed,
    verify_urgency_computed_and_range
)


def main():
    parser = argparse.ArgumentParser(description="End-to-end procurement pipeline")

    # Matcher inputs
    parser.add_argument("--base-file", required=True,
                        help="Path to base CSV file")
    parser.add_argument("--additional-dir", required=True,
                        help="Directory with additional CSVs to match against")
    parser.add_argument("--matched-dir", required=True,
                        help="Directory to save matcher output CSVs")
    parser.add_argument("--similarity-threshold", type=float, default=0.8,
                        help="Threshold for fuzzy matching in matcher"),
    # Cleaning args
    parser.add_argument("--country", required=True,
                        help="2-letter country code for filtering (e.g. FR)")
    # Graph expansion inputs
    parser.add_argument("--shareholders-folder", required=True,
                        help="folder of shareholder relationships")
    parser.add_argument("--subsidiaries-folder", required=True,
                        help="folder of subsidiary relationships")
    parser.add_argument("--basic-shareholders-folder", required=True,
                        help="folder of basic shareholders data")
    # Outputs and screening
    parser.add_argument("--output-dir", required=True,
                        help="Directory to write outputs")
    parser.add_argument("--checkpoint", default="screening.csv",
                        help="Filename for screening checkpoint CSV (placed in output-dir)")
    parser.add_argument("--yente-base", default="http://localhost:8000",
                        help="Base URL for screening service")

    args = parser.parse_args()
    # create output dirs
    os.makedirs(args.matched_dir, exist_ok=True)
    os.makedirs(args.output_dir, exist_ok=True)

    # 1) Run matcher
    print("[1/8] Running matcher on additional CSVs...")
    base = args.base_file
    for add_path in glob.glob(os.path.join(args.additional_dir, "*.csv")):
        identifier = os.path.splitext(os.path.basename(add_path))[0]
        out_path = os.path.join(args.matched_dir, f"matched_{identifier}.csv")
        df_matched = merge_tables_on_processed_names(
            table1_path=base,
            table2_path=add_path,
            similarity_threshold=args.similarity_threshold
        )
        df_matched.to_csv(out_path, index=False)
        print(f"  • Saved matcher result to {out_path}")

    # 2) Clean & merge
    print("[2/8] Loading and filtering matched CSVs...")
    dfs = load_and_filter_csvs(args.matched_dir, args.country)
    if not dfs:
        print("No data after cleaning. Exiting.", file=sys.stderr)
        sys.exit(1)
    merged_df = pd.concat(dfs, ignore_index=True)
    merged_path = os.path.join(args.output_dir, "merged_filtered.csv")
    merged_df.to_csv(merged_path, index=False)
    print(f"  • Merged DataFrame saved to {merged_path}")

    # 3) Build & initialize graph
    print("[3/8] Initializing graph...")
    G = load_or_initialize_graph(args.country)
    add_procurement_winners(G, args.country, merged_df)
    init_graph_path = os.path.join(args.output_dir, "graph_initial.graphml")
    save_graph_util(G, init_graph_path)
    print(f"  • Initial graph saved to {init_graph_path}")

    # 4) Expand graph
    print("[4/8] Expanding graph with relationships...")
    G = load_and_match_all(
        G,
        args.shareholders_folder,
        args.subsidiaries_folder,
        args.basic_shareholders_folder
    )
    clean_node_attributes(G)
    exp_graph_path = os.path.join(args.output_dir, "graph_expanded.graphml")
    save_graph_util(G, exp_graph_path)
    print(f"  • Expanded graph saved to {exp_graph_path}")

    # 5) Screen graph
    print("[5/8] Screening graph nodes...")
    chk = os.path.join(args.output_dir, args.checkpoint)
    G = screen_graph_multi_threads(
        G,
        yente_base=args.yente_base,
        batch_size=50,
        checkpoint_file=chk,
        checkpoint_frequency=100
    )
    # attach screening scores
    df_screen = pd.read_csv(chk, dtype=str)
    G = attach_screening_results(G, df_screen)
    screened_path = os.path.join(args.output_dir, "graph_screened.graphml")
    save_graph_util(G, screened_path)
    print(f"  • Screened graph saved to {screened_path}")

    # 6) Shadiness pipeline
    print("[6/8] Computing shadiness pipeline...")
    run_shadiness_pipeline(G)

    # 7) Metrics calculations
    print("[7/8] Computing metrics via metrics.py...")
    compute_shadiness(G)
    compute_expected_shadiness(G)
    compute_urgency_from_winner_values(G)
    # verify
    ok1 = verify_shadiness_computed_and_range(G)
    ok2 = verify_expected_shadiness_computed(G)
    ok3 = verify_urgency_computed_and_range(G)
    metrics_report = os.path.join(args.output_dir, "metrics_report.txt")
    with open(metrics_report, 'w') as mf:
        mf.write(f"Shadiness verification: {ok1}")
        mf.write(f"Expected shadiness verification: {ok2}")
        mf.write(f"Urgency verification: {ok3}")
    print(f"  • Metrics report saved to {metrics_report}")

    # 8) Final save
    final_graph = os.path.join(args.output_dir, "graph_final.graphml")
    save_graph_util(G, final_graph)
    print(f"  • Final graph saved to {final_graph}")

    print("Pipeline complete!")

if __name__ == '__main__':
    main()
