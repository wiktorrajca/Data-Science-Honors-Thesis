#!/usr/bin/env python3
"""
Standalone script to compute and export procurement urgency diagnostics from a NetworkX graph.
Usage:
    python diagnostic_report.py --graph path/to/graph.graphml --output-dir results_folder
"""
import os
import sys
import re
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx


def ensure_folder(folder):
    os.makedirs(folder, exist_ok=True)


def parse_numeric(val):
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        s = re.sub(r'[^0-9.]', '', val)
        try:
            return float(s)
        except ValueError:
            return None
    return None


def build_procurement_df(G, value_attrs=('AWARD_VALUE_EURO_FIN_1','AWARD_EST_VALUE_EURO','AWARD_VALUE_EURO')):
    records = []
    for node, attrs in G.nodes(data=True):
        if attrs.get('type') == 'Procurement':
            vals = []
            for u, v, data in G.in_edges(node, data=True):
                if data.get('relationship')=='WON' and G.nodes[u].get('bid_winner')=='True':
                    for attr in value_attrs:
                        num = parse_numeric(G.nodes[u].get(attr))
                        if num is not None:
                            vals.append(num)
                            break
            records.append({
                'procurement_id': node,
                'value': np.mean(vals) if vals else np.nan,
                'expected_shadiness': attrs.get('expected_shadiness', np.nan),
                'urgency': attrs.get('urgency', np.nan),
                'urgency_linear': attrs.get('urgency_linear', np.nan),
                'num_candidates': sum(1 for u, v, d in G.in_edges(node, data=True)
                                      if d.get('relationship')=='WON' and G.nodes[u].get('bid_winner')=='True')
            })
    return pd.DataFrame(records)


def compute_robust_urgency(G,
                           value_attrs=('AWARD_VALUE_EURO_FIN_1','AWARD_EST_VALUE_EURO','AWARD_VALUE_EURO'),
                           a=0.5, b=2.0, denom_percentile=99, median_fill=True):
    raw_vals = []
    proc_vals = {}
    for p, attrs in G.nodes(data=True):
        if attrs.get('type') == 'Procurement':
            vals = []
            for u, v, data in G.in_edges(p, data=True):
                if data.get('relationship')=='WON' and G.nodes[u].get('bid_winner')=='True':
                    for attr in value_attrs:
                        num = parse_numeric(G.nodes[u].get(attr))
                        if num is not None:
                            vals.append(num)
                            break
            if vals:
                proc_vals[p] = float(np.mean(vals))
                raw_vals.append(proc_vals[p])
    if not raw_vals:
        raise ValueError("No numeric values found on winner nodes.")

    robust_vmax = np.percentile(raw_vals, denom_percentile)
    median_v = np.median(raw_vals)

    for p, attrs in G.nodes(data=True):
        if attrs.get('type') == 'Procurement':
            v = proc_vals.get(p, None)
            if v is None:
                v = median_v if median_fill else 0.0
            v_clamped = min(v, robust_vmax)
            x = v_clamped / robust_vmax if robust_vmax > 0 else 0.0
            gamma = a + (b - a) * x
            v_scaled = x ** gamma if x > 0 else 0.0
            r = float(attrs.get('expected_shadiness', 0.0))
            G.nodes[p]['urgency'] = r * v_scaled
            G.nodes[p]['urgency_linear'] = r * x


def urgency_bucket_counts_df(df, col='urgency', bucket_size=0.1):
    urgencies = df[col].dropna().values
    edges = np.arange(0, 1 + bucket_size, bucket_size)
    counts, _ = np.histogram(urgencies, bins=edges)
    buckets = [f"[{edges[i]:.1f},{edges[i+1]:.1f})" for i in range(len(edges)-1)]
    buckets[-1] = f"[{edges[-2]:.1f},{edges[-1]:.1f}]"
    return pd.DataFrame({'Bucket': buckets, 'Count': counts})


def plot_and_save_boxplot(df, folder):
    bins = [0, 1, 2, 5, 10, np.inf]
    labels = ['1', '2', '3-5', '6-10', '>10']
    df['candidate_bin'] = pd.cut(df['num_candidates'], bins=bins, labels=labels, right=True)

    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    boxprops = dict(linewidth=2)
    medianprops = dict(linewidth=2, color='firebrick')
    whiskerprops = dict(linewidth=2)
    capprops = dict(linewidth=2)

    df.boxplot(column='expected_shadiness', by='candidate_bin', ax=axes[0, 0],
               boxprops=boxprops, medianprops=medianprops,
               whiskerprops=whiskerprops, capprops=capprops)
    axes[0, 0].set_title('Expected Shadiness by Candidate Bin')
    axes[0, 0].set_xlabel('Candidates')
    axes[0, 0].set_ylabel('Expected Shadiness')

    df.boxplot(column='urgency', by='candidate_bin', ax=axes[0, 1],
               boxprops=boxprops, medianprops=medianprops,
               whiskerprops=whiskerprops, capprops=capprops)
    axes[0, 1].set_title('Urgency (nonlinear) by Candidate Bin')
    axes[0, 1].set_xlabel('Candidates')
    axes[0, 1].set_ylabel('Urgency')

    df.boxplot(column='urgency_linear', by='candidate_bin', ax=axes[1, 0],
               boxprops=boxprops, medianprops=medianprops,
               whiskerprops=whiskerprops, capprops=capprops)
    axes[1, 0].set_title('Urgency (linear) by Candidate Bin')
    axes[1, 0].set_xlabel('Candidates')
    axes[1, 0].set_ylabel('Urgency Linear')

    axes[1, 1].axis('off')

    plt.suptitle('')
    plt.tight_layout()
    fig_path = os.path.join(folder, 'boxplots_by_candidate_bin.png')
    fig.savefig(fig_path)
    plt.close(fig)


def compute_urgency_cdf_table(df, col='urgency', step=0.1):
    thresholds = np.arange(0, 1 + step, step)
    cdf = []
    urg = df[col].dropna()
    for t in thresholds:
        cdf.append({'urgency_threshold': t, 'cumulative_proportion': (urg <= t).mean()})
    return pd.DataFrame(cdf)


def plot_and_save_cdf(df, folder):
    for col, name in [('urgency', 'nonlinear'), ('urgency_linear', 'linear')]:
        sorted_u = np.sort(df[col].dropna().values)
        cdf = np.arange(1, len(sorted_u) + 1) / len(sorted_u)
        fig, ax = plt.subplots()
        ax.plot(sorted_u, cdf)
        ax.set_xlabel(f'Urgency ({name})')
        ax.set_ylabel('Cumulative Proportion')
        ax.set_title(f'CDF of Procurement Urgency ({name})')
        ax.grid(True)
        plt.tight_layout()
        fig_path = os.path.join(folder, f'urgency_cdf_{name}.png')
        fig.savefig(fig_path)
        plt.close(fig)


def top_n_urgency(df, n=10, folder=None):
    top_n_nonlinear = df.nlargest(n, 'urgency')[['procurement_id', 'value', 'expected_shadiness', 'urgency']]
    top_n_linear = df.nlargest(n, 'urgency_linear')[['procurement_id', 'value', 'expected_shadiness', 'urgency_linear']]
    if folder:
        top_n_nonlinear.to_csv(os.path.join(folder, 'top10_procurements_by_urgency_nonlinear.csv'), index=False)
        top_n_linear.to_csv(os.path.join(folder, 'top10_procurements_by_urgency_linear.csv'), index=False)
    return top_n_nonlinear, top_n_linear


def summary_statistics(df):
    stats = {
        'expected_shadiness_mean': df['expected_shadiness'].mean(),
        'expected_shadiness_median': df['expected_shadiness'].median(),
        'urgency_mean': df['urgency'].mean(),
        'urgency_median': df['urgency'].median(),
        'urgency_linear_mean': df['urgency_linear'].mean(),
        'urgency_linear_median': df['urgency_linear'].median()
    }
    return pd.DataFrame([stats])


def generate_diagnostic_report_to_folder(G, folder):
    ensure_folder(folder)
    # compute urgency based on current graph
    compute_robust_urgency(G)
    df = build_procurement_df(G)

    # buckets
    buckets = urgency_bucket_counts_df(df, col='urgency')
    buckets.to_csv(os.path.join(folder, 'urgency_buckets.csv'), index=False)
    buckets_linear = urgency_bucket_counts_df(df, col='urgency_linear')
    buckets_linear.to_csv(os.path.join(folder, 'urgency_linear_buckets.csv'), index=False)

    # boxplots
    plot_and_save_boxplot(df, folder)

    # CDF tables and plots
    cdf_df = compute_urgency_cdf_table(df, col='urgency')
    cdf_df.to_csv(os.path.join(folder, 'urgency_cdf_nonlinear_table.csv'), index=False)
    cdf_df_lin = compute_urgency_cdf_table(df, col='urgency_linear')
    cdf_df_lin.to_csv(os.path.join(folder, 'urgency_cdf_linear_table.csv'), index=False)
    plot_and_save_cdf(df, folder)

    # top N
    top_n_urgency(df, n=10, folder=folder)

    # summary
    summary_df = summary_statistics(df)
    summary_df.to_csv(os.path.join(folder, 'summary_statistics.csv'), index=False)

    print(f"✅ Diagnostic report exported to '{folder}' folder.")


def main():
    parser = argparse.ArgumentParser(description="Generate procurement urgency diagnostics from a graph.")
    parser.add_argument("--graph", required=True, help="Path to input graph (GraphML)")
    parser.add_argument("--output-dir", required=True, help="Folder to write all results into")
    args = parser.parse_args()

    if not os.path.isfile(args.graph):
        print(f"Error: Graph file not found: {args.graph}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading graph from {args.graph}...")
    G = nx.read_graphml(args.graph)
    print(f"Graph loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges.")

    generate_diagnostic_report_to_folder(G, args.output_dir)

if __name__ == "__main__":
    main()
