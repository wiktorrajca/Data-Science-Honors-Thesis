#!/usr/bin/env python3
"""
Standalone script to compare two procurement graphs and export summary CSVs and optional CDF plot.
Usage:
    python compare_graphs.py \
      --graph1 path/to/graph1.graphml \
      --graph2 path/to/graph2.graphml \
      --name1 France --name2 Italy \
      --percentiles 25 50 75 100 \
      --output-dir results_folder \
      [--plot-cdf-path path/to/cdf_plot.png]
"""
import os
import sys
import argparse
import numpy as np
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt


def compare_graphs_summary_percentiles(G1, G2, percentiles, name1, name2):
    def parse_numeric(val):
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            try:
                return float(val.replace(',', '').strip())
            except:
                return None
        return None

    def get_procurement_df(G):
        records = []
        for node, attrs in G.nodes(data=True):
            if attrs.get('type') == 'Procurement':
                vals = []
                winners = []
                winner_ids = []
                for u, v, data in G.in_edges(node, data=True):
                    if data.get('relationship') == 'WON' and G.nodes[u].get('bid_winner') == 'True':
                        for attr in ('AWARD_VALUE_EURO_FIN_1', 'AWARD_EST_VALUE_EURO', 'AWARD_VALUE_EURO'):
                            num = parse_numeric(G.nodes[u].get(attr))
                            if num is not None:
                                vals.append(num)
                                break
                        winners.append(G.nodes[u].get('the_name', 'Unknown'))
                        winner_ids.append(u)
                records.append({
                    'procurement_id': node,
                    'winner_name': winners[0] if winners else 'Unknown',
                    'winner_id': winner_ids[0] if winner_ids else None,
                    'value': np.mean(vals) if vals else np.nan,
                    'urgency_linear': attrs.get('urgency_linear', np.nan),
                    'urgency': attrs.get('urgency', np.nan),
                    'expected_shadiness': attrs.get('expected_shadiness', np.nan)
                })
        return pd.DataFrame(records)

    def basic_stats(df, name):
        u = df['urgency'].dropna()
        es = df['expected_shadiness'].dropna()
        return {
            'Dataset': name,
            'Procurement Count': len(df),
            'Avg Urgency': u.mean(),
            'Median Urgency': u.median(),
            'Std Dev Urgency': u.std(),
            'Max Urgency': u.max(),
            'Avg Value (EUR)': df['value'].dropna().mean(),
            'Avg Expected Shadiness': es.mean(),
            'Median Expected Shadiness': es.median(),
            'Std Dev Expected Shadiness': es.std()
        }

    def get_percentile_row(df, percentile):
        uvals = df['urgency'].dropna()
        if uvals.empty:
            return {'winner_name': 'N/A', 'urgency': np.nan, 'value': np.nan}
        thr = np.percentile(uvals, percentile)
        elig = df[df['urgency'] >= thr]
        if elig.empty:
            return {'winner_name': 'N/A', 'urgency': np.nan, 'value': np.nan}
        row = elig.sort_values('urgency', ascending=True).iloc[0]
        return {'winner_name': row['winner_name'], 'urgency': row['urgency'], 'value': row['value']}

    df1 = get_procurement_df(G1)
    df2 = get_procurement_df(G2)

    stats = pd.DataFrame([basic_stats(df1, name1), basic_stats(df2, name2)]).set_index('Dataset').T

    summary = {'Percentile': []}
    for label in [f'{name1} Winner', f'{name1} Urgency', f'{name1} Value (EUR)',
                  f'{name2} Winner', f'{name2} Urgency', f'{name2} Value (EUR)']:
        summary[label] = []

    for p in percentiles:
        r1 = get_percentile_row(df1, p)
        r2 = get_percentile_row(df2, p)
        summary['Percentile'].append(f'{p}th')
        summary[f'{name1} Winner'].append(r1['winner_name'])
        summary[f'{name1} Urgency'].append(r1['urgency'])
        summary[f'{name1} Value (EUR)'].append(r1['value'])
        summary[f'{name2} Winner'].append(r2['winner_name'])
        summary[f'{name2} Urgency'].append(r2['urgency'])
        summary[f'{name2} Value (EUR)'].append(r2['value'])
    percentiles_df = pd.DataFrame(summary)

    def top_k_combined(dfA, dfB, G1, G2, k=3):
        topA = dfA.nlargest(k, 'urgency').reset_index(drop=True)
        topB = dfB.nlargest(k, 'urgency').reset_index(drop=True)
        def get_bvd(G, win_id):
            return G.nodes.get(win_id, {}).get('bvdidnumber', 'Unknown')
        return pd.DataFrame({
            f'{name1} Winner': topA['winner_name'],
            f'{name1} Urgency': topA['urgency'],
            f'{name1} Expected Shadiness': topA['expected_shadiness'],
            f'{name1} Value (€)': topA['value'],
            f'{name1} BVDID': [get_bvd(G1, wid) for wid in topA['winner_id']],
            f'{name2} Winner': topB['winner_name'],
            f'{name2} Urgency': topB['urgency'],
            f'{name2} Expected Shadiness': topB['expected_shadiness'],
            f'{name2} Value (€)': topB['value'],
            f'{name2} BVDID': [get_bvd(G2, wid) for wid in topB['winner_id']],
        })

    top_comb_df = top_k_combined(df1, df2, G1, G2, k=3)
    return stats, percentiles_df, top_comb_df


def plot_urgency_linear_cdf_comparison(G1, G2, name1='Graph A', name2='Graph B', save_path=None):
    def get_urgency_linear(G):
        return [
            float(attrs.get('urgency_linear'))
            for _, attrs in G.nodes(data=True)
            if attrs.get('type') == 'Procurement' and attrs.get('urgency_linear') is not None
        ]

    u1 = sorted(get_urgency_linear(G1))
    u2 = sorted(get_urgency_linear(G2))

    def compute_cdf(data):
        data = np.array(data)
        cdf_y = np.arange(1, len(data) + 1) / len(data)
        return data, cdf_y

    x1, y1 = compute_cdf(u1)
    x2, y2 = compute_cdf(u2)

    plt.figure(figsize=(10, 6))
    plt.plot(x1, y1, label=name1, linewidth=3)
    plt.plot(x2, y2, label=name2, linewidth=3, linestyle='--')
    plt.xlabel('Linear Urgency Score', fontsize=25)
    plt.ylabel('Cumulative Proportion', fontsize=25)
    plt.title('CDF of Linear Urgency Scores', fontsize=30)
    plt.legend(fontsize=25, loc='lower right')
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)
    plt.grid(True)
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path)
    plt.show()


def main():
    parser = argparse.ArgumentParser(description='Compare two procurement graphs and save CSV summaries')
    parser.add_argument('--graph1', required=True, help='Path to first GraphML file')
    parser.add_argument('--graph2', required=True, help='Path to second GraphML file')
    parser.add_argument('--name1', default='Graph A', help='Label for first graph')
    parser.add_argument('--name2', default='Graph B', help='Label for second graph')
    parser.add_argument('--percentiles', nargs='+', type=int, default=[25,50,75,100],
                        help='List of percentiles to compute')
    parser.add_argument('--output-dir', required=True, help='Directory to save CSV outputs')
    parser.add_argument('--plot-cdf-path', default=None,
                        help='If set, save the linear urgency CDF plot to this file path')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    G1 = nx.read_graphml(args.graph1)
    G2 = nx.read_graphml(args.graph2)

    stats_df, pct_df, top_df = compare_graphs_summary_percentiles(
        G1, G2, args.percentiles, args.name1, args.name2
    )

    stats_df.to_csv(os.path.join(args.output_dir, 'stats.csv'))
    pct_df.to_csv(os.path.join(args.output_dir, 'percentiles.csv'), index=False)
    top_df.to_csv(os.path.join(args.output_dir, 'top3.csv'), index=False)

    if args.plot_cdf_path:
        plot_urgency_linear_cdf_comparison(
            G1, G2, args.name1, args.name2, save_path=args.plot_cdf_path
        )

    print(f"Saved summary CSVs in {args.output_dir}")

if __name__ == '__main__':
    main()
