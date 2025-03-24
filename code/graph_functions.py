import networkx as nx
import pandas as pd
from collections import Counter, defaultdict
from tqdm import tqdm
import os

def split_graph_by_country(G, output_dir="graphs_by_country_optimized"):
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Group winner nodes by country
    country_winners = {}
    for node, data in tqdm(G.nodes(data=True), desc="Grouping nodes by country"):
        if data.get("type") == "Company" and data.get("bid_winner"):
            country = str(data.get("win_country_code", "Unknown"))[:2]
            country_winners.setdefault(country, []).append(node)

    # Step 2: Build a neighbor map only once
    print("Indexing neighbors...")
    neighbor_map = {}
    for node in tqdm(G.nodes(), desc="Indexing neighbors"):
        neighbors = set(G.successors(node)).union(G.predecessors(node))
        neighbor_map[node] = neighbors

    # Step 3: Build subgraphs efficiently using neighbor map
    split_graphs = {}
    for country, nodes in tqdm(country_winners.items(), desc="Creating subgraphs"):
        sub_nodes = set(nodes)
        for node in nodes:
            sub_nodes.update(neighbor_map.get(node, set()))

        subgraph = G.subgraph(sub_nodes).copy()
        split_graphs[country] = subgraph

        path = os.path.join(output_dir, f"graph_{country}.graphml")
        nx.write_graphml(subgraph, path)
        tqdm.write(f"Saved {country} ({len(subgraph.nodes)} nodes) → {path}")

    return split_graphs

def summarize_graph(G):
    print("\n===== Graph Summary =====")
    print(f"Total Nodes: {len(G.nodes)}")
    print(f"Total Edges: {len(G.edges)}")

    node_types = [G.nodes[n].get("type", "Unknown") for n in G.nodes]
    node_type_counts = Counter(node_types)
    print("\nNode Types:")
    for node_type, count in node_type_counts.items():
        print(f"  - {node_type}: {count}")

    edge_relationships = [G.edges[e].get("relationship", "Unknown") for e in G.edges]
    edge_type_counts = Counter(edge_relationships)
    print("\nEdge Types:")
    for rel, count in edge_type_counts.items():
        print(f"  - {rel}: {count}")

    flagged_count = sum(1 for n in G.nodes if G.nodes[n].get("flagged") == 'True')
    print(f"\nFlagged Companies: {flagged_count}")
    print("=========================\n")

def summarize_flagged_entity_relations_separately(G, top_k=10):
    print("===== FLAGGED ENTITY RELATION SUMMARY (by type) =====")

    flagged_companies = {}
    flagged_people = {}

    for node, data in tqdm(G.nodes(data=True), desc="Scanning nodes", unit="node"):
        if data.get("flagged") == "True" and data.get("type") == "Company":
            flagged_companies[node] = data.get("name", "Unknown")
        elif data.get("type") == "Flagged":
            flagged_people[node] = data.get("name", "Unknown")

    won_counts = defaultdict(int)
    owns_counts = defaultdict(int)
    controls_counts = defaultdict(int)
    subsidiary_counts = defaultdict(int)
    flagged_links = defaultdict(set)

    for u, v, d in tqdm(G.edges(data=True), desc="Scanning edges", unit="edge"):
        rel = d.get("relationship")
        if rel == "WON" and u in flagged_companies:
            won_counts[u] += 1
        if rel == "OWNS" and u in flagged_companies:
            owns_counts[u] += 1
        if rel == "CONTROLS" and u in flagged_companies:
            controls_counts[u] += 1
        if rel == "SUBSIDIARY_OF" and u in flagged_companies:
            subsidiary_counts[u] += 1
        if rel == "FLAGGED_LINK" and u in flagged_people:
            flagged_links[u].add(v)

    def print_top(mapping, label, name_map):
        print(f"\nTop {top_k} by {label}:")
        top = sorted(mapping.items(), key=lambda x: x[1] if isinstance(x[1], int) else len(x[1]), reverse=True)[:top_k]
        df = pd.DataFrame([
            {
                "Name": name_map.get(k, "Unknown"),
                "ID": k,
                label: v if isinstance(v, int) else len(v)
            }
            for k, v in top
        ])
        print(df.to_string(index=False))

    print(f"\nFlagged Companies Total: {len(flagged_companies)}")
    print(f"Flagged People Total: {len(flagged_people)}")

    print(f"\nFlagged Companies with WON edges: {len(won_counts)}")
    print(f"Flagged Companies with OWNS edges: {len(owns_counts)}")
    print(f"Flagged Companies with CONTROLS edges: {len(controls_counts)}")
    print(f"Flagged Companies with SUBSIDIARY_OF edges: {len(subsidiary_counts)}")
    print(f"Flagged People with FLAGGED_LINK connections: {len(flagged_links)}")

    print_top(won_counts, "Contracts Won", flagged_companies)
    print_top(owns_counts, "Companies Owned", flagged_companies)
    print_top(controls_counts, "Companies Controlled", flagged_companies)
    print_top(subsidiary_counts, "Subsidiary Links", flagged_companies)
    print_top(flagged_links, "Linked Companies", flagged_people)

    print("\n===== END OF SUMMARY =====\n")