import networkx as nx
import pandas as pd
from collections import Counter, defaultdict, deque
from tqdm import tqdm
import os
import copy

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

def summarize_flagged_entities_to_csv(G, output_path="flagged_entity_summary.csv"):
    """
    Summarizes flagged companies and people along with their graph relationships (WON, OWNS, CONTROLS, etc.).
    Outputs the summary as a CSV.
    
    Parameters:
        G (networkx.Graph): The input graph.
        output_path (str): File path to save the summary CSV.
    
    Returns:
        pd.DataFrame: The summary DataFrame.
    """
    flagged_summary = []

    for node, data in G.nodes(data=True):
        if data.get("flagged") == "True" or data.get("type") == "Flagged":
            node_id = node
            node_type = data.get("type", "Unknown")
            name = data.get("name", "Unknown")

            # Try to extract the sanction_id if present in flagged_reason
            flagged_reason = data.get("flagged_reason", "")
            match = re.search(r"Matched to flagged entity (\S+)", flagged_reason)
            sanction_id = match.group(1) if match else ""

            # Count relationships from this node
            won_count = 0
            owns_count = 0
            controls_count = 0
            subsidiary_count = 0
            flagged_links = 0

            for _, tgt, edata in G.out_edges(node, data=True):
                rel = edata.get("relationship")
                if rel == "WON":
                    won_count += 1
                elif rel == "OWNS":
                    owns_count += 1
                elif rel == "CONTROLS":
                    controls_count += 1
                elif rel == "SUBSIDIARY_OF":
                    subsidiary_count += 1
                elif rel == "FLAGGED_LINK":
                    flagged_links += 1

            for src, _, edata in G.in_edges(node, data=True):
                rel = edata.get("relationship")
                if rel == "FLAGGED_LINK":
                    flagged_links += 1

            flagged_summary.append({
                "ID": node_id,
                "Sanction_ID": sanction_id,
                "Name": name,
                "Type": node_type,
                "Contracts_Won": won_count,
                "Owns_Companies": owns_count,
                "Controls_Companies": controls_count,
                "Subsidiary_Links": subsidiary_count,
                "Flagged_Links": flagged_links
            })

    df = pd.DataFrame(flagged_summary)
    df.to_csv(output_path, index=False)
    print(f"✅ Summary saved to {output_path}")
    return df

def clean_node_attributes(G):
    """
    Ensures all node attributes are stored as strings and missing values are handled.
    """
    for node in G.nodes:
        for attr, value in G.nodes[node].items():
            if pd.isna(value) or value is None:  # Replace NaN/None with 'Unknown'
                G.nodes[node][attr] = "Unknown"
            else:
                G.nodes[node][attr] = str(value)  # Convert everything to a string

def save_graph(G, country_code):

    """
    Saves the graph to GraphML format.
    """
    clean_node_attributes(G)  # Ensure consistent types before saving
    nx.write_graphml(G, f"procurement_graph_{country_code}_clean.graphml")
    print(f"✅ Graph saved to procurement_graph_{country_code}_clean.graphml")

def label_and_clean_matched_companies_create_new(G):
    """
    Creates a cleaned copy of the graph with company nodes labeled by match confidence,
    removes 'SuperLow' confidence matches, and recursively removes disconnected Orbis nodes.
    """
    G_cleaned = copy.deepcopy(G)

    # Step 1: Group company nodes by name
    name_to_nodes = defaultdict(list)
    name_to_unique_ids = defaultdict(set)

    for node, data in G.nodes(data=True):
        if data.get("type") == "Company" and data.get("bid_winner") == "True":
            name = data.get("name")
            national_id = data.get("win_nationalid")
            name_to_nodes[name].append(node)
            if national_id:
                name_to_unique_ids[name].add(national_id)

    # Step 2: Label confidence
    last_win_country = None
    superlow_nodes = set()
    for name, nodes in name_to_nodes.items():
        expected_matches = len(name_to_unique_ids[name])
        for node in nodes:
            data = G_cleaned.nodes[node]
            bvd_country = str(data.get("bvdidnumber", ""))[:2].upper()
            win_country = str(data.get("win_country_code", ""))[:2].upper()
            last_win_country = win_country

            country_match = bvd_country == win_country
            total_matches = len(nodes)

            # Determine confidence
            if country_match and total_matches == 1:
                confidence = "Very High"
            elif country_match and total_matches <= expected_matches:
                confidence = "High"
            elif not country_match and total_matches <= expected_matches:
                confidence = "Medium"
            elif country_match and total_matches >= expected_matches:
                confidence = "Low"
            else:
                confidence = "SuperLow"

            data["match_confidence"] = confidence

            if confidence == "SuperLow":
                superlow_nodes.add(node)

    # Step 3: Remove all SuperLow nodes and clean neighbors
    print(f"🧹 Removing {len(superlow_nodes)} SuperLow nodes and cleaning neighbors...")
    G_cleaned.remove_nodes_from(superlow_nodes)

    # Step 4: Keep only nodes reachable from procurement
    reachable = set()
    for node, data in G_cleaned.nodes(data=True):
        if data.get("type") == "Procurement":
            queue = deque([node])
            visited = set()
            while queue:
                current = queue.popleft()
                if current in visited:
                    continue
                visited.add(current)
                reachable.add(current)
                neighbors = set(G_cleaned.successors(current)).union(G_cleaned.predecessors(current))
                queue.extend(neighbors - visited)

    all_nodes = set(G_cleaned.nodes())
    unreachable_nodes = all_nodes - reachable
    G_cleaned.remove_nodes_from(unreachable_nodes)

    # Step 5: Remove final orphans
    orphans = [n for n in G_cleaned.nodes if G_cleaned.degree(n) == 0]
    G_cleaned.remove_nodes_from(orphans)

    save_graph(G_cleaned, last_win_country)
    print(f"✅ Cleaned graph created. Removed {len(superlow_nodes)} SuperLow nodes and {len(unreachable_nodes)} unreachable nodes.")
    return G_cleaned

def check_graph_integrity(G):
    """
    Checks if the graph meets integrity expectations:
    - No orphan nodes (nodes with no edges)
    - Only isolated pairs are connected by 'WON' edges
    - Each procurement node has only one incoming 'WON' edge
    """
    print("🧪 Running graph integrity checks...")

    # 1. Orphan nodes
    orphan_nodes = [n for n in G.nodes if G.degree(n) == 0]
    if orphan_nodes:
        print(f"❌ Found {len(orphan_nodes)} orphan nodes.")
    else:
        print("✅ No orphan nodes found.")

    # 2. Isolated pairs must be connected only by 'WON' edges
    only_one_edge = []
    for u, v in G.edges:
        if G.degree(u) == 1 and G.degree(v) == 1:
            rel = G.edges[u, v].get("relationship")
            if rel != "WON":
                only_one_edge.append((u, v, rel))

    if only_one_edge:
        print(f"❌ Found {len(only_one_edge)} node pairs connected only by non-WON edges.")
        for u, v, rel in only_one_edge[:5]:  # Show a few examples
            print(f"   - ({u}, {v}) via {rel}")
    else:
        print("✅ All isolated pairs are connected by 'WON' edges only.")

    # 3. Each Procurement node should have at most one incoming 'WON' edge
    bad_procurements = []
    for node, data in G.nodes(data=True):
        if data.get("type") == "Procurement":
            won_in_edges = [
                (u, v) for u, v in G.in_edges(node)
                if G.edges[u, v].get("relationship") == "WON"
            ]
            if len(won_in_edges) > 1:
                bad_procurements.append((node, len(won_in_edges)))

    if bad_procurements:
        print(f"❌ Found {len(bad_procurements)} procurement nodes with multiple 'WON' edges.")
        for pid, count in bad_procurements[:5]:
            print(f"   - Procurement {pid} has {count} winners.")
    else:
        print("✅ All procurements have at most one winning company.")

    print("🧪 Integrity check complete.\n")

def check_for_superlow_winners(G):
    """
    Checks whether any procurement-winning companies still have 'SuperLow' match_confidence.
    Returns a list of such nodes and prints a summary.
    """
    superlow_nodes = []

    for node, data in G.nodes(data=True):
        if data.get("type") == "Company" and data.get("bid_winner") == "True":
            if data.get("match_confidence") == "SuperLow":
                superlow_nodes.append((node, data.get("name", "Unknown")))

    print(f"❗ Found {len(superlow_nodes)} procurement winners with 'SuperLow' confidence.")
    if superlow_nodes:
        print("🔍 Sample:")
        for node, name in superlow_nodes[:10]:
            print(f"  - Node ID: {node}, Name: {name}")

    return superlow_nodes

