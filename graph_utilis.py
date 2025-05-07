import pandas as pd
import networkx as nx
from tqdm import tqdm
import os

def load_or_initialize_graph(country_code):
    """
    Loads an existing graph from GraphML if available, otherwise initializes a new one.
    """
    if os.path.exists(f"procurement_graph_{country_code}.graphml"):
        print(f" Loading existing graph from procurement_graph_{country_code}.graphml...")
        return nx.read_graphml(f"procurement_graph_{country_code}.graphml")
    
    print("🔍 Creating a new graph...")
    return nx.DiGraph()  # Initialize empty graph

def add_procurement_winners(G, country_code, procurement_df):
    """
    Processes the procurement DataFrame grouped by 'Unnamed: 0'. In this DiGraph:
      - Each procurement group produces one procurement node.
      - For each procurement group, company nodes are added (based on 'bvdidnumber')
        and an edge is added from the company node to the procurement node.
    
    Debug output (limited summary):
      - Prints a sample (first 5 rows) of the procurement group’s data.
      - Prints the unique, cleaned company IDs and their count.
      - Prints each edge addition attempt.
      - For each procurement, prints the full list of incoming neighbors using G.predecessors(procurement_key)
        and then filters these to list only those with "type" == "Company".
      - For procurement "24", prints detailed neighbor info.
    
    For each procurement group:
      - If any row has both WIN_COUNTRY_CODE and the company’s bvdidnumber prefix matching the target country_code,
        only those rows are used (with "country_matched" = True);
      - Otherwise, all rows in the group are used (with "country_matched" = False).
    
    Likelihood is computed as 1 divided by the number of unique companies in the selected group.
    Both procurement and company IDs are converted to strings (and stripped) for consistency.
    """
    added_procurements = 0
    added_edges = 0

    grouped = procurement_df.groupby('Unnamed: 0')

    for procurement_id, group in grouped:
        procurement_key = str(procurement_id).strip()
        
        # Copy and ensure key columns are strings without extraneous whitespace.
        group = group.copy()
        group['WIN_COUNTRY_CODE'] = group['WIN_COUNTRY_CODE'].astype(str).str.strip()
        group['bvdidnumber'] = group['bvdidnumber'].astype(str).str.strip()
        
        # print(f"\n--- Processing Procurement {procurement_key} ---")
        # print("Sample group data (first 5 rows):")
        # print(group[['WIN_COUNTRY_CODE', 'bvdidnumber']].head())
        
        # Use strict matching if possible.
        strict_mask = (group['WIN_COUNTRY_CODE'].str[:2] == country_code) & (group['bvdidnumber'].str[:2] == country_code)
        if strict_mask.any():
            selected = group[strict_mask]
            country_matched_val = True
            # print("Using strict matching rows (country_matched: True)")
        else:
            selected = group
            country_matched_val = False
            # print("No strict match found; using all rows (country_matched: False)")
        
        # Determine unique company IDs.
        unique_company_ids = selected['bvdidnumber'].unique().tolist()
        unique_company_ids = [cid.strip() for cid in unique_company_ids]
        unique_companies = len(unique_company_ids)
        likelihood = 1 / unique_companies
        
        # print(f"Unique company IDs for procurement {procurement_key}: {unique_company_ids}")
        # print(f"Computed unique companies = {unique_companies} and likelihood = {likelihood:.4f}")
        
        # Add or update the procurement node.
        if procurement_key not in G:
            G.add_node(procurement_key, type="Procurement", unique_companies_count=unique_companies)
            added_procurements += 1
            # print(f"Procurement node {procurement_key} added.")
        else:
            G.nodes[procurement_key]['unique_companies_count'] = unique_companies
            # print(f"Procurement node {procurement_key} updated with unique_companies_count.")
        
        # Process each selected row.
        for _, row in selected.iterrows():
            company_id = row['bvdidnumber'].strip()
            company_attributes = row.to_dict()
            company_attributes.update({
                "type": "Company",
                "bid_winner": True,
                "likelyhood": likelihood,
                "country_matched": country_matched_val,
                "the_name": row['name'],
                "the_country": row['WIN_COUNTRY_CODE'],
                "the_town": row['WIN_TOWN'],
                "the_postal_code": row['WIN_POSTAL_CODE'],
                "the_identifier": row['WIN_NATIONALID']
            })
            
            if company_id not in G:
                G.add_node(company_id, **company_attributes)
                # print(f"Company node {company_id} added.")
            else:
                G.nodes[company_id].setdefault("likelyhood", likelihood)
                G.nodes[company_id].setdefault("country_matched", country_matched_val)
                # print(f"Company node {company_id} exists; attributes ensured.")
            
            # print(f"Attempting to add edge from company {company_id} to procurement {procurement_key}")
            if not G.has_edge(company_id, procurement_key):
                G.add_edge(company_id, procurement_key, relationship='WON')
                added_edges += 1
                # print(f"Edge added from {company_id} to {procurement_key}")
            else:
                print(f"Edge from {company_id} to {procurement_key} already exists.")
        
        # # Debug: Print full list of incoming neighbors (predecessors) for this procurement.
        # all_predecessors = list(G.predecessors(procurement_key))
        # print(f"All incoming neighbors for procurement {procurement_key}: {all_predecessors}")
        
        # # Now filter to those with type 'Company'.
        # company_predecessors = [nbr for nbr in all_predecessors if G.nodes[nbr].get("type") == "Company"]
        # print(f"Filtered company neighbors for procurement {procurement_key}: {company_predecessors}")
        
        # if procurement_key == '24':
        #     print(f"Detailed neighbor info for procurement {procurement_key}:")
        #     for nbr in all_predecessors:
        #         print(f"Neighbor {nbr} attributes: {G.nodes[nbr]}")
    
    print(f"\n✅ Added {added_procurements} procurement nodes, {added_edges} company-procurement edges.")
    return G

def lowest_likelihood_group(G):
    """
    Finds the procurement group (a procurement node along with its connected companies)
    from a directed graph G (DiGraph) that has more than one company (i.e., multi-winner)
    and returns the group that has the lowest likelihood value.
    
    The likelihood for a procurement is computed as 1 / (number of companies in the group),
    so a lower likelihood indicates that more companies are connected.
    
    Returns:
        dict: A dictionary containing the procurement key, the list of company nodes,
              and the common likelihood value for that group. For example:
                {
                  "procurement": <procurement_node_key>,
                  "companies": [<company_node_id>, ...],
                  "likelihood": <lowest_likelihood_value>
                }
        If no multi-winner procurement is found, returns an empty dict.
    """
    # First, create a dictionary of all procurement groups with more than one company.
    multi_winner = {}
    # Iterate over all procurement nodes (we assume they have "type" == "Procurement")
    for node, data in G.nodes(data=True):
        if data.get("type") == "Procurement":
            # In a DiGraph, companies are incoming neighbors (predecessors)
            companies = [nbr for nbr in G.predecessors(node) 
                         if G.nodes[nbr].get("type") == "Company"]
            if len(companies) > 1:
                # We assume all companies share the same likelihood (as computed during node creation)
                likelihood = G.nodes[companies[0]].get("likelyhood")
                multi_winner[node] = {
                    "companies": companies,
                    "likelihood": likelihood
                }
    
    if not multi_winner:
        return {}
    
    # Find the procurement (key) with the lowest likelihood value.
    lowest_procurement = min(multi_winner.keys(), key=lambda k: multi_winner[k]["likelihood"])
    
    result = {
        "procurement": lowest_procurement,
        "number of members": 1/multi_winner[lowest_procurement]["likelihood"],
        "likelihood": multi_winner[lowest_procurement]["likelihood"],
        "companies": multi_winner[lowest_procurement]["companies"]
    }
    
    return result

def check_procurements_with_multiple_companies(G):
    """
    Checks which procurement nodes are connected to more than one company.
    
    Returns:
        list: A list of procurement node keys with more than one company neighbor.
    """
    multi_company_procurements = []
    for node, attr in G.nodes(data=True):
        if attr.get("type") == "Procurement":
            # For DiGraph, the associated companies are stored as predecessors.
            company_predecessors = [nbr for nbr in G.predecessors(node) if G.nodes[nbr].get("type") == "Company"]
            if len(company_predecessors) > 1:
                multi_company_procurements.append(node)
                print(f"Procurement node {node} is connected to {len(company_predecessors)} companies.")
    if not multi_company_procurements:
        print("No procurement nodes are connected to more than one company.")
    return multi_company_procurements

def check_graph_integrity(G):
    """
    Checks if the graph meets integrity expectations:
    - No orphan nodes (nodes with no edges)
    - Only isolated pairs are connected by 'WON' edges
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

    print("🧪 Integrity check complete.\n")

def add_matching_entities(G, df, source_column, target_column, the_name, first_name, country_code, town, relationship_type):
    """
    Adds nodes and edges to the graph only if the `source_column` matches an existing node.
    If `target_column` already exists in the graph, only adds the edge if it doesn’t exist.
    Also stores full row data for each new node.
    """
    if len(G) == 0:
        print("⚠️ Graph is empty. Skipping entity matching.")
        return
    existing_nodes = set(G.nodes)
    existing_edges = set(G.edges)

    for _, row in tqdm(df.iterrows(), total=len(df), desc=f"→ {relationship_type} rows processed"):
        source_id = row[source_column]
        target_id = row[target_column]
        if pd.isna(row[target_column]):
            continue
        if source_id in existing_nodes and G.nodes[source_id].get("bid_winner") == True:
            if target_id not in existing_nodes:
                node_attributes = row.to_dict()
                node_attributes["type"] = "Person" if first_name in row and pd.notna(row[first_name]) else "Company"
                node_attributes['the_name'] = row[the_name]
                node_attributes['the_country'] = row[country_code]
                node_attributes['the_town'] = row[town]
                G.add_node(target_id, **node_attributes)
                existing_nodes.add(target_id)

            if (target_id, source_id) not in existing_edges:
                G.add_edge(target_id, source_id, relationship=relationship_type)
                existing_edges.add((target_id, source_id))

    print(f"✅ {relationship_type} relationships updated. Graph now has {len(G.nodes)} nodes and {len(G.edges)} edges.")


def load_and_match_all(graph, shareholders_path, subsidiaries_path, basic_shareholders_path):
    folder_config = {
        shareholders_path: {
            "source_column": "bvdidnumber",
            "target_column": "shareholderbvdidnumber",
            "the_name": "shareholdername",
            "first_name": "shareholderfirstname",
            "country_code": "shareholdercountryisocode",
            "town": "shareholdercity",
            "relationship_type": "SHAREHOLDER_OF"
        },
        subsidiaries_path: {
            "source_column": "bvdidnumber",
            "target_column": "subsidiarybvdidnumber",
            "the_name": "subsidiaryname",
            "first_name": "subsidiaryfirstname",
            "country_code": "subsidiarycountryisocode",
            "town": "subsidiarycity",
            "relationship_type": "SUBSIDIARY_OF"
        },
        basic_shareholders_path: [
            {
                "source_column": "bvdidnumber",
                "target_column": "duobvdidnumber",
                "the_name": "duoname",
                "first_name": "duofirstname",
                "country_code": "duocountryisocode",
                "town": "duocity",
                "relationship_type": "DOMESTIC_ULTIMATE_OWNER_OF"
            },
            {
                "source_column": "bvdidnumber",
                "target_column": "guobvdidnumber",
                "the_name": "guoname",
                "first_name": "guofirstname",
                "country_code": "guocountryisocode",
                "town": "guocity",
                "relationship_type": "GLOBAL_ULTIMATE_OWNER_OF"
            }
        ]
    }

    for folder, configs in folder_config.items():
        if isinstance(configs, dict):
            configs = [configs]

        file_list = [f for f in os.listdir(folder) if f.endswith(".csv")]
        for file_name in tqdm(file_list, desc=f"📂 Processing files in {folder}"):
            file_path = os.path.join(folder, file_name)
            df = pd.read_csv(file_path, dtype=str)
            for config in configs:
                add_matching_entities(
                    graph,
                    df,
                    source_column=config["source_column"],
                    target_column=config["target_column"],
                    the_name=config["the_name"],
                    first_name=config["first_name"],
                    country_code=config["country_code"],
                    town=config["town"],
                    relationship_type=config["relationship_type"]
                )

###---------------SAVING-THE-GRAPH-------------------###

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

###-------------MERGING-THE-GRAPH-WITH-SCREENING---------------###
def attach_screening_results(G: nx.Graph, df: pd.DataFrame, node_id_col: str = "node_id") -> nx.Graph:
    """
    Attach screening results from a DataFrame to each node in a NetworkX graph.

    Parameters
    ----------
    G : nx.Graph
        Your pre-existing graph (DiGraph or Graph) whose nodes have IDs matching df[node_id_col].
    df : pd.DataFrame
        A DataFrame containing at least the columns:
         - node_id_col  : identifier for each node
         - 'match_found': boolean-like
         - 'dataset'    : dataset name (when match_found is True)
         - 'score'      : numeric score (optional / may be NaN)
    node_id_col : str, default "node_id"
        Name of the column in `df` that holds the node identifiers.

    Returns
    -------
    G : nx.Graph
        The same graph instance, with each node’s `G.nodes[node]["screening"]`
        set to a dict of the form:
          { "match_found": bool,
            "datasets"   : "comma, separated, list" (if any),
            "scores"     : { dataset_name: score, … } (if any)
          }
    """
    # ensure node_id is string for matching
    df = df.copy()
    df[node_id_col] = df[node_id_col].astype(str)
    
    # aggregate
    screening_results = {}
    for node_id, group in df.groupby(node_id_col):
        any_found = group['match_found'].fillna(False).astype(bool).any()
        if not any_found:
            screening_results[node_id] = {"match_found": False}
        else:
            datasets = []
            scores = {}
            for _, row in group.iterrows():
                if bool(row['match_found']):
                    ds = row['dataset']
                    datasets.append(ds)
                    scores[ds] = row['score'] if pd.notnull(row['score']) else None
            screening_results[node_id] = {
                "match_found": True,
                "datasets": ", ".join(datasets),
                "scores": scores
            }
    
    # attach to graph
    for n in G.nodes:
        nid = str(n)
        info = screening_results.get(nid, {"match_found": False})
        G.nodes[n]["screening"] = info
    
    return G

def check_all_nodes_have_screening(G):
    """
    Check if every node in the graph has a 'screening' attribute.
    Returns True if all nodes have it, False otherwise.
    """
    for node, attrs in G.nodes(data=True):
        if 'screening' not in attrs:
            return False
    return True