import networkx as nx
import pandas as pd
import os
import pickle
import hashlib

GRAPH_FILE = f"procurement_graph.graphml"  # Stored as GraphML for visualization & reusability

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

def load_or_initialize_graph(country_code):
    """
    Loads an existing graph from GraphML if available, otherwise initializes a new one.
    """
    if os.path.exists(f"procurement_graph_{country_code}.graphml"):
        print(f"✅ Loading existing graph from procurement_graph_{country_code}.graphml...")
        return nx.read_graphml(f"procurement_graph_{country_code}.graphml")
    
    print("🔍 Creating a new graph...")
    return nx.DiGraph()  # Initialize empty graph

def save_graph(G, country_code):
    """
    Saves the graph to GraphML format.
    """
    clean_node_attributes(G)  # Ensure consistent types before saving
    nx.write_graphml(G, f"procurement_graph_{country_code}.graphml")
    print(f"✅ Graph saved to procurement_graph_{country_code}.graphml")

def generate_procurement_id(row):
    """
    Generates a unique procurement ID using a hash of key fields.
    Ensures uniqueness across multiple runs.
    """
    unique_string = f"{row['bvdidnumber']}_{row.get('contract_id', row.name)}"  # Use contract_id if available
    return f"procurement_{hashlib.md5(unique_string.encode()).hexdigest()[:10]}"  # Short hash


def add_procurement_winners(G, country_code, procurement_df):
    """
    Adds procurement-winning companies to the graph as 'bid_winners' and stores extra node information.
    Ensures unique procurement IDs and prevents duplicate edges.
    """
    # print(f"🔍 Loading procurement data from {procurement_csv}...")
    # procurement_df = pd.read_csv(procurement_csv)

    added_procurements = 0
    added_edges = 0

    for _, row in procurement_df.iterrows():
        if str(row['WIN_COUNTRY_CODE'])[:2] == country_code or str(row['WIN_COUNTRY_CODE'])[:2] != country_code: #delete the other part if you want graphs for specific countries
            procurement_id = generate_procurement_id(row)  # Generate a unique procurement ID
            company_id = str(row['bvdidnumber'])  # Company ID

            # If procurement does not exist, add it
            if procurement_id not in G:
                procurement_attributes = {"type": "Procurement"}
                G.add_node(procurement_id, **procurement_attributes)
                added_procurements += 1

            # Ensure company node exists
            if company_id not in G:
                company_attributes = row.to_dict()
                company_attributes["type"] = "Company"
                company_attributes["bid_winner"] = True  # Mark as procurement winner
                G.add_node(company_id, **company_attributes)

            # Ensure edge exists before adding
            if not G.has_edge(company_id, procurement_id):
                G.add_edge(company_id, procurement_id, relationship='WON')
                added_edges += 1

    print(f"✅ Added {added_procurements} new procurements.")
    print(f"✅ Created {added_edges} new company-procurement edges.")

def normalize_id(value):
    """Ensures IDs are stored in a consistent format across runs."""
    return str(value).strip().lower() if pd.notna(value) else None

def add_matching_entities(G, df, source_column, target_column, relationship_type):
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
    # print(f"Sample nodes in graph: {list(existing_nodes)[:10]}")

    for _, row in df.iterrows():
        source_id = row[source_column]
        target_id = row[target_column]
        # Skip invalid target IDs
        if pd.isna(row[target_column]):
            # print(f"⚠️ Skipping row: target_id is NaN for source {source_id}")
            continue
        # Only proceed if the source node exists
        if source_id in existing_nodes and G.nodes[source_id].get("bid_winner") == True: #don't include and if we want to match subsidiaries of subsidiaries etc.
            # If the target node is not already in the graph, add it with metadata
            if target_id not in existing_nodes:
                node_attributes = row.to_dict()  # Store all row data
                node_attributes["type"] = "Company"
                G.add_node(target_id, **node_attributes)

                # Update hash set
                existing_nodes.add(target_id)

            # Check if an edge with the **same relationship type** already exists
            if (source_id, target_id) not in existing_edges:
                G.add_edge(source_id, target_id, relationship=relationship_type)

                # Update hash set
                existing_edges.add((source_id, target_id))

    print(f"✅ {relationship_type} relationships updated. Graph now has {len(G.nodes)} nodes and {len(G.edges)} edges.")

def add_flagged_entities(G, df):
    """
    Tags flagged entities that already exist in the graph and adds necessary edges.
    
    :param G: NetworkX graph
    :param flagged_csv: Path to CSV containing flagged individuals/entities
    """
    # print(f"🔍 Tagging flagged entities in the graph (no new nodes)...")
    # df = pd.read_csv(flagged_csv)

    for _, row in df.iterrows():
        flagged_id = str(row["id"])  # ID of flagged entity
        flagged_name = row["name"]  # Name of flagged entity
        entity_type = row["schema"]  # 'Person' or 'LegalEntity'
        matched_company = str(row["bvdidnumber"])  # Matched company ID

        if matched_company in G:
            if entity_type == "LegalEntity":
                # ✅ Only update the company if it exists in the graph
                G.nodes[matched_company]["flagged"] = True
                G.nodes[matched_company]["flagged_reason"] = f"Matched to flagged entity {flagged_id}"
                print(f"⚠️ Company {matched_company} is flagged.")

            elif entity_type == "Person":
                # ✅ Only add flagged person if the company is in the graph
                if flagged_id in G:
                    print(f"🔄 Flagged person {flagged_name} already exists, skipping node creation.")
                else:
                    G.add_node(flagged_id, type="Flagged", name=flagged_name)
                    print(f"✅ Added flagged person {flagged_name}.")

                # ✅ Add connection from flagged person to the company
                G.add_edge(flagged_id, matched_company, relationship="FLAGGED_LINK")
                print(f"✅ Linked flagged person {flagged_name} to {matched_company}.")
        # else:
            # 🚫 Ignore flagged entities if they are not linked to anything in the graph
            # print(f"🚫 Ignoring flagged entity {flagged_name} (not linked to any procurement).")

    print(f"✅ Finished tagging flagged entities.")

def expand_graph(G, shareholders_csv, subsidiaries_csv, controlling_shareholders_csv, depth=1):
    """
    Expands the graph with ownership structures, only adding entities related to procurement winners.
    Depth allows adding deeper connections.
    """
    print(f"🔄 Expanding graph with depth={depth}...")

    # Add direct ownership relationships
    add_matching_entities(G, shareholders_csv, "bvdidnumber", "shareholderbvdidnumber", "OWNS")
    add_matching_entities(G, subsidiaries_csv, "subsidiarybvdidnumber", "bvdidnumber", "SUBSIDIARY_OF")
    add_matching_entities(G, controlling_shareholders_csv, "bvdidnumber", "guobvdidnumber", "CONTROLS")

    if depth > 1:
        print("🔄 Expanding to deeper levels (e.g., shareholders of shareholders)...")

        # Expand to shareholders of shareholders
        add_matching_entities(G, shareholders_csv, "bvdidnumber", "shareholderbvdidnumber", "OWNS")

        # Expand to subsidiaries of subsidiaries
        add_matching_entities(G, subsidiaries_csv, "subsidiarybvdidnumber", "bvdidnumber", "SUBSIDIARY_OF")

    print(f"✅ Graph expansion complete (depth={depth}).")

def expand_graph_by_type(G, data_csv, entity_type):
    """
    Expands the graph using a specific dataset type: 'shareholder', 'subsidiary', or 'controlling'.
    Only adds entities related to procurement winners.

    Parameters:
        G (networkx.Graph): The graph to expand.
        data_csv (str): Path to the CSV file to process.
        entity_type (str): Type of expansion ('shareholder', 'subsidiary', 'controlling').

    """
    print(f"🔄 Expanding graph with {entity_type.upper()}") #data from {data_csv}...

    if entity_type == "shareholder":
        add_matching_entities(G, data_csv, "bvdidnumber", "shareholderbvdidnumber", "OWNS")

    elif entity_type == "subsidiary":
        add_matching_entities(G, data_csv, "subsidiarybvdidnumber", "bvdidnumber", "SUBSIDIARY_OF")

    elif entity_type == "controlling":
        add_matching_entities(G, data_csv, "bvdidnumber", "guobvdidnumber", "CONTROLS")

    else:
        raise ValueError(f"❌ Invalid entity type: {entity_type}. Choose 'shareholder', 'subsidiary', or 'controlling'.")

    print(f"✅ Graph expansion complete for {entity_type.upper()}.")