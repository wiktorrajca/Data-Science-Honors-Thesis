import networkx as nx
import pandas as pd
import os
import pickle

GRAPH_FILE = "procurement_graph.pkl"  # File where the graph is stored

def initialize_graph_from_csvs(procurement_csv, shareholders_csv, subsidiaries_csv, first_level_shareholders_csv, controlling_shareholders_csv):
    """
    Builds and saves a directed graph from procurement, shareholder, and subsidiary data.
    If the graph already exists, it loads it instead of rebuilding.
    """

    # Check if the graph already exists
    if os.path.exists(GRAPH_FILE):
        print(f"✅ Loading existing graph from {GRAPH_FILE}...")
        with open(GRAPH_FILE, "rb") as f:
            return pickle.load(f)

    print("🔍 Creating a new graph...")
    G = nx.DiGraph()

    # Step 1: Load procurement matches
    print(f"🔍 Loading procurement data from {procurement_csv}...")
    procurement_df = pd.read_csv(procurement_csv)

    for index, row in procurement_df.iterrows():
        procurement_id = index  # Use the row index as the ID
        company_id = str(row['bvdidnumber'])  # Company ID

        # Add nodes
        G.add_node(procurement_id, type='Procurement')
        G.add_node(company_id, type='Company')

        # Create edge: Company won Procurement
        G.add_edge(company_id, procurement_id, relationship='WON')

    # Step 2: Load shareholder matches
    print(f"🔍 Loading shareholder data from {shareholders_csv}...")
    shareholders_df = pd.read_csv(shareholders_csv)

    for _, row in shareholders_df.iterrows():
        shareholder_id = str(row['guobvdidnumber'])
        company_id = str(row['bvdidnumber'])

        if shareholder_id != company_id:
            G.add_node(shareholder_id, type='Shareholder')
            G.add_edge(shareholder_id, company_id, relationship='OWNS')

    # Step 3: Load subsidiaries data
    print(f"🔍 Loading subsidiaries data from {subsidiaries_csv}...")
    subsidiaries_df = pd.read_csv(subsidiaries_csv)

    for _, row in subsidiaries_df.iterrows():
        parent_id = str(row['bvdidnumber'])
        subsidiary_id = str(row['subsidiarybvdidnumber'])

        if parent_id != subsidiary_id:
            G.add_node(parent_id, type='Company')
            G.add_node(subsidiary_id, type='Company')
            G.add_edge(parent_id, subsidiary_id, relationship='SUBSIDIARY_OF')

    # Step 4: Load first-level shareholders data
    print(f"🔍 Loading first-level shareholders data from {first_level_shareholders_csv}...")
    first_level_df = pd.read_csv(first_level_shareholders_csv)

    for _, row in first_level_df.iterrows():
        shareholder_id = str(row['shareholderbvdidnumber'])
        company_id = str(row['bvdidnumber'])

        if shareholder_id != company_id:
            G.add_node(shareholder_id, type='Shareholder')
            G.add_edge(shareholder_id, company_id, relationship='FIRST_LEVEL_SHAREHOLDER_OF')

    # Step 5: Load controlling shareholders data
    print(f"🔍 Loading controlling shareholders data from {controlling_shareholders_csv}...")
    controlling_df = pd.read_csv(controlling_shareholders_csv)

    for _, row in controlling_df.iterrows():
        shareholder_id = str(row['cshbvdidnumber'])
        company_id = str(row['bvdidnumber'])

        if shareholder_id != company_id:
            G.add_node(shareholder_id, type='Shareholder')
            G.add_edge(shareholder_id, company_id, relationship='CONTROLLING_SHAREHOLDER_OF')

    print("✅ Graph initialization complete. Saving to disk...")

    # Save the graph to disk
    with open(GRAPH_FILE, "wb") as f:
        pickle.dump(G, f)

    print(f"✅ Graph saved as {GRAPH_FILE}")
    return G

def analyze_flagged_procurements(G, flagged_shareholders):
    """
    Identifies procurements won by companies that have at least one flagged shareholder.
    
    flagged_shareholders: List of flagged shareholder IDs (bvdidnumbers).
    """
    flagged_procurements = set()

    for shareholder in flagged_shareholders:
        if G.has_node(shareholder):
            for company in G.successors(shareholder):  # Shareholder → Company
                for procurement in G.successors(company):  # Company → Procurement
                    flagged_procurements.add(procurement)

    return list(flagged_procurements)



import networkx as nx
import pandas as pd
import os

GRAPH_FILE = "procurement_graph.graphml"  # Stored as GraphML for visualization & reusability

def load_or_initialize_graph():
    """
    Loads an existing graph from GraphML if available, otherwise initializes a new one.
    """
    if os.path.exists(GRAPH_FILE):
        print(f"✅ Loading existing graph from {GRAPH_FILE}...")
        return nx.read_graphml(GRAPH_FILE)
    
    print("🔍 Creating a new graph...")
    return nx.DiGraph()  # Initialize empty graph

def save_graph(G):
    """
    Saves the graph to GraphML format.
    """
    nx.write_graphml(G, GRAPH_FILE)
    print(f"✅ Graph saved to {GRAPH_FILE}")

def add_procurement_winners(G, procurement_csv):
    """
    Adds procurement-winning companies to the graph as 'bid_winners' and stores extra node information.
    """
    print(f"🔍 Loading procurement data from {procurement_csv}...")
    procurement_df = pd.read_csv(procurement_csv)

    for index, row in procurement_df.iterrows():
        procurement_id = f"procurement_{index}"  # Use index as unique ID
        company_id = str(row['bvdidnumber'])  # Company ID

        # Store full row data as node attributes
        company_attributes = row.to_dict()
        company_attributes["type"] = "Company"
        company_attributes["bid_winner"] = True  # Mark as procurement winner

        procurement_attributes = {"type": "Procurement"}

        # Add procurement node with full details
        G.add_node(procurement_id, **procurement_attributes)

        # Add company node with full row data
        G.add_node(company_id, **company_attributes)

        # Create edge: Company won Procurement
        G.add_edge(company_id, procurement_id, relationship='WON')

    print("✅ Added procurement winners to the graph with full metadata.")

    print("✅ Added procurement winners to the graph.")

def add_matching_entities(G, data_csv, source_column, target_column, relationship_type):
    """
    Adds nodes and edges to the graph only if the `source_column` matches an existing node.
    Also stores full row data for each new node.
    """
    print(f"🔍 Loading {relationship_type} data from {data_csv}...")
    df = pd.read_csv(data_csv)

    for _, row in df.iterrows():
        source_id = str(row[source_column])
        target_id = str(row[target_column])

        # Only add if the source is already in the graph (ensuring relevant connections)
        if source_id in G:
            node_attributes = row.to_dict()  # Store all row data
            node_attributes["type"] = "Company"

            # Add target node with all available metadata
            G.add_node(target_id, **node_attributes)

            # Create relationship edge
            G.add_edge(source_id, target_id, relationship=relationship_type)

    print(f"✅ Added {relationship_type} relationships with metadata.")

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
        add_matching_entities(G, shareholders_csv, "shareholder_bvdid", "ultimate_shareholder_bvdid", "OWNS")

        # Expand to subsidiaries of subsidiaries
        add_matching_entities(G, subsidiaries_csv, "parent_bvdid", "ultimate_parent_bvdid", "SUBSIDIARY_OF")

    print(f"✅ Graph expansion complete (depth={depth}).")

def analyze_flagged_procurements(G, flagged_shareholders):
    """
    Identifies procurements won by companies that have at least one flagged shareholder.
    """
    flagged_procurements = set()

    for shareholder in flagged_shareholders:
        if G.has_node(shareholder):
            for company in G.successors(shareholder):  # Shareholder → Company
                for procurement in G.successors(company):  # Company → Procurement
                    flagged_procurements.add(procurement)

    return list(flagged_procurements)