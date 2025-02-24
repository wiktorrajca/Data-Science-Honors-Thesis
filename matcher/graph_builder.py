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