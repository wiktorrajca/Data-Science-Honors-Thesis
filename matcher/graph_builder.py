import networkx as nx
import pandas as pd
import os
import pickle

# GRAPH_FILE = "procurement_graph.pkl"  # File where the graph is stored

# def initialize_graph_from_csvs(procurement_csv, shareholders_csv, subsidiaries_csv, first_level_shareholders_csv, controlling_shareholders_csv):
#     """
#     Builds and saves a directed graph from procurement, shareholder, and subsidiary data.
#     If the graph already exists, it loads it instead of rebuilding.
#     """

#     # Check if the graph already exists
#     if os.path.exists(GRAPH_FILE):
#         print(f"✅ Loading existing graph from {GRAPH_FILE}...")
#         with open(GRAPH_FILE, "rb") as f:
#             return pickle.load(f)

#     print("🔍 Creating a new graph...")
#     G = nx.DiGraph()

#     # Step 1: Load procurement matches
#     print(f"🔍 Loading procurement data from {procurement_csv}...")
#     procurement_df = pd.read_csv(procurement_csv)

#     for index, row in procurement_df.iterrows():
#         procurement_id = index  # Use the row index as the ID
#         company_id = str(row['bvdidnumber'])  # Company ID

#         # Add nodes
#         G.add_node(procurement_id, type='Procurement')
#         G.add_node(company_id, type='Company')

#         # Create edge: Company won Procurement
#         G.add_edge(company_id, procurement_id, relationship='WON')

#     # Step 2: Load shareholder matches
#     print(f"🔍 Loading shareholder data from {shareholders_csv}...")
#     shareholders_df = pd.read_csv(shareholders_csv)

#     for _, row in shareholders_df.iterrows():
#         shareholder_id = str(row['guobvdidnumber'])
#         company_id = str(row['bvdidnumber'])

#         if shareholder_id != company_id:
#             G.add_node(shareholder_id, type='Shareholder')
#             G.add_edge(shareholder_id, company_id, relationship='OWNS')

#     # Step 3: Load subsidiaries data
#     print(f"🔍 Loading subsidiaries data from {subsidiaries_csv}...")
#     subsidiaries_df = pd.read_csv(subsidiaries_csv)

#     for _, row in subsidiaries_df.iterrows():
#         parent_id = str(row['bvdidnumber'])
#         subsidiary_id = str(row['subsidiarybvdidnumber'])

#         if parent_id != subsidiary_id:
#             G.add_node(parent_id, type='Company')
#             G.add_node(subsidiary_id, type='Company')
#             G.add_edge(parent_id, subsidiary_id, relationship='SUBSIDIARY_OF')

#     # Step 4: Load first-level shareholders data
#     print(f"🔍 Loading first-level shareholders data from {first_level_shareholders_csv}...")
#     first_level_df = pd.read_csv(first_level_shareholders_csv)

#     for _, row in first_level_df.iterrows():
#         shareholder_id = str(row['shareholderbvdidnumber'])
#         company_id = str(row['bvdidnumber'])

#         if shareholder_id != company_id:
#             G.add_node(shareholder_id, type='Shareholder')
#             G.add_edge(shareholder_id, company_id, relationship='FIRST_LEVEL_SHAREHOLDER_OF')

#     # Step 5: Load controlling shareholders data
#     print(f"🔍 Loading controlling shareholders data from {controlling_shareholders_csv}...")
#     controlling_df = pd.read_csv(controlling_shareholders_csv)

#     for _, row in controlling_df.iterrows():
#         shareholder_id = str(row['cshbvdidnumber'])
#         company_id = str(row['bvdidnumber'])

#         if shareholder_id != company_id:
#             G.add_node(shareholder_id, type='Shareholder')
#             G.add_edge(shareholder_id, company_id, relationship='CONTROLLING_SHAREHOLDER_OF')

#     print("✅ Graph initialization complete. Saving to disk...")

#     # Save the graph to disk
#     with open(GRAPH_FILE, "wb") as f:
#         pickle.dump(G, f)

#     print(f"✅ Graph saved as {GRAPH_FILE}")
#     return G

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
    if os.path.exists(f"procurement_graph"):
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

def add_procurement_winners(G, country_code, procurement_csv):
    """
    Adds procurement-winning companies to the graph as 'bid_winners' and stores extra node information.
    """
    print(f"🔍 Loading procurement data from {procurement_csv}...")
    procurement_df = pd.read_csv(procurement_csv)

    for index, row in procurement_df.iterrows():
        if str(row['WIN_COUNTRY_CODE'])[:2] == country_code:
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

def add_flagged_entities(G, flagged_csv):
    """
    Tags flagged entities that already exist in the graph and adds necessary edges.
    
    :param G: NetworkX graph
    :param flagged_csv: Path to CSV containing flagged individuals/entities
    """
    print(f"🔍 Tagging flagged entities in the graph (no new nodes)...")
    df = pd.read_csv(flagged_csv)

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
        else:
            # 🚫 Ignore flagged entities if they are not linked to anything in the graph
            print(f"🚫 Ignoring flagged entity {flagged_name} (not linked to any procurement).")

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