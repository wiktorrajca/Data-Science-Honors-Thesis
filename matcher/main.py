from graph_builder import (
    load_or_initialize_graph, 
    save_graph, 
    add_procurement_winners, 
    expand_graph, 
    analyze_flagged_procurements
)

def main():
    # Define paths to CSV files
    procurement_csv = "/Users/wiktorrajca/Documents/GitHub/Data-Science-Honors-Thesis/matcher/output/merged_result_3.csv"
    shareholders_csv = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/basic_shareholder_info1 (1).csv"
    subsidiaries_csv = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/subsidiaries_first_level1.csv"
    first_level_shareholders_csv = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/shareholders_first_level1.csv"
    controlling_shareholders_csv = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/controlling_shareholders1.csv"

    # Step 1: Load existing graph or create a new one
    G = load_or_initialize_graph()

    # Step 2: Add procurement winners (only if they are not already present)
    if not any(G.nodes[n].get("bid_winner") for n in G.nodes):
        add_procurement_winners(G, procurement_csv)

    # Step 3: Expand the graph with additional ownership layers
    expand_graph(G, first_level_shareholders_csv, subsidiaries_csv, shareholders_csv, depth=1)

    # Step 4: Save the updated graph
    save_graph(G)

    # Step 5: Analyze flagged shareholders
    flagged_shareholders = {"JP3430003015395", "SE*110357629864"}
    flagged_procurements = analyze_flagged_procurements(G, flagged_shareholders)

    print("\n⚠️ Procurements linked to flagged shareholders:", flagged_procurements)

if __name__ == "__main__":
    main()