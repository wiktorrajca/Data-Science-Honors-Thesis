from graph_builder import initialize_graph_from_csvs, analyze_flagged_procurements

def main():
    # Define paths to CSV files
    procurement_csv = "/Users/wiktorrajca/Documents/GitHub/Data-Science-Honors-Thesis/matcher/output/merged_result_3.csv"
    shareholders_csv = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/basic_shareholder_info1 (1).csv"
    subsidiaries_csv = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/subsidiaries_first_level1.csv"
    first_level_shareholders_csv = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/shareholders_first_level1.csv"
    controlling_shareholders_csv = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/controlling_shareholders1.csv"

    # Step 1: Load the graph (either from disk or create a new one)
    G = initialize_graph_from_csvs(procurement_csv, shareholders_csv, subsidiaries_csv, first_level_shareholders_csv, controlling_shareholders_csv)

    # Step 2: Define flagged shareholders (from a sanctions list, database, etc.)
    flagged_shareholders = {"JP3430003015395", "SE*110357629864"}

    # Step 3: Find procurements linked to flagged shareholders
    flagged_procurements = analyze_flagged_procurements(G, flagged_shareholders)

    print("\n⚠️ Procurements linked to flagged shareholders:", flagged_procurements)

if __name__ == "__main__":
    main()