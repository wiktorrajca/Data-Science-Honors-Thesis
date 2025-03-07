import argparse
import os
import glob
import pandas as pd
from graph_builder import (
    load_or_initialize_graph, 
    save_graph, 
    add_procurement_winners, 
    expand_graph, 
    expand_graph_by_type,
    add_flagged_entities
)

def add_procurements(G, country, procurement_csv_list):
    """
    Adds procurement winners from multiple procurement datasets before expanding the graph.
    """
    for procurement_csv in procurement_csv_list:
        procurement_df = pd.read_csv(procurement_csv)
        add_procurement_winners(G, country, procurement_df)

def expand_ownership(G, first_level_shareholders_csv_list, subsidiaries_csv_list, shareholders_csv_list, depth=1):
    """
    Expands the graph multiple times using multiple shareholder & subsidiary datasets.
    """
    for first_level_shareholders_csv in first_level_shareholders_csv_list:
        first_level_shareholders_df = pd.read_csv(first_level_shareholders_csv)
        expand_graph_by_type(G, first_level_shareholders_df, "shareholder")
    for subsidiaries_csv in subsidiaries_csv_list:
        subsidiaries_df = pd.read_csv(subsidiaries_csv)
        expand_graph_by_type(G, subsidiaries_df, "subsidiary")
    for shareholders_csv in shareholders_csv_list:
        shareholders_df = pd.read_csv(shareholders_csv)
        expand_graph_by_type(G, shareholders_df, "controlling")

def process_country(country, procurement_csv_list, first_level_shareholders_csv_list, subsidiaries_csv_list, shareholders_csv_list, flagged_csv_list, depth=1):
    """
    Loads or initializes a graph for a given country, adds procurements first, then expands the ownership structure.
    """
    # Step 1: Load existing graph or create a new one
    G = load_or_initialize_graph(country)

    # Step 2: Add procurement winners from multiple datasets
    add_procurements(G, country, procurement_csv_list)

    # Step 3: Expand the graph with multiple ownership datasets
    expand_ownership(G, first_level_shareholders_csv_list, subsidiaries_csv_list, shareholders_csv_list, depth)

    # Step 4: Add flagged entities (ONLY IF THEY EXIST IN THE GRAPH)
    for flagged_csv in flagged_csv_list:
        flagged_df = pd.read_csv(flagged_csv)
        add_flagged_entities(G, flagged_df)

    # Step 5: Save the updated graph
    save_graph(G, country)

def get_files_from_folder(folder_path, extension="csv"):
    """
    Retrieves all files with a given extension from a folder.

    Parameters:
        folder_path (str): Path to the folder containing datasets.
        extension (str): File extension to look for (default: "csv").

    Returns:
        list: List of file paths found in the folder.
    """
    if not folder_path or not os.path.isdir(folder_path):
        print(f"⚠️ Warning: Folder '{folder_path}' not found. Using default paths.")
        return []
    
    return glob.glob(os.path.join(folder_path, f"*.{extension}"))

def main():
    parser = argparse.ArgumentParser(description="Build procurement fraud detection graphs.")
    parser.add_argument("--country", nargs="+", help="Specify one or more country codes to generate graphs for specific countries. If omitted, generates graphs for all countries.")
    
    parser.add_argument("--procurement_folder", type=str, help="Folder containing procurement datasets.")
    parser.add_argument("--first_level_shareholders_folder", type=str, help="Folder containing first-level shareholder datasets.")
    parser.add_argument("--subsidiary_folder", type=str, help="Folder containing subsidiaries datasets.")
    parser.add_argument("--shareholder_folder", type=str, help="Folder containing controlling shareholder datasets.")
    parser.add_argument("--flagged_folder", type=str, help="Folder containing flagged entity datasets.")

    args = parser.parse_args()

    # Default list of countries if not provided via CLI
    countries = args.country or [
        'IT', 'FR', 'CZ', 'DK', 'DE', 'EE', 'NL', 'PT', 'PL', 'BG', 'HU', 'MT', 'LU', 'ES', 'GR', 'SE', 'LT', 'CY', 'BE', 'LV', 
        'SK', 'UK', 'RO', 'SI', 'IE', 'AT', 'SA', 'US', 'MK', 'NO', 'CH', 'HR', 'FI', 'AU', 'TR', 'IS', 'SR', 'AD', 'BT', 'JP', 
        'HK', 'AX', 'VU', 'ZA', 'MA', 'BY', 'RU', 'CA', 'KE', 'GA', 'VN', 'LI', 'TN', 'OM', 'AF', 'RS', 'AE', 'MU', 'VA', 'SG', 
        'AR', 'GE', 'IL', 'BI', 'CN', 'NE', 'PE', 'BF', 'VG', 'EG', 'UA', 'AM', 'AI', 'PG', 'SZ', 'BA', 'IN', 'FK', 'BR', 'JO', 
        'KR', 'SV', 'NP', 'SM', 'VI', 'IO', 'VE', 'AL', '1A', 'PH', 'TG', 'ET', 'CC', 'IQ', 'MC', 'LB', 'DZ', 'NZ', 'MY', 'IR', 
        'FO', 'WF', 'BB', 'PA', 'AZ', 'BZ', 'MG', 'TW', 'TZ', 'SD', 'MZ', 'PR', 'CO', 'ME', 'PN', 'GM', 'GH', 'TH', 'UG', 'MD', 
        'MS', 'UM', 'KN', 'MR', 'PK', 'EC', 'BJ', 'KP', 'UY', 'BO', 'HN', 'BM', 'GG', 'CR', 'CL', 'GD', 'GN', 'NI'
    ]

    # Retrieve file lists from folders or use defaults
    procurement_csv_list = get_files_from_folder(args.procurement_folder) or [
        "/Users/wiktorrajca/Documents/GitHub/Data-Science-Honors-Thesis/matcher/output/merged_result_3.csv",
        "/Users/wiktorrajca/Documents/GitHub/Data-Science-Honors-Thesis/matcher/output/merged_result_1.csv"
    ]
    
    subsidiaries_csv_list = get_files_from_folder(args.subsidiary_folder) or [
        "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/subsidiaries_first_level1.csv"
    ]

    first_level_shareholders_csv_list = get_files_from_folder(args.first_level_shareholders_folder) or [
        "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/shareholders_first_level1.csv"
    ]

    shareholders_csv_list = get_files_from_folder(args.shareholder_folder) or [
        "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/basic_shareholder_info1 (1).csv"
    ]

    flagged_csv_list = get_files_from_folder(args.flagged_folder) or [
        "/Users/wiktorrajca/Documents/GitHub/Data-Science-Honors-Thesis/matcher/output/flagged_little.csv"
    ]

    for country in countries:
        process_country(
            country, 
            procurement_csv_list, 
            first_level_shareholders_csv_list, 
            subsidiaries_csv_list, 
            shareholders_csv_list, 
            flagged_csv_list
        )

if __name__ == "__main__":
    main()