import argparse
from graph_builder import (
    load_or_initialize_graph, 
    save_graph, 
    add_procurement_winners, 
    expand_graph, 
    analyze_flagged_procurements,
    add_flagged_entities
)

def loading_creating(procurement_csv, country, first_level_shareholders_csv, subsidiaries_csv, shareholders_csv, flagged_csv, depth=1):
    # Step 1: Load existing graph or create a new one
    G = load_or_initialize_graph(country)

    # Step 2: Add procurement winners (only if they are not already present)
    if not any(G.nodes[n].get("bid_winner") for n in G.nodes):
        add_procurement_winners(G, country, procurement_csv)

    # Step 3: Expand the graph with additional ownership layers
    expand_graph(G, first_level_shareholders_csv, subsidiaries_csv, shareholders_csv, depth)

    # Step 4: Add flagged entities (ONLY IF THEY EXIST IN THE GRAPH)
    add_flagged_entities(G, flagged_csv)

    # Step 5: Save the updated graph
    save_graph(G, country)

def main():
    parser = argparse.ArgumentParser(description="Build procurement fraud detection graphs.")
    parser.add_argument(
        "--country",
        nargs="+",  # Allows multiple country codes as a list
        help="Specify one or more country codes to generate graphs for specific countries. If omitted, generates graphs for all countries."
    )
    args = parser.parse_args()
    # Define paths to CSV files
    procurement_csv = "/Users/wiktorrajca/Documents/GitHub/Data-Science-Honors-Thesis/matcher/output/merged_result_3.csv"
    shareholders_csv = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/basic_shareholder_info1 (1).csv"
    subsidiaries_csv = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/subsidiaries_first_level1.csv"
    first_level_shareholders_csv = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/shareholders_first_level1.csv"
    controlling_shareholders_csv = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/controlling_shareholders1.csv"
    flagged_csv = "/Users/wiktorrajca/Documents/GitHub/Data-Science-Honors-Thesis/matcher/output/flagged_little.csv"

    countries = args.country

    if countries is None:
        countries = ['IT', 'FR', 'CZ', 'DK', 'DE', 'EE', 'NL', 'PT', 'PL', 'BG', 'HU', 'MT', 'LU', 'ES', 'GR', 'SE', 'LT', 'CY', 'BE', 'LV', 'SK', 'UK', 'RO', 'SI', 'IE', 'AT', 'SA', 'US', 'MK', 'NO', 'CH', 'HR', 'FI', 'AU', 'TR', 'IS', 'SR', 'AD', 'BT', 'JP', 'HK', 'AX', 'VU', 'ZA', 'MA', 'BY', 'RU', 'CA', 'KE', 'GA', 'VN', 'LI', 'TN', 'OM', 'AF', 'RS', 'AE', 'MU', 'VA', 'SG', 'AR', 'GE', 'IL', 'BI', 'CN', 'NE', 'PE', 'BF', 'VG', 'EG', 'UA', 'AM', 'AI', 'PG', 'SZ', 'BA', 'IN', 'FK', 'BR', 'JO', 'KR', 'SV', 'NP', 'SM', 'VI', 'IO', 'VE', 'AL', '1A', 'PH', 'TG', 'ET', 'CC', 'IQ', 'MC', 'LB', 'DZ', 'NZ', 'MY', 'IR', 'FO', 'WF', 'BB', 'PA', 'AZ', 'BZ', 'MG', 'TW', 'TZ', 'SD', 'MZ', 'PR', 'CO', 'ME', 'PN', 'GM', 'GH', 'TH', 'UG', 'MD', 'MS', 'UM', 'KN', 'MR', 'PK', 'EC', 'BJ', 'KP', 'UY', 'BO', 'HN', 'BM', 'GG', 'CR', 'CL', 'GD', 'GN', 'NI']
    for country in countries:
        loading_creating(procurement_csv, country, first_level_shareholders_csv, subsidiaries_csv, shareholders_csv, flagged_csv, depth=1)

if __name__ == "__main__":
    main()