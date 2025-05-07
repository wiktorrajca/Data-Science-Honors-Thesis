from matcher import merge_tables_on_processed_names

# Define paths to sample data
procurement_data_path = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/TED_winner_names.csv"
shareholder_data_path = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/BvD_ID_and_Name1.csv"

# Run the matcher and print a few results
print("🔍 Testing matcher.py ...\n")
matches = merge_tables_on_processed_names(procurement_data_path, shareholder_data_path)

# Print first 10 matches
for i, match in enumerate(iter(matches)):
    print(match)
    if i >= 9:  # Stop after printing 10 matches
        break

print("\n✅ Matcher test completed.")