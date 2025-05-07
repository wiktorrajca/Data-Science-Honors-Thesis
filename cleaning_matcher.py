import os
import pandas as pd

def load_and_filter_csvs(directory_path, country_of_procurement):
    all_dfs = []

    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory_path, filename)
            df = pd.read_csv(file_path)

            # Ensure both columns are treated as strings
            df['WIN_COUNTRY_CODE'] = df['WIN_COUNTRY_CODE'].astype(str)
            df['bvdidnumber'] = df['bvdidnumber'].astype(str)
            df['ISO_COUNTRY_CODE'] = df['ISO_COUNTRY_CODE'].astype(str)

            # Filter rows
            df_filtered = df[df['ISO_COUNTRY_CODE'].str[:2] == country_of_procurement]
            df_filtered = df[df['WIN_COUNTRY_CODE'].str[:2] == df['bvdidnumber'].str[:2]]

            all_dfs.append(df_filtered)

    # Combine all filtered DataFrames into one
    combined_df = pd.concat(all_dfs, ignore_index=True)
    return combined_df