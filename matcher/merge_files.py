import pandas as pd
import argparse
from tqdm import tqdm
import glob
from matcher_modules.preprocessor import preprocess_dataframe
from matcher_modules.exact_matcher import exact_match
from matcher_modules.fuzzy_matcher import match_chunk
from matcher_modules.parallel import parallel_match
from matcher_modules.utils import validate_columns, get_output_path
import os

def merge_tables(table1_path, table2_path, config):
    # Load data
    table1 = pd.read_csv(table1_path)
    table2 = pd.read_csv(table2_path)
    
    # Preprocess
    table1 = preprocess_dataframe(table1, config['name_column1'])
    table2 = preprocess_dataframe(table2, config['name_column2'])
    
    # Exact matching
    exact_matches = exact_match(
        table1[~table1['is_transliterated']],
        table2[~table2['is_transliterated']],
        processed_name_column=config['processed_name_column1'],
        name_column1=config['name_column1'],
        name_column2=config['name_column2'],
        id_column=config['id_column']
    )
    
    # Fuzzy matching
    fuzzy_matches = parallel_match(
        table1[table1['is_transliterated']],
        table2,
        match_function=lambda chunk, t2, thresh: match_chunk(
            chunk, t2, 
            processed_name_column1=config['processed_name_column1'],
            processed_name_column2=config['processed_name_column2'],
            original_name_column1=config['name_column1'],
            original_name_column2=config['name_column2'],
            id_column=config['id_column'],
            similarity_threshold=thresh
        ),
        similarity_threshold=config['similarity_threshold']
    )
    
    return pd.concat([exact_matches, fuzzy_matches])

if __name__ == "__main__":
    import sys
    # Import the parser setup
    from matcher_modules.utils import setup_arg_parser
    
    # Set up and parse arguments
    parser = setup_arg_parser()
    args = parser.parse_args()
    
    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Get a list of all additional CSV files to process
    additional_files = glob.glob(os.path.join(args.additional_dir, f"{args.file_prefix}*.csv"))

    # Check if any additional files were found
    if not additional_files:
        print(f"No files found with prefix '{args.file_prefix}' in directory '{args.additional_dir}'.")
        sys.exit(1)

    # Define configuration
    config = {
        'name_column1': args.name_column1,
        'name_column2': args.name_column2,
        'id_column': args.id_column,
        'processed_name_column1': 'processed_name',
        'processed_name_column2': 'processed_name',
        'similarity_threshold': 0.8
    }
    # Process each additional file
    for additional_file_path in tqdm(additional_files, desc="Processing Files"):
        # Extract a unique identifier from the file name for output naming
        file_identifier = os.path.basename(additional_file_path).replace(f"{args.file_prefix}", "").replace(".csv", "")

        # Perform the merging process
        result_df = merge_tables(
            table1_path=args.base_file,
            table2_path=additional_file_path,
            config=config
        )

        # Construct the output file path
        output_file_path = os.path.join(args.output_dir, f"merged_result_{file_identifier}.csv")

        # Save the merged result to a CSV file
        result_df.to_csv(output_file_path, index=False)
        print(f"Saved merged result to {output_file_path}")