import argparse
import glob
import os
from multiprocessing import Pool, cpu_count

import numpy as np
import pandas as pd
import regex as re  # Use the 'regex' module for Unicode support
from rapidfuzz import fuzz
from tqdm import tqdm
from unidecode import unidecode

# Initialize tqdm for pandas apply operations
tqdm.pandas()

# Compile regex patterns once for efficiency
SUFFIXES_RE = re.compile(r'\b(inc|corp|ltd|llc|company|co|sas)\b', re.IGNORECASE)
SPECIAL_CHARS_RE = re.compile(r'[\p{P}\p{S}]', re.IGNORECASE)
MULTIPLE_SPACES_RE = re.compile(r'\s+')
NON_LATIN_RE = re.compile(r'[^\p{Latin}\s\d]', re.IGNORECASE)
NUMBERS_RE = re.compile(r'\d+')

def preprocess_name(name):
    """
    Preprocesses a company name by cleaning, transliterating, and extracting numbers.

    Parameters:
        name (str): The original company name.

    Returns:
        tuple: A tuple containing:
            - processed_name (str or None): The cleaned and transliterated name.
            - is_transliterated (bool): Flag indicating if the name was transliterated.
            - numbers (list): List of numbers extracted from the name.
    """
    if isinstance(name, float) or pd.isnull(name):  # Handle NaN or missing values
        return None, False, []

    # Convert to lowercase
    name = name.lower()

    # Extract numbers from the name
    numbers = NUMBERS_RE.findall(name)

    # Remove common company suffixes
    name_cleaned = SUFFIXES_RE.sub('', name)

    # Remove punctuation and symbols
    name_cleaned = SPECIAL_CHARS_RE.sub('', name_cleaned)

    # Normalize whitespace
    name_cleaned = MULTIPLE_SPACES_RE.sub(' ', name_cleaned).strip()

    # If the name is empty after cleaning, return None
    if not name_cleaned:
        return None, False, numbers

    # Check for non-Latin characters
    if NON_LATIN_RE.search(name_cleaned):
        # Transliterate non-Latin characters to Latin alphabet
        name_transliterated = unidecode(name_cleaned)
        return name_transliterated, True, numbers  # Return transliterated name, flag, and numbers
    else:
        return name_cleaned, False, numbers  # Return cleaned name, flag, and numbers

def exact_match(table1, table2, processed_name_column='processed_name', name_column1='WIN_NAME',
                name_column2='name', id_column='bvdidnumber'):
    """
    Performs an exact match on the processed names between two tables, including a strict number check.

    Parameters:
        table1 (pd.DataFrame): The first table containing preprocessed names.
        table2 (pd.DataFrame): The second table containing preprocessed names.
        processed_name_column (str): The column name for processed names.
        name_column1 (str): The original name column in table1.
        name_column2 (str): The original name column in table2.
        id_column (str): The ID column in table2.

    Returns:
        pd.DataFrame: A DataFrame containing the matched records.
    """
    # Merge the two tables on the processed name column
    merged = pd.merge(
        table1,
        table2[[processed_name_column, name_column2, id_column, 'numbers']],
        on=processed_name_column,
        how='inner',
        suffixes=('', '_t2')
    )

    # Filter out matches where the numbers do not match (strict equality)
    merged = merged[merged['numbers'].apply(set) == merged['numbers_t2'].apply(set)]

    # Return the merged DataFrame
    return merged

def match_chunk(chunk, table2, processed_name_column1, processed_name_column2,
                original_name_column1, original_name_column2, id_column, similarity_threshold=0.8):
    """
    Matches a chunk of table1 with table2 using RapidFuzz's similarity ratio,
    ensuring numbers match and applying a letter check on the first three letters.

    Parameters:
        chunk (pd.DataFrame): A chunk of table1 to process.
        table2 (pd.DataFrame): The second table to match against.
        processed_name_column1 (str): The processed name column in table1.
        processed_name_column2 (str): The processed name column in table2.
        original_name_column1 (str): The original name column in table1.
        original_name_column2 (str): The original name column in table2.
        id_column (str): The ID column in table2.
        similarity_threshold (float): The minimum similarity score to consider a match.

    Returns:
        pd.DataFrame: A DataFrame containing the matched records.
    """
    matches = []

    # Convert chunk to list of dictionaries for faster access
    chunk_records = chunk.to_dict('records')

    # Precompute table2 data for faster access during matching
    table2_numbers = table2['numbers'].tolist()
    table2_names = table2[processed_name_column2].tolist()
    table2_original_names = table2[original_name_column2].tolist()
    table2_ids = table2[id_column].tolist()

    # Precompute first three letters of table2 names and build an index
    prefix_to_indices = {}
    for idx, name in enumerate(table2_names):
        prefix = name[:3].lower()
        if prefix not in prefix_to_indices:
            prefix_to_indices[prefix] = []
        prefix_to_indices[prefix].append(idx)

    # Iterate over each record in the chunk
    for row1 in tqdm(chunk_records, total=len(chunk_records), desc="Matching chunk"):
        name1 = row1[processed_name_column1]
        original_name1 = row1[original_name_column1]
        numbers1 = set(row1['numbers'])

        # Skip names that are too short (optional)
        # if len(name1) < 3:
        #     continue

        # Get first three letters of name1
        prefix1 = name1[:3].lower()

        # Retrieve candidate indices with matching prefixes
        candidate_indices = prefix_to_indices.get(prefix1, [])

        if not candidate_indices:
            continue

        # Filter candidates based on numbers (strict equality)
        valid_indices = []
        for idx in candidate_indices:
            nums2_set = set(table2_numbers[idx])
            if numbers1 == nums2_set:
                valid_indices.append(idx)

        if not valid_indices:
            continue

        # Prepare lists for similarity calculation
        candidates_names = [table2_names[idx] for idx in valid_indices]
        candidates_original_names = [table2_original_names[idx] for idx in valid_indices]
        candidates_ids = [table2_ids[idx] for idx in valid_indices]

        # Compute similarity scores using RapidFuzz's fuzz.ratio
        similarities = np.array([
            fuzz.ratio(name1.lower(), name2.lower()) / 100.0  # Normalize to [0, 1]
            for name2 in candidates_names
        ])

        # Filter candidates based on the similarity threshold
        above_threshold = similarities >= similarity_threshold
        idxs = np.where(above_threshold)[0]

        # Collect matching records
        for idx in idxs:
            match = row1.copy()  # Copy the original row to include all columns from table1
            match[original_name_column2] = candidates_original_names[idx]  # Add matching name from table2
            match[id_column] = candidates_ids[idx]  # Add matching ID from table2
            match['similarity'] = similarities[idx]  # Include similarity score for reference
            matches.append(match)

    # Return the matched records as a DataFrame
    return pd.DataFrame(matches)

def merge_with_parallel_processing(table1, table2, processed_name_column1, processed_name_column2,
                                   original_name_column1, original_name_column2, id_column, similarity_threshold=0.8):
    """
    Parallelizes the fuzzy matching process by splitting table1 into chunks and processing them in parallel.

    Parameters:
        table1 (pd.DataFrame): The first table containing transliterated names.
        table2 (pd.DataFrame): The second table to match against.
        processed_name_column1 (str): The processed name column in table1.
        processed_name_column2 (str): The processed name column in table2.
        original_name_column1 (str): The original name column in table1.
        original_name_column2 (str): The original name column in table2.
        id_column (str): The ID column in table2.
        similarity_threshold (float): The minimum similarity score to consider a match.

    Returns:
        pd.DataFrame: A DataFrame containing all matched records from the parallel processing.
    """
    print("Splitting data into chunks for parallel processing...")

    # Determine the number of CPU cores available
    num_cores = cpu_count()
    print(f"Using {num_cores} CPU cores...")

    # Split table1 into chunks equal to the number of cores
    chunks = np.array_split(table1, num_cores)

    # Prepare arguments for each chunk to pass to match_chunk
    args = [
        (
            chunk,
            table2,
            processed_name_column1,
            processed_name_column2,
            original_name_column1,
            original_name_column2,
            id_column,
            similarity_threshold
        ) for chunk in chunks
    ]

    # Perform matching in parallel using multiprocessing
    with Pool(processes=num_cores) as pool:
        results = pool.starmap(match_chunk, args)

    # Combine results from all processes into a single DataFrame
    print("Combining results from all processes...")
    return pd.concat(results, ignore_index=True)

def merge_tables_on_processed_names(table1_path, table2_path, name_column1='WIN_NAME',
                                    name_column2='name', id_column='bvdidnumber', similarity_threshold=0.8):
    """
    Main function to merge two tables based on processed names, including exact and fuzzy matching.

    Parameters:
        table1_path (str): Path to the base CSV file (table1).
        table2_path (str): Path to the additional CSV file (table2).
        name_column1 (str): Name column in the base file.
        name_column2 (str): Name column in the additional file.
        id_column (str): ID column in the additional file.
        similarity_threshold (float): The minimum similarity score for fuzzy matching.

    Returns:
        pd.DataFrame: A DataFrame containing the merged results.
    """
    print(f"Loading Table 1 from {table1_path}...")
    table1 = pd.read_csv(table1_path)
    print(f"Loading Table 2 from {table2_path}...")
    table2 = pd.read_csv(table2_path)

    # Record original columns of Table 1 before processing
    original_table1_columns = table1.columns.tolist()

    # Normalize column names by stripping whitespace
    table1.columns = table1.columns.str.strip()
    table2.columns = table2.columns.str.strip()

    # Ensure specified columns exist in the tables
    if name_column1 not in table1.columns:
        raise KeyError(f"Column '{name_column1}' not found in Table 1. Available columns: {table1.columns.tolist()}")
    if name_column2 not in table2.columns:
        raise KeyError(f"Column '{name_column2}' not found in Table 2. Available columns: {table2.columns.tolist()}")
    if id_column not in table2.columns:
        raise KeyError(f"Column '{id_column}' not found in Table 2. Available columns: {table2.columns.tolist()}")

    # Preprocess names in Table 1
    print("Preprocessing names in Table 1...")
    table1[['processed_name', 'is_transliterated', 'numbers']] = table1[name_column1].progress_apply(
        lambda x: pd.Series(preprocess_name(x))
    )

    # Preprocess names in Table 2
    print("Preprocessing names in Table 2...")
    table2[['processed_name', 'is_transliterated', 'numbers']] = table2[name_column2].progress_apply(
        lambda x: pd.Series(preprocess_name(x))
    )

    # Flag short names (3 characters or fewer)
    table1['is_short'] = table1['processed_name'].apply(lambda x: isinstance(x, str) and len(x) <= 3)
    table2['is_short'] = table2['processed_name'].apply(lambda x: isinstance(x, str) and len(x) <= 3)
    

    # Precompute name prefixes in Table 2 for optimized letter check
    table2['name_prefix'] = table2['processed_name'].str[:3].str.lower()

    # Drop rows with null or very short processed names from both tables
    table1 = table1.dropna(subset=['processed_name'])
    table1 = table1[table1['processed_name'].str.len() >= 3]
    table2 = table2.dropna(subset=['processed_name'])
    table2 = table2[table2['processed_name'].str.len() >= 3]

    # Perform exact matching on non-transliterated names
    print("Performing exact match with number check for non-transliterated names...")
    exact_table1 = table1[(~table1['is_transliterated']) | (table1['is_short'])]
    exact_table2 = table2[(~table2['is_transliterated']) | (table2['is_short'])]
    

    exact_matches = exact_match(
        exact_table1,
        exact_table2,
        processed_name_column='processed_name',
        name_column1=name_column1,
        name_column2=name_column2,
        id_column=id_column
    )

    # Perform fuzzy matching on transliterated names (parallelized)
    transliterated_table1 = table1[(table1['is_transliterated']) & (~table1['is_short'])]
    transliterated_table2 = table2  # Matching against all entries in table2

    if not transliterated_table1.empty and not transliterated_table2.empty:
        print("Performing fuzzy matching with number check and letter check for transliterated names (parallelized)...")
        fuzzy_matches = merge_with_parallel_processing(
            transliterated_table1,
            transliterated_table2,
            processed_name_column1='processed_name',
            processed_name_column2='processed_name',
            original_name_column1=name_column1,
            original_name_column2=name_column2,
            id_column=id_column,
            similarity_threshold=similarity_threshold
        )
    else:
        # Create an empty DataFrame if no transliterated names are present
        fuzzy_matches = pd.DataFrame(columns=table1.columns.tolist() + [name_column2, id_column, 'similarity'])

    # Combine exact and fuzzy matches
    print("Combining exact and fuzzy matches...")
    merged_results = pd.concat([exact_matches, fuzzy_matches], ignore_index=True)

    # Remove duplicate matches based on key columns
    merged_results.drop_duplicates(subset=[name_column1, name_column2, id_column], inplace=True)

    print(f"Total Unique Matches: {len(merged_results)}")

    # Prepare the final result DataFrame with original columns from Table 1 and matching columns from Table 2
    columns_to_include = original_table1_columns + [name_column2, id_column]
    result_df = merged_results[columns_to_include]

    # Return the final merged DataFrame
    return result_df

if __name__ == "__main__":
    import sys

    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description="Merge base CSV with additional CSV files using transliteration and optimized fuzzy matching.")
    parser.add_argument("base_file", help="Path to the base CSV file.")
    parser.add_argument("additional_dir", help="Directory containing additional CSV files.")
    parser.add_argument("--output_dir", default="output", help="Directory to save output files (default: output).")
    parser.add_argument("--name_column1", default="WIN_NAME", help="Name column in the base file (default: WIN_NAME).")
    parser.add_argument("--name_column2", default="name", help="Name column in additional files (default: name).")
    parser.add_argument("--id_column", default="bvdidnumber", help="ID column in additional files (default: bvdidnumber).")
    parser.add_argument("--file_prefix", default="BvD_ID_and_Name", help="Prefix for additional files (default: BvD_ID_and_Name).")
    parser.add_argument("--similarity_threshold", type=float, default=0.8, help="Minimum normalized similarity for fuzzy matching (default: 0.8).")
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Get a list of all additional CSV files to process
    additional_files = glob.glob(os.path.join(args.additional_dir, f"{args.file_prefix}*.csv"))

    # Check if any additional files were found
    if not additional_files:
        print(f"No files found with prefix '{args.file_prefix}' in directory '{args.additional_dir}'.")
        sys.exit(1)

    # Process each additional file
    for additional_file_path in tqdm(additional_files, desc="Processing Files"):
        # Extract a unique identifier from the file name for output naming
        file_identifier = os.path.basename(additional_file_path).replace(f"{args.file_prefix}", "").replace(".csv", "")

        # Perform the merging process
        result_df = merge_tables_on_processed_names(
            table1_path=args.base_file,
            table2_path=additional_file_path,
            name_column1=args.name_column1,
            name_column2=args.name_column2,
            id_column=args.id_column,
            similarity_threshold=args.similarity_threshold
        )

        # Construct the output file path
        output_file_path = os.path.join(args.output_dir, f"merged_result_{file_identifier}.csv")

        # Save the merged result to a CSV file
        result_df.to_csv(output_file_path, index=False)
        print(f"Saved merged result to {output_file_path}")