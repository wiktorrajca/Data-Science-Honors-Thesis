import numpy as np
import pandas as pd
from tqdm import tqdm
from rapidfuzz import fuzz

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