import pandas as pd

def exact_match(table1, table2, processed_name_column='processed_name',
                name_column1='WIN_NAME', name_column2='name', id_column='bvdidnumber'):
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