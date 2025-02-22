import os
import glob
import argparse

def validate_columns(df, required_columns, df_name):
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {df_name}: {missing}")

def get_output_path(input_path, output_dir, prefix="merged"):
    base_name = os.path.basename(input_path)
    return os.path.join(output_dir, f"{prefix}_{base_name}")
def setup_arg_parser():
    """Configure and return the argument parser for CLI inputs."""
    parser = argparse.ArgumentParser(
        description="Merge CSV files using transliteration and optimized fuzzy matching."
    )
    
    # Required arguments
    parser.add_argument(
        "base_file",
        help="Path to the base CSV file (e.g., 'input/base.csv')."
    )
    parser.add_argument(
        "additional_dir",
        help="Directory containing additional CSV files to merge."
    )
    
    # Optional arguments
    parser.add_argument(
        "--output_dir",
        default="output",
        help="Output directory for merged files (default: 'output')."
    )
    parser.add_argument(
        "--name_column1",
        default="WIN_NAME",
        help="Name column in the base file (default: 'WIN_NAME')."
    )
    parser.add_argument(
        "--name_column2",
        default="name",
        help="Name column in additional files (default: 'name')."
    )
    parser.add_argument(
        "--id_column",
        default="bvdidnumber",
        help="ID column in additional files (default: 'bvdidnumber')."
    )
    parser.add_argument(
        "--file_prefix",
        default="BvD_ID_and_Name",
        help="Prefix for additional files (default: 'BvD_ID_and_Name')."
    )
    parser.add_argument(
        "--similarity_threshold",
        type=float,
        default=0.8,
        help="Minimum similarity score for fuzzy matching (default: 0.8)."
    )
    
    return parser