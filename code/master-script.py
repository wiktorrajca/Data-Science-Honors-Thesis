import os
import subprocess
import argparse
import glob
import shutil

def run_matcher(base_file, name_column1, additional_dir, output_dir, file_prefix):
    """
    Runs matcher.py to match procurement/sanction data with Orbis records.
    """
    output_path = os.path.join(output_dir)
    cmd = [
        "python3", "matcher.py", base_file, additional_dir,
        "--output_dir", output_dir,
        "--name_column1", name_column1,
        "--file_prefix", file_prefix
    ]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    return output_path

def run_main(procurement_path, country, shareholders_path, subsidiaries_path, controlling_path, flagged_path):
    """
    Runs main.py to construct the procurement fraud detection graph.
    """
    cmd = [
        "python3", "main.py",
        "--country", country,
        "--procurement_folder", procurement_path,
        "--first_level_shareholders_folder", shareholders_path,
        "--subsidiary_folder", subsidiaries_path,
        "--shareholder_folder", controlling_path,
        "--flagged_folder", flagged_path
    ]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def get_orbis_files(orbis_dir):
    """
    Searches for specific files in the given Orbis directory:
    - 'subsidiaries' in the filename -> Subsidiaries data
    - 'basic' in the filename -> Controlling shareholders data
    - 'shareholders_first' in the filename -> First-level shareholders data

    Parameters:
        orbis_dir (str): The path to the Orbis data folder.

    Returns:
        dict: Dictionary containing lists of file paths for each category.
    """
    if not os.path.isdir(orbis_dir):
        raise FileNotFoundError(f"Orbis directory not found: {orbis_dir}")

    subsidiaries_files = glob.glob(os.path.join(orbis_dir, "*subsidiaries*.csv"))
    controlling_files = glob.glob(os.path.join(orbis_dir, "*basic*.csv"))
    shareholders_files = glob.glob(os.path.join(orbis_dir, "*shareholders_first*.csv"))

    return {
        "subsidiaries": subsidiaries_files,
        "controlling": controlling_files,
        "shareholders": shareholders_files
    }

def main():
    parser = argparse.ArgumentParser(description="Master script for procurement fraud detection graph.")
    parser.add_argument("--ted_data", type=str, default="/disk/homedirs/nber/tder/bulk/procurement/data/intermediate/", help="Path to TED procurement dataset.")
    parser.add_argument("--sanction_data", type=str, default="/disk/homedirs/nber/tder/bulk/procurement/data/intermediate/", help="Path to sanctioned entities dataset.")
    parser.add_argument("--orbis_dir", type=str, default="/disk/homedirs/nber/tder/bulk/procurement/data/intermediate/ORBIS", help="Directory containing Orbis firm datasets.")
    parser.add_argument("--country", type=str, default="PL", help="Specify for which country you want to create graphs for")
    parser.add_argument("--output_dir", type=str, default="output", help="Directory for merged datasets.")
    args = parser.parse_args()
    
    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Get the absolute path of the script directory
    script_dir = os.getcwd()

    # Step 1: Match TED procurement data with Orbis
    merged_ted_path = os.path.join(script_dir, args.output_dir, "TED")
    run_matcher(args.ted_data, "WIN_NAME", args.orbis_dir, merged_ted_path, "BvD_ID_and_Name")
    print("✅ Merged TED path:", merged_ted_path)

    # Step 2: Match sanction list with Orbis
    merged_sanction_path = os.path.join(script_dir, args.output_dir, "Sanctions")
    run_matcher(args.sanction_data, "name", args.orbis_dir, merged_sanction_path, "BvD_ID_and_Name")
    print("✅ Merged Sanctions path:", merged_sanction_path)

    # Step 3: Get Orbis files
    orbis_files = get_orbis_files(args.orbis_dir)
    print("Orbis files:", orbis_files)

    orbis_output_paths = {}
    for category, files in orbis_files.items():
        folder_path = os.path.join(script_dir, args.output_dir, category)
        os.makedirs(folder_path, exist_ok=True)  # Create folder if it doesn't exist
        for file in files:
            shutil.move(file, folder_path)  # Move file to the folder
        orbis_output_paths[category] = folder_path  # Store folder path

    print("✅ Organized Orbis files into folders:", orbis_output_paths)

    # Step 4: Run main.py with merged datasets
    run_main("/Users/wiktorrajca/Documents/GitHub/Data-Science-Honors-Thesis/code/output_test/TED", args.country, orbis_output_paths["shareholders"], orbis_output_paths["subsidiaries"], orbis_output_paths["controlling"], "/Users/wiktorrajca/Documents/GitHub/Data-Science-Honors-Thesis/code/output_test/Sanctions")

if __name__ == "__main__":
    main()
