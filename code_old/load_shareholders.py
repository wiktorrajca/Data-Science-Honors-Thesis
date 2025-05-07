import os
import glob
import csv
import psycopg2
from tqdm import tqdm
from datetime import datetime

FOLDER = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/shareholders_first_level"
DB_NAME = "procurement"
DB_USER = "wiktorrajca"
DB_PASSWORD = "pussinboots2"
DB_HOST = "localhost"
DB_PORT = "5432"
TABLE_NAME = "orbis_shareholders"

# --- Connect ---
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cur = conn.cursor()

# --- Temporary cleaned file path ---
TEMP_FILE = "/tmp/shareholder_cleaned.csv"

def parse_date(value):
    try:
        return datetime.strptime(value.strip(), "%Y/%m").date()
    except:
        return ""

def clean_and_save_subset(original_file, temp_file_path):
    with open(original_file, "r", encoding="utf-8") as infile, open(temp_file_path, "w", encoding="utf-8", newline="") as outfile:
        reader = csv.DictReader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            if not row.get("shareholderbvdidnumber"):
                continue

            writer.writerow([
                row.get("bvdidnumber"),
                row.get("shareholderbvdidnumber"),
                row.get("shareholdername"),
                row.get("shareholdercountryisocode"),
                row.get("shareholdercity"),
                row.get("shareholdertype"),
                row.get("shareholderdirect") if row.get("shareholderdirect") != "-" else None,
                row.get("shareholdertotal") if row.get("shareholdertotal") != "-" else None,
                parse_date(row.get("shareholderinformationdate"))
            ])

def copy_into_postgres(temp_file_path):
    with open(temp_file_path, "r", encoding="utf-8") as f:
        cur.copy_expert(f"""
            COPY {TABLE_NAME} (
                parent_bvdid,
                shareholder_bvdid,
                shareholder_name,
                country_code,
                city,
                type,
                direct_ownership,
                total_ownership,
                info_date
            ) FROM STDIN WITH (FORMAT CSV)
        """, f)
    conn.commit()

# --- Process files ---
files = glob.glob(os.path.join(FOLDER, "*.csv"))
print(f"📂 Found {len(files)} CSV files.")

for file_path in tqdm(files, desc="Processing and copying"):
    clean_and_save_subset(file_path, TEMP_FILE)
    copy_into_postgres(TEMP_FILE)

# --- Cleanup ---
cur.close()
conn.close()
print("✅ All shareholder data loaded with COPY.")