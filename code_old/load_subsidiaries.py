import os
import glob
import pandas as pd
import psycopg2
from tqdm import tqdm
from datetime import datetime

# --- Config ---
FOLDER = "/Users/wiktorrajca/Desktop/Research/URAP_Fedyk/data/Orbis_Data/subsidiaries_first_level"
DB_NAME = "procurement"
DB_USER = "wiktorrajca"
DB_PASSWORD = "pussinboots2"
DB_HOST = "localhost"
DB_PORT = "5432"
TABLE_NAME = "orbis_subsidiaries"

# --- Connect ---
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cur = conn.cursor()

# --- Helper: Parse dates safely ---
def parse_date(val):
    try:
        return datetime.strptime(val.strip(), "%Y/%m").date()
    except:
        return None

# --- Load files ---
all_files = glob.glob(os.path.join(FOLDER, "*.csv"))
print(f"📂 Found {len(all_files)} files to load...")

for file_path in tqdm(all_files, desc="Loading files"):
    df = pd.read_csv(file_path, dtype=str)

    # Filter and rename
    df = df.rename(columns={
        "bvdidnumber": "parent_bvdid",
        "subsidiarybvdidnumber": "subsidiary_bvdid",
        "subsidiaryname": "subsidiary_name",
        "subsidiarycountryisocode": "country_code",
        "subsidiarycity": "city",
        "subsidiarytype": "type",
        "subsidiarydirect": "direct_ownership",
        "subsidiarytotal": "total_ownership",
        "subsidiaryinformationdate": "info_date"
    })

    # Drop rows with missing subsidiary BVD ID
    df = df[df["subsidiary_bvdid"].notna()]

    # Clean/convert columns
    df["direct_ownership"] = pd.to_numeric(df["direct_ownership"], errors="coerce")
    df["total_ownership"] = pd.to_numeric(df["total_ownership"], errors="coerce")
    df["info_date"] = df["info_date"].apply(parse_date)

    for row in df.itertuples(index=False):
        cur.execute(f"""
            INSERT INTO {TABLE_NAME} (
                parent_bvdid, subsidiary_bvdid, subsidiary_name,
                country_code, city, type, direct_ownership,
                total_ownership, info_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row.parent_bvdid,
            row.subsidiary_bvdid,
            row.subsidiary_name,
            row.country_code,
            row.city,
            row.type,
            row.direct_ownership,
            row.total_ownership,
            row.info_date
    ))

    conn.commit()

cur.close()
conn.close()
print("✅ All subsidiaries loaded.")