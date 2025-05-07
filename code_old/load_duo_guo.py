#!/usr/bin/env python3
import os
import glob
import argparse
import psycopg2

DSN = "dbname=procurement user=wiktorrajca password=pussinboots2 host=localhost port=5432"
TABLE = "orbis_guo_duo"

DDL = f"""
DROP TABLE IF EXISTS {TABLE};
CREATE TABLE {TABLE} (
  bvdidnumber                          text,
  bvdindependenceindicator            text,
  noofcompaniesincorporategroup        text,
  noofrecordedshareholders             text,
  noofrecordedsubsidiaries             text,
  noofrecordedbranchlocations          text,
  ishname                              text,
  ishsalutation                        text,
  ishfirstname                         text,
  ishlastname                          text,
  ishbvdidnumber                       text,
  ishcountryisocode                    text,
  ishcity                              text,
  ishtype                              text,
  ishsameorsimilarnameinthelexisne    text,
  ishdirect                            text,
  ishtotal                             text,
  ishinformationsource                 text,
  ishinformationdate                   text,
  ishalsoamanager                      text,
  guoname                              text,
  guosalutation                        text,
  guofirstname                         text,
  guolastname                          text,
  guobvdidnumber                       text,
  guocountryisocode                    text,
  guotype                              text,
  guocity                              text,
  guosameorsimilarnameinthelexisne    text,
  guodirect                            text,
  guototal                             text,
  guoinformationsource                 text,
  guoinformationdate                   text,
  guoalsoamanager                      text,
  duoname                              text,
  duosalutation                        text,
  duofirstname                         text,
  duolastname                          text,
  duobvdidnumber                       text,
  duocountryisocode                    text,
  duocity                              text,
  duotype                              text,
  duosameorsimilarnameinthelexisne    text,
  duodirect                            text,
  duototal                             text,
  duoinformationsource                 text,
  duoinformationdate                   text,
  duoalsoamanager                      text
);
"""

# Use default CSV delimiter (comma)
COPY_SQL = f"""
COPY {TABLE} FROM STDIN
WITH (FORMAT csv, HEADER true);
"""

def main():
    parser = argparse.ArgumentParser(
        description="Load all GUO/DUO CSVs into orbis_guo_duo"
    )
    parser.add_argument("folder", help="Directory containing .csv files")
    args = parser.parse_args()

    files = sorted(glob.glob(os.path.join(args.folder, "*.csv")))
    if not files:
        print("❌ No CSV files found in", args.folder)
        return

    conn = psycopg2.connect(DSN)
    cur  = conn.cursor()
    print("📋 Recreating table…")
    cur.execute(DDL)
    conn.commit()

    for idx, path in enumerate(files, 1):
        print(f"[{idx}/{len(files)}] Loading {os.path.basename(path)}…", end="", flush=True)
        with open(path, "r", encoding="utf-8") as f:
            try:
                cur.copy_expert(COPY_SQL, f)
                conn.commit()
                print(" done.")
            except Exception as e:
                conn.rollback()
                print(" FAILED.")
                print("   →", e)

    print("✅ All files processed.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()