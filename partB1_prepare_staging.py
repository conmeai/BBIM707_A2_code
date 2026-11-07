"""
Run on YOUR PC (needs pandas) BEFORE the HDFS step. Builds the folder structure that the ingest script expects.
  python partB1_prepare_staging.py            -> creates ./hdfs_input/
Then:  docker cp hdfs_input namenode:/data_in
"""
import os, shutil
import pandas as pd

OUT = "hdfs_input"
shutil.rmtree(OUT, ignore_errors=True)
for d in ["raw", "db_export", "events", "clean/contracts", "clean/vehicles", "clean/claims"]:
    os.makedirs(f"{OUT}/{d}")
for f in ["contracts", "vehicles", "claims"]:                      # source 1: flat files
    shutil.copy(f"{f}.csv", f"{OUT}/raw/{f}.csv")
    shutil.copy(f"output/{f}_clean.csv", f"{OUT}/clean/{f}/{f}_clean.csv")
# source 2: export of the relational table (from the integrated CSV; MySQL export gives the same content)
pd.read_csv("output/integrated_contracts.csv").to_csv(f"{OUT}/db_export/integrated_contracts_db.csv", index=False)
# source 3: semi-structured events (claims as JSON Lines)
pd.read_csv("output/claims_clean.csv").to_json(f"{OUT}/events/claims_events.jsonl", orient="records", lines=True)
for root, _, files in os.walk(OUT):
    for f in files:
        print(os.path.join(root, f))
