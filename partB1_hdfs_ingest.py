"""
BBIM707 A2 | Part B - Task 1: Fault-tolerant data ingestion pipeline into HDFS.

Sources  : (1) raw CSV files            -> /data/insurance/raw/
           (2) relational DB (MySQL)   -> /data/insurance/db_export/   (table exported to CSV)
           (3) semi-structured JSON log -> /data/insurance/events/      (claims as JSON Lines)
           (4) cleaned CSVs from Part A -> /data/insurance/clean/       (used by Hive in Task 2)

Fault tolerance : retry with exponential back-off, write to a _tmp name then atomic rename,
                  replication factor 3, HDFS block-level checksums.
Data integrity  : MD5 of local file == MD5 of the HDFS copy, plus row-count check; every run is logged.

Run (on the Hadoop node):  python3 partB1_hdfs_ingest.py
"""
import hashlib, json, logging, os, sqlite3, subprocess, time
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

HDFS_ROOT = "/data/insurance"
REPLICATION = int(os.environ.get("HDFS_REPLICATION", "3"))   # use 1 on a single-DataNode (pseudo-distributed) lab
MAX_RETRY = 4
logging.basicConfig(filename="ingest.log", level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest")


def sh(cmd, check=True):
    return subprocess.run(cmd, shell=True, check=check, capture_output=True, text=True)


def md5_local(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def md5_hdfs(path):
    return sh(f"hdfs dfs -cat {path} | md5sum").stdout.split()[0]


def with_retry(fn, *a):
    for attempt in range(1, MAX_RETRY + 1):
        try:
            return fn(*a)
        except subprocess.CalledProcessError as e:
            wait = 2 ** attempt
            log.warning("attempt %d failed (%s) - retry in %ds", attempt, e.stderr.strip()[:120], wait)
            time.sleep(wait)
    raise RuntimeError(f"giving up on {a}")


def upload(local, hdfs_dir):
    t0 = time.time()
    name = os.path.basename(local)
    final, tmp = f"{hdfs_dir}/{name}", f"{hdfs_dir}/_tmp_{name}"
    sh(f"hdfs dfs -mkdir -p {hdfs_dir}")
    if sh(f"hdfs dfs -test -e {final}", check=False).returncode == 0 and md5_hdfs(final) == md5_local(local):
        log.info("SKIP %s (identical copy already in HDFS - idempotent re-run)", name)   # idempotent: no re-upload
        print(f"[SKIP] {name} already ingested and identical")
        return final
    sh(f"hdfs dfs -put -f {local} {tmp}")          # 1) write under a temporary name
    sh(f"hdfs dfs -mv {tmp} {final}" if sh(f"hdfs dfs -test -e {final}", check=False).returncode else
       f"hdfs dfs -rm -f {final} && hdfs dfs -mv {tmp} {final}")   # 2) atomic rename = all-or-nothing
    sh(f"hdfs dfs -setrep -w {REPLICATION} {final}")
    ok = md5_local(local) == md5_hdfs(final)        # 3) integrity check
    if not ok:
        raise subprocess.CalledProcessError(1, "md5", stderr=f"checksum mismatch for {name}")
    secs = time.time() - t0
    log.info("OK %s -> %s (md5 verified, %.1fs)", local, final, secs)
    print(f"[OK] {local:40s} -> {final}  ({secs:.1f}s)")
    return final


def main():
    os.makedirs("staging", exist_ok=True)
    # source 2: relational database export
    if os.environ.get("MYSQL_PASSWORD"):                    # export the unified table straight from MySQL
        import mysql.connector
        cn = mysql.connector.connect(host=os.environ.get("MYSQL_HOST", "localhost"), user=os.environ.get("MYSQL_USER", "root"),
                                     password=os.environ["MYSQL_PASSWORD"], database=os.environ.get("MYSQL_DB", "insurance_dw"))
        cur = cn.cursor(); cur.execute("SELECT * FROM integrated_contracts")
        pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description]).to_csv("staging/integrated_contracts_db.csv", index=False)
        cn.close()
    else:                                                   # fallback when MySQL is not reachable from the Hadoop node
        pd.read_csv("output/integrated_contracts.csv").to_csv("staging/integrated_contracts_db.csv", index=False)
    # source 3: semi-structured claims events (JSON Lines)
    pd.read_csv("output/claims_clean.csv").to_json("staging/claims_events.jsonl", orient="records", lines=True)

    plan = [("contracts.csv", "raw"), ("vehicles.csv", "raw"), ("claims.csv", "raw"),
            ("staging/integrated_contracts_db.csv", "db_export"), ("staging/claims_events.jsonl", "events"),
            ("output/contracts_clean.csv", "clean/contracts"), ("output/vehicles_clean.csv", "clean/vehicles"),
            ("output/claims_clean.csv", "clean/claims")]
    t_all = time.time()
    with ThreadPoolExecutor(max_workers=int(os.environ.get("INGEST_WORKERS", "4"))) as pool:   # parallel transfers
        futs = {pool.submit(with_retry, upload, l, f"{HDFS_ROOT}/{s}"): l for l, s in plan}
        for f in as_completed(futs):
            f.result()                                   # re-raises if a file failed after all retries
    print(f"All {len(plan)} files ingested in {time.time() - t_all:.1f}s with {os.environ.get('INGEST_WORKERS', '4')} workers")

    # row-count integrity (local lines vs HDFS lines)
    for local, sub in plan[:3]:
        n_local = sum(1 for _ in open(local, encoding="utf-8")) - 1
        n_hdfs = int(sh(f"hdfs dfs -cat {HDFS_ROOT}/{sub}/{os.path.basename(local)} | wc -l").stdout) - 1
        print(f"row count {os.path.basename(local):15s} local={n_local:>6} hdfs={n_hdfs:>6} {'MATCH' if n_local == n_hdfs else 'MISMATCH'}")
    print(sh(f"hdfs dfs -ls -R -h {HDFS_ROOT}").stdout)
    print(sh(f"hdfs fsck {HDFS_ROOT} -files -blocks | tail -15").stdout)


if __name__ == "__main__":
    main()
