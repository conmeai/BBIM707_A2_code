#!/usr/bin/env bash
# BBIM707 A2 | Part B - Task 1: fault-tolerant ingestion into HDFS (bash version, runs INSIDE the namenode container)
# Input  : a folder with this layout (made on your PC by partB1_prepare_staging.py and copied in with docker cp)
#            raw/        contracts.csv vehicles.csv claims.csv        (flat files)
#            db_export/  integrated_contracts_db.csv                  (relational database export)
#            events/     claims_events.jsonl                          (semi-structured JSON Lines)
#            clean/contracts|vehicles|claims/*_clean.csv              (cleaned Part A output)
# Fault tolerance: retry with exponential back-off, write to _tmp name then atomic rename, replication, HDFS checksums
# Integrity      : MD5 of local file == MD5 of the HDFS copy; row counts for the raw files
# Efficiency     : parallel transfers, idempotent (skips files that are already in HDFS and identical)
# Usage: bash partB1_hdfs_ingest.sh [SRC_DIR]      env: HDFS_REPLICATION (default 3), MAX_PARALLEL (default 4)
SRC="${1:-/data_in}"; ROOT=/data/insurance; REPL="${HDFS_REPLICATION:-3}"; PAR="${MAX_PARALLEL:-4}"
LOG=ingest.log
log() { echo "$(date '+%F %T') $*" >> "$LOG"; }
md5_local() { md5sum "$1" | cut -d' ' -f1; }
md5_hdfs()  { hdfs dfs -cat "$1" | md5sum | cut -d' ' -f1; }

retry() {                                   # retry <command...>   (4 attempts: wait 2,4,8 s)
  local n=0 max=4 w
  until "$@"; do
    n=$((n + 1)); [ "$n" -ge "$max" ] && { log "GIVE UP: $*"; return 1; }
    w=$((2 ** n)); echo "  attempt $n failed, retrying in ${w}s..."; log "attempt $n failed for $* - retry in ${w}s"; sleep "$w"
  done
}

upload() {                                  # upload <local file> <hdfs dir>
  local src="$1" dir="$2" name final tmp t0=$SECONDS
  name=$(basename "$src"); final="$dir/$name"; tmp="$dir/_tmp_$name"
  hdfs dfs -mkdir -p "$dir" || return 1
  if hdfs dfs -test -e "$final" && [ "$(md5_hdfs "$final")" = "$(md5_local "$src")" ]; then
    echo "[SKIP] $name already in HDFS and identical"; log "SKIP $name"; return 0     # idempotent re-run
  fi
  hdfs dfs -put -f "$src" "$tmp" || return 1                 # 1) write under a temporary name
  hdfs dfs -rm -f "$final" > /dev/null 2>&1
  hdfs dfs -mv "$tmp" "$final" || return 1                   # 2) atomic rename = all-or-nothing
  hdfs dfs -setrep -w "$REPL" "$final" > /dev/null || return 1   # 3) replicate
  if [ "$(md5_hdfs "$final")" != "$(md5_local "$src")" ]; then   # 4) integrity check
    echo "[FAIL] checksum mismatch for $name"; log "FAIL checksum $name"; return 1
  fi
  echo "[OK]   $src -> $final (md5 verified, $((SECONDS - t0))s)"; log "OK $src -> $final"
}

echo "Source folder: $SRC | replication: $REPL | parallel transfers: $PAR"
T0=$SECONDS; pids=(); failed=0
mapfile -t FILES < <(cd "$SRC" && find . -type f | sed 's|^\./||' | sort)
for rel in "${FILES[@]}"; do
  while [ "$(jobs -rp | wc -l)" -ge "$PAR" ]; do sleep 0.2; done      # limit parallelism
  ( retry upload "$SRC/$rel" "$ROOT/$(dirname "$rel")" ) &
  pids+=($!)
done
for p in "${pids[@]}"; do wait "$p" || failed=$((failed + 1)); done
echo "Ingested ${#FILES[@]} files in $((SECONDS - T0))s, failures: $failed"

echo; echo "Row-count check (raw files, header excluded)"
for f in contracts vehicles claims; do
  l=$(( $(wc -l < "$SRC/raw/$f.csv") - 1 )); h=$(( $(hdfs dfs -cat "$ROOT/raw/$f.csv" | wc -l) - 1 ))
  [ "$l" = "$h" ] && s=MATCH || s=MISMATCH; printf '  %-10s local=%6s hdfs=%6s %s\n' "$f.csv" "$l" "$h" "$s"
done
echo; hdfs dfs -ls -R -h "$ROOT"
echo; hdfs fsck "$ROOT" -files -blocks | tail -15
[ "$failed" -eq 0 ]
