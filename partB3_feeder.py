"""
Helper for Part B Task 3. Splits the Auto contracts into history (70 %) and a replay set (30 %),
then replays the replay set as small JSON micro-batch files to simulate a live event stream.

  python partB3_feeder.py --prepare            # create output/stream_history.csv + output/stream_replay.csv
  python partB3_feeder.py --replay 10 --delay 3  # drop 10 JSON files, one every 3 seconds
"""
import argparse, json, os, time
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--prepare", action="store_true")
ap.add_argument("--replay", type=int, default=0, help="number of micro-batch files")
ap.add_argument("--delay", type=float, default=3.0)
a = ap.parse_args()
cols = ["contract_id", "client_age", "annual_premium_eur", "current_value_eur", "vehicle_age", "power_hp"]

if a.prepare:
    df = pd.read_csv("output/integrated_contracts.csv")
    auto = df[(df["product"] == "Auto") & df["current_value_eur"].notna()].sort_values("start_date")[cols]
    cut = int(len(auto) * 0.7)
    auto.iloc[:cut].to_csv("output/stream_history.csv", index=False)
    auto.iloc[cut:].to_csv("output/stream_replay.csv", index=False)
    print(f"history={cut} rows, replay={len(auto) - cut} rows")

if a.replay:
    os.makedirs("stream_in", exist_ok=True)
    rp = pd.read_csv("output/stream_replay.csv")
    size = len(rp) // a.replay
    for i in range(a.replay):
        chunk = rp.iloc[i * size:(i + 1) * size].copy()
        chunk["event_time"] = pd.Timestamp.now().isoformat()
        tmp, final = f"stream_in/.tmp_{i}.json", f"stream_in/batch_{i:03d}.json"
        chunk.to_json(tmp, orient="records", lines=True)
        os.rename(tmp, final)               # atomic move so Spark never reads half-written files
        print(f"wrote {final} ({len(chunk)} events)")
        time.sleep(a.delay)
