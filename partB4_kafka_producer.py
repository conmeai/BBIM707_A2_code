"""
BBIM707 A2 | Part B - Task 4: Kafka producer - streams contract events to topic 'insurance.contracts'.
Reliability settings:  acks=all, enable.idempotence=true (no duplicates on retry), retries, key = contract_id
(so all events of a contract go to the same partition -> per-key ordering).

  pip install confluent-kafka pandas
  python3 partB4_kafka_producer.py --rate 20
"""
import argparse, json, time
import pandas as pd
from confluent_kafka import Producer

ap = argparse.ArgumentParser(); ap.add_argument("--rate", type=float, default=20, help="events per second")
ap.add_argument("--limit", type=int, default=0)
a = ap.parse_args()

conf = {"bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
        "acks": "all", "enable.idempotence": True, "retries": 10, "linger.ms": 20, "compression.type": "snappy",
        "client.id": "insurance-producer"}
p = Producer(conf)
sent = failed = 0


def on_delivery(err, msg):
    global sent, failed
    if err:
        failed += 1; print("DELIVERY FAILED:", err)
    else:
        sent += 1
        if sent % 200 == 0:
            print(f"delivered {sent} | partition={msg.partition()} offset={msg.offset()}")


cols = ["contract_id", "client_age", "annual_premium_eur", "current_value_eur", "vehicle_age", "power_hp"]
df = pd.read_csv("output/stream_replay.csv")[cols]


def valid(rec):
    """Schema + range validation before publishing; invalid events go to the dead-letter topic instead."""
    return (isinstance(rec["contract_id"], str) and all(pd.notna(rec[c]) for c in cols[1:])
            and 18 <= rec["client_age"] <= 100 and rec["annual_premium_eur"] > 0 and rec["current_value_eur"] > 0)
if a.limit:
    df = df.head(a.limit)
for rec in df.to_dict("records"):
    rec["event_time"] = pd.Timestamp.now().isoformat()
    topic = "insurance.contracts" if valid(rec) else "insurance.contracts.dlq"      # dead-letter queue
    p.produce(topic, key=str(rec["contract_id"]), value=json.dumps(rec, default=str), on_delivery=on_delivery)
    p.poll(0)
    time.sleep(1 / a.rate)
p.flush()
print(f"finished: delivered={sent} failed={failed}")
