"""
BBIM707 A2 | Part B - Task 4: Kafka consumer group - consistency + fault tolerance on the consumer side.
Manual offset commit AFTER processing -> at-least-once delivery; idempotent processing (de-duplicate by contract_id)
-> effectively-once results. If a consumer dies, its partitions are re-assigned to the others in the group.

  python3 partB4_kafka_consumer.py --id c1      (run 2-3 copies in separate terminals, then kill one)
"""
import argparse, json, time
from collections import defaultdict
from confluent_kafka import Consumer, Producer, KafkaError

ap = argparse.ArgumentParser(); ap.add_argument("--id", default="c1"); a = ap.parse_args()
c = Consumer({"bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092", "group.id": "insurance-analytics",
              "client.id": a.id, "enable.auto.commit": False, "auto.offset.reset": "earliest",
              "isolation.level": "read_committed", "session.timeout.ms": 10000})
c.subscribe(["insurance.contracts"],
            on_assign=lambda cons, parts: print(f"[{a.id}] assigned:", [p.partition for p in parts]),
            on_revoke=lambda cons, parts: print(f"[{a.id}] revoked :", [p.partition for p in parts]))
dlq = Producer({"bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092", "acks": "all"})
seen, premium_sum, n, received = set(), defaultdict(float), 0, 0
last_status = time.time()
try:
    while True:
        m = c.poll(1.0)
        if m is None:
            if time.time() - last_status > 5:               # idle heartbeat: shows the consumer is alive and caught up
                print(f"[{a.id}] waiting for new events... received={received} unique_processed={n}")
                last_status = time.time()
            continue
        if m.error():
            if m.error().code() != KafkaError._PARTITION_EOF:
                print("ERROR", m.error())
            continue
        try:
            ev = json.loads(m.value())
            assert ev["annual_premium_eur"] > 0
        except Exception as exc:                          # poison message: park it, do not crash or block the partition
            dlq.produce("insurance.contracts.dlq", key=m.key(), value=m.value(), headers=[("error", str(exc).encode())])
            dlq.flush(); c.commit(message=m, asynchronous=False); continue
        if ev["contract_id"] not in seen:                 # idempotent processing
            seen.add(ev["contract_id"])
            band = "high" if ev["annual_premium_eur"] > 1000 else "standard"
            premium_sum[band] += ev["annual_premium_eur"]; n += 1
        c.commit(message=m, asynchronous=False)            # commit offset only after processing
        received += 1
        if received % 100 == 0:
            print(f"[{a.id}] p{m.partition()} off={m.offset()} received={received} unique_processed={n} premium_by_band={ {k: round(v) for k, v in premium_sum.items()} }")
except KeyboardInterrupt:
    print(f"[{a.id}] stopped: received={received} unique_processed={n}")
finally:
    c.close()
