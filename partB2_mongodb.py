"""
BBIM707 A2 | Part B - Task 2 (MongoDB part): hybrid document model.
  contracts : one document per contract, vehicle EMBEDDED (1:0..1, read together, rarely changes independently)
  claims    : separate collection, REFERENCES contract_id (claims have their own lifecycle: status updates,
              can grow beyond one per contract, so embedding would create an unbounded array)
Features demonstrated: $jsonSchema validators and indexes created WITH the collections, unique keys,
indexed lookup fields, $match-first aggregation, parameterised guarded updates, duplicate detection, explain().

  pip install pymongo pandas ; mongod must be running on localhost:27017
  python3 partB2_mongodb.py [--uri mongodb://localhost:27017]
"""
import argparse
import pandas as pd
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError, WriteError

ap = argparse.ArgumentParser(); ap.add_argument("--uri", default="mongodb://localhost:27017"); a = ap.parse_args()
db = MongoClient(a.uri)["insurance"]
db.client.drop_database("insurance")

PRODUCTS = ["Auto", "Home", "Life", "Health"]
CLAIM_STATUS = ["Open", "In_progress", "Expert_review", "Closed", "Rejected"]
ALLOWED_TRANSITIONS = {"Open": ["In_progress", "Rejected"], "In_progress": ["Expert_review", "Closed", "Rejected"],
                       "Expert_review": ["Closed", "Rejected"]}

# ---- 1) create collections WITH validation rules (data integrity enforced by the database)
db.create_collection("contracts", validator={"$jsonSchema": {
    "bsonType": "object", "required": ["_id", "contract_id", "client_id", "product", "status", "annual_premium_eur"],
    "properties": {
        "_id": {"bsonType": "string"}, "contract_id": {"bsonType": "string", "pattern": "^CTR_[0-9]{6}$"},
        "client_id": {"bsonType": "string"}, "product": {"enum": PRODUCTS},
        "client_age": {"bsonType": ["int", "long", "double"], "minimum": 18, "maximum": 100},
        "annual_premium_eur": {"bsonType": ["double", "int", "long"], "exclusiveMinimum": 0} if False else {"bsonType": ["double", "int", "long"], "minimum": 0.01},
        "vehicle": {"bsonType": "object", "required": ["brand", "model"],
                    "properties": {"current_value_eur": {"bsonType": ["double", "int", "long"], "minimum": 0.01},
                                   "power_hp": {"bsonType": ["int", "long", "double"], "minimum": 1}}}}}},
    validationLevel="strict", validationAction="error")
db.create_collection("claims", validator={"$jsonSchema": {
    "bsonType": "object", "required": ["_id", "claim_id", "contract_id", "claim_type", "claim_status"],
    "properties": {"claim_status": {"enum": CLAIM_STATUS},
                   "damage_amount_eur": {"bsonType": ["double", "int", "long"], "minimum": 0},
                   "indemnified_amount_eur": {"bsonType": ["double", "int", "long"], "minimum": 0}}}})

# ---- 2) indexes created immediately, matched to the query patterns used below
db.contracts.create_index([("contract_id", ASCENDING)], unique=True, name="uq_contract_id")
db.contracts.create_index([("product", ASCENDING), ("risk_zone", ASCENDING)], name="ix_product_risk")
db.contracts.create_index([("vehicle.fuel_type", ASCENDING)], sparse=True, name="ix_fuel_sparse")
db.claims.create_index([("claim_id", ASCENDING)], unique=True, name="uq_claim_id")
db.claims.create_index([("contract_id", ASCENDING)], name="ix_claim_contract")       # foreignField of $lookup
db.claims.create_index([("claim_status", ASCENDING), ("claim_type", ASCENDING)], name="ix_status_type")

# ---- 3) load
df = pd.read_csv("output/integrated_contracts.csv")
veh_cols = ["brand", "model", "vehicle_year", "fuel_type", "vehicle_color", "vehicle_usage", "previous_claims_count",
            "current_value_eur", "power_hp", "vehicle_age", "value_band"]
clm_cols = ["claim_id", "contract_id", "occurrence_date", "declaration_date", "claim_type", "claim_status", "expert_id",
            "liability", "damage_amount_eur", "indemnified_amount_eur", "days_to_declare"]
base_cols = ["contract_id", "client_id", "client_name", "product", "start_date", "end_date", "status", "risk_zone",
             "client_age", "channel", "csp", "gender", "annual_premium_eur", "city", "postal_code", "age_group"]


def clean(d):
    return {k: (v.item() if hasattr(v, "item") else v) for k, v in d.items() if pd.notna(v)}


contract_docs, claim_docs = [], []
for r in df.to_dict("records"):
    doc = clean({k: r[k] for k in base_cols}); doc["_id"] = doc["contract_id"]
    if r["has_vehicle"]:
        doc["vehicle"] = clean({k: r[k] for k in veh_cols})
    contract_docs.append(doc)
    if r["has_claim"]:
        cd = clean({k: r[k] for k in clm_cols}); cd["_id"] = cd["claim_id"]; claim_docs.append(cd)
try:
    db.contracts.insert_many(contract_docs, ordered=False); db.claims.insert_many(claim_docs, ordered=False)
except BulkWriteError as e:
    print("Rejected by validator:", len(e.details["writeErrors"]))
print("contracts:", db.contracts.count_documents({}), "| claims:", db.claims.count_documents({}))

# ---- 4) validator in action: an invalid document must be rejected
try:
    db.contracts.insert_one({"_id": "BAD", "contract_id": "BAD", "client_id": "x", "product": "Boat", "status": "Active", "annual_premium_eur": -5})
except WriteError as e:
    print("Validator rejected invalid document (expected):", e.code)

# ---- 5) queries (parameterised; $match first so the index can be used)
def avg_premium_by_product(min_premium=0):
    return list(db.contracts.aggregate([{"$match": {"annual_premium_eur": {"$gt": min_premium}}},
                                        {"$group": {"_id": "$product", "n": {"$sum": 1}, "avg_premium": {"$avg": "$annual_premium_eur"}}},
                                        {"$sort": {"avg_premium": -1}}]))


def claims_with_contract(product, exclude_status=("Rejected",)):
    """claims -> contracts via indexed $lookup; rejected claims excluded from cost; only the requested product."""
    return list(db.claims.aggregate([
        {"$match": {"claim_status": {"$nin": list(exclude_status)}}},
        {"$lookup": {"from": "contracts", "localField": "contract_id", "foreignField": "_id", "as": "c"}},
        {"$unwind": "$c"}, {"$match": {"c.product": product}},
        {"$group": {"_id": "$claim_type", "n": {"$sum": 1}, "avg_damage": {"$avg": "$damage_amount_eur"}}},
        {"$sort": {"n": -1}}]))


print("\nQ1 average premium by product");           [print(d) for d in avg_premium_by_product()]
print("\nQ2 non-rejected Auto claims by type");     [print(d) for d in claims_with_contract("Auto")]

# ---- 6) guarded update: only legal status transitions, never overwrite blindly
def advance_claim(claim_id, new_status):
    cur = db.claims.find_one({"_id": claim_id}, {"claim_status": 1})
    if cur is None:
        return "claim not found"
    if new_status not in ALLOWED_TRANSITIONS.get(cur["claim_status"], []):
        return f"blocked: {cur['claim_status']} -> {new_status} is not allowed"
    res = db.claims.update_one({"_id": claim_id, "claim_status": cur["claim_status"]},      # optimistic concurrency
                               {"$set": {"claim_status": new_status}})
    return f"updated ({res.modified_count})"

open_claim = db.claims.find_one({"claim_status": "Open"}, {"_id": 1})["_id"]
print("\nUpdate legal   :", advance_claim(open_claim, "In_progress"))
print("Update illegal :", advance_claim(open_claim, "Open"))

# ---- 7) duplicate detection (aggregation) + explain
dups = list(db.contracts.aggregate([{"$group": {"_id": "$client_id", "n": {"$sum": 1}}}, {"$match": {"n": {"$gt": 1}}}]))
print("\nDuplicate client_id groups:", len(dups))
plan = db.contracts.find({"product": "Auto", "risk_zone": "High"}).explain()["queryPlanner"]["winningPlan"]
print("Explain winning plan:", plan.get("inputStage", plan).get("stage"), "using index",
      plan.get("inputStage", {}).get("indexName"))
