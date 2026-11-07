"""
BBIM707 - Advanced Data Engineering | Assessment 2 | Part A (ETL + Analysis)
Dataset : Insurance Dataset for Data Engineering Practice (Kaggle)
Sources : contracts.csv, vehicles.csv, claims.csv
Run     : python partA_etl_analysis.py
Output  : ./output/  (clean CSVs, SQLite database, charts, log)
"""
import os
import re
import sqlite3
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DATA_DIR = "."          # folder holding the three raw CSV files
OUT_DIR = "output"
REFERENCE_YEAR = int(os.environ.get("REFERENCE_YEAR", 2026))   # used to derive vehicle age (override via env var)
os.makedirs(OUT_DIR, exist_ok=True)
pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 50)


def banner(txt):
    print("\n" + "=" * 78 + f"\n{txt}\n" + "=" * 78)


# =============================================================================
# 1. EXTRACTION
# =============================================================================
banner("STAGE 1 - EXTRACTION")
contracts = pd.read_csv(os.path.join(DATA_DIR, "contracts.csv"))
vehicles = pd.read_csv(os.path.join(DATA_DIR, "vehicles.csv"))
claims = pd.read_csv(os.path.join(DATA_DIR, "claims.csv"))

for name, df in [("contracts", contracts), ("vehicles", vehicles), ("claims", claims)]:
    print(f"\n[{name}] shape = {df.shape}")
    print(df.head(3).to_string(index=False))
    print("Missing values per column:")
    print(df.isna().sum()[df.isna().sum() > 0].to_string())
    print("Duplicate rows:", df.duplicated().sum(), "| duplicate keys:",
          df.iloc[:, 0].duplicated().sum())

print("\nReferential integrity check (raw data)")
print(" vehicles.contract_id  in contracts:", vehicles.contract_id.isin(contracts.contract_id).mean())
print(" claims.contract_id    in contracts:", claims.contract_id.isin(contracts.contract_id).mean())
print(" claims.contract_id    in vehicles :", round(claims.contract_id.isin(vehicles.contract_id).mean(), 3))

raw_counts = {"contracts": len(contracts), "vehicles": len(vehicles), "claims": len(claims)}

# =============================================================================
# 2. TRANSFORMATION - helper functions
# =============================================================================
banner("STAGE 2 - TRANSFORMATION")


def parse_money(series):
    """'1974.98EUR', '$620.93', '713.53 euros', '689' -> float (all treated as EUR)."""
    txt = series.astype(str).str.replace(r"[^0-9.\-]", "", regex=True)
    return pd.to_numeric(txt, errors="coerce").astype("float64")


def parse_mixed_date(series):
    """ISO (YYYY-MM-DD) | DD-MM-YYYY | DD/MM/YYYY, with MM/DD/YYYY as fallback
    when the day-first reading is impossible (e.g. 03/25/2023)."""
    s = series.astype(str).str.strip().where(series.notna())
    out = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y"):
        out = out.fillna(pd.to_datetime(s, format=fmt, errors="coerce"))
    return out


def power_to_hp(series):
    """Normalise engine power to horsepower. kW*1.34102 ; CV*0.98632 ; no unit -> HP."""
    s = series.astype(str).str.strip().str.lower().where(series.notna())
    num = pd.to_numeric(s.str.extract(r"(\d+(?:\.\d+)?)")[0], errors="coerce").astype("float64")
    unit = s.str.extract(r"([a-z]+)")[0]
    factor = np.where(unit == "kw", 1.34102, np.where(unit == "cv", 0.98632, 1.0))
    return (num * factor).round(0)


log = []   # data-quality log: (table, issue, rows affected, action)


def note(table, issue, n, action):
    log.append({"table": table, "issue": issue, "rows_affected": int(n), "action": action})


# ---------------------------------------------------------------- contracts
c = contracts.copy()
c.columns = [x.strip().lower() for x in c.columns]
c["client_name"] = c["client_name"].str.strip().str.title()

c["annual_premium_eur"] = parse_money(c["annual_premium"])
note("contracts", "annual_premium stored as text with mixed symbols (EUR, $, euros)",
     c["annual_premium"].str.contains(r"[^0-9.]", regex=True).sum(), "stripped symbols, cast to float, assumed EUR")

c["start_date"] = parse_mixed_date(c["start_date"])
c["end_date"] = parse_mixed_date(c["end_date"])
note("contracts", "dates in 3-4 formats (ISO, DD-MM-YYYY, DD/MM/YYYY, MM/DD/YYYY)",
     (~contracts["start_date"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$")).sum(),
     "parsed to datetime64; ambiguous slash dates read as day-first")

bad_period = (c["end_date"] <= c["start_date"])
note("contracts", "end_date not after start_date", bad_period.sum(), "flagged in invalid_period, duration set to NaN")
c["invalid_period"] = bad_period

gender_map = {"M": "Male", "MALE": "Male", "F": "Female", "FEMALE": "Female"}
note("contracts", "gender coded as M/F and Male/Female + missing", c["gender"].isna().sum() + c["gender"].isin(["M", "F"]).sum(),
     "standardised to Male/Female, missing -> Unknown")
c["gender"] = c["gender"].str.upper().map(gender_map).fillna("Unknown")

note("contracts", "csp (socio-professional category) missing", c["csp"].isna().sum(), "filled with 'Unknown'")
c["csp"] = c["csp"].fillna("Unknown")

note("contracts", "client_age missing", c["client_age"].isna().sum(),
     "imputed with median age per product, flagged in age_imputed")
c["age_imputed"] = c["client_age"].isna()
c["client_age"] = c["client_age"].fillna(c.groupby("product")["client_age"].transform("median")).astype(int)

c[["city", "postal_code"]] = c["city_postal"].str.split("_", expand=True)
c = c.drop(columns=["annual_premium", "city_postal"])

# derived fields
c["contract_duration_days"] = (c["end_date"] - c["start_date"]).dt.days.where(~c["invalid_period"])
c["start_year"] = c["start_date"].dt.year
c["age_group"] = pd.cut(c["client_age"], [17, 29, 39, 49, 59, 100],
                        labels=["18-29", "30-39", "40-49", "50-59", "60+"])
c["premium_band"] = pd.qcut(c["annual_premium_eur"], 3, labels=["Low", "Medium", "High"])

# ----------------------------------------------------------------- vehicles
v = vehicles.copy()
v["current_value_eur"] = parse_money(v["current_value"])
note("vehicles", "current_value stored as text with mixed symbols", len(v), "stripped symbols, cast to float, assumed EUR")

v["power_hp"] = power_to_hp(v["power"])
note("vehicles", "power in mixed units (HP/hp/CV/kW/no unit) + missing", len(v),
     "converted to HP (kW x1.341, CV x0.986, unitless = HP)")
note("vehicles", "power missing", v["power_hp"].isna().sum(), "imputed with median HP per model, flagged")
v["power_imputed"] = v["power_hp"].isna()
v["power_hp"] = v["power_hp"].fillna(v.groupby("model")["power_hp"].transform("median")).round(0).astype(int)

note("vehicles", "year missing", v["year"].isna().sum(), "imputed with median year per model, flagged")
v["year_imputed"] = v["year"].isna()
v["year"] = v["year"].fillna(v.groupby("model")["year"].transform("median")).round(0).astype(int)

note("vehicles", "previous_claims missing", v["previous_claims"].isna().sum(),
     "imputed with overall median, flagged")
v["prev_claims_imputed"] = v["previous_claims"].isna()
v["previous_claims"] = v["previous_claims"].fillna(v["previous_claims"].median()).astype(int)

note("vehicles", "color missing", v["color"].isna().sum(), "filled with 'Unknown'")
v["color"] = v["color"].fillna("Unknown")

v = v.drop(columns=["power", "current_value"])
v = v.rename(columns={"year": "vehicle_year", "usage": "vehicle_usage", "color": "vehicle_color",
                      "previous_claims": "previous_claims_count"})
v["vehicle_age"] = REFERENCE_YEAR - v["vehicle_year"]
v["value_band"] = pd.qcut(v["current_value_eur"], 3, labels=["Low", "Medium", "High"])

# ------------------------------------------------------------------- claims
k = claims.copy()
k["damage_amount_eur"] = parse_money(k["damage_amount"])
k["indemnified_amount_eur"] = parse_money(k["indemnified_amount"])
note("claims", "money columns stored as text with mixed symbols", len(k), "stripped symbols, cast to float")

k["occurrence_date"] = parse_mixed_date(k["occurrence_date"])
k["declaration_date"] = parse_mixed_date(k["declaration_date"])
note("claims", "dates in mixed formats", len(k), "parsed to datetime64")

bad_seq = k["declaration_date"] < k["occurrence_date"]
note("claims", "declaration_date earlier than occurrence_date (logical error)", bad_seq.sum(),
     "flagged in date_inconsistent, days_to_declare set to NaN")
k["date_inconsistent"] = bad_seq
k["days_to_declare"] = (k["declaration_date"] - k["occurrence_date"]).dt.days.where(~bad_seq)

over = k["indemnified_amount_eur"] > k["damage_amount_eur"]
note("claims", "indemnified_amount greater than damage_amount", over.sum(), "flagged in indemnity_exceeds_damage")
k["indemnity_exceeds_damage"] = over

note("claims", "indemnified_amount missing (open / in-progress claims)", k["indemnified_amount_eur"].isna().sum(),
     "kept as NULL - amount not yet determined, not imputed")
note("claims", "liability missing", k["liability"].isna().sum(), "filled with 'Unknown'")
note("claims", "expert_id missing", k["expert_id"].isna().sum(), "filled with 'Unassigned'")
k["liability"] = k["liability"].fillna("Unknown")
k["expert_id"] = k["expert_id"].fillna("Unassigned")
k = k.rename(columns={"status": "claim_status"})
k["indemnity_ratio"] = (k["indemnified_amount_eur"] / k["damage_amount_eur"]).round(3)
k = k.drop(columns=["damage_amount", "indemnified_amount"])

# validation of data types and keys after cleaning
banner("VALIDATION - data types and keys after cleaning")
for name, df in [("contracts", c), ("vehicles", v), ("claims", k)]:
    print(f"\n[{name}]"); print(df.dtypes.to_string())
assert c["contract_id"].is_unique and v["contract_id"].is_unique and k["claim_id"].is_unique
print("\nPrimary keys unique: OK")

dq = pd.DataFrame(log)
print("\nDATA QUALITY LOG"); print(dq.to_string(index=False))
dq.to_csv(f"{OUT_DIR}/data_quality_log.csv", index=False)

# =============================================================================
# 3. LOADING
# =============================================================================
banner("STAGE 3 - LOADING")
# one claim per contract in this dataset -> safe 1:1 join; left joins keep all 15,000 contracts
integrated = (c.merge(v, on="contract_id", how="left")
                .merge(k, on="contract_id", how="left"))
integrated["has_claim"] = integrated["claim_id"].notna()
integrated["has_vehicle"] = integrated["brand"].notna()
assert len(integrated) == len(c), "join duplicated rows!"
print("Integrated shape:", integrated.shape)
print("Row counts  raw -> clean:", raw_counts, "->", {"contracts": len(c), "vehicles": len(v), "claims": len(k)})

c.to_csv(f"{OUT_DIR}/contracts_clean.csv", index=False)
v.to_csv(f"{OUT_DIR}/vehicles_clean.csv", index=False)
k.to_csv(f"{OUT_DIR}/claims_clean.csv", index=False)
integrated.to_csv(f"{OUT_DIR}/integrated_contracts.csv", index=False)

DB_MODE = os.environ.get("DB_MODE", "mysql")                      # mysql (default) or sqlite


def load_sqlite():
    db_path = f"{OUT_DIR}/insurance_dw.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")

    # Explicit schema: data types, primary/foreign keys, NOT NULL and CHECK constraints (declared BEFORE loading)
    DDL = """
    CREATE TABLE dim_contract (
      contract_id TEXT PRIMARY KEY, client_id TEXT NOT NULL UNIQUE, client_name TEXT, product TEXT NOT NULL
        CHECK (product IN ('Auto','Home','Life','Health')),
      start_date TEXT, end_date TEXT, status TEXT NOT NULL, risk_zone TEXT, client_age INTEGER CHECK (client_age BETWEEN 18 AND 100),
      channel TEXT, csp TEXT, gender TEXT, annual_premium_eur REAL NOT NULL CHECK (annual_premium_eur > 0),
      invalid_period INTEGER, age_imputed INTEGER, city TEXT, postal_code TEXT, contract_duration_days REAL,
      start_year INTEGER, age_group TEXT, premium_band TEXT);
    CREATE TABLE dim_vehicle (
      contract_id TEXT PRIMARY KEY REFERENCES dim_contract(contract_id), brand TEXT NOT NULL, model TEXT NOT NULL,
      vehicle_year INTEGER, fuel_type TEXT, vehicle_color TEXT, vehicle_usage TEXT, previous_claims_count INTEGER CHECK (previous_claims_count >= 0),
      current_value_eur REAL CHECK (current_value_eur > 0), power_hp INTEGER, power_imputed INTEGER, year_imputed INTEGER,
      prev_claims_imputed INTEGER, vehicle_age INTEGER, value_band TEXT);
    CREATE TABLE fact_claim (
      claim_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL REFERENCES dim_contract(contract_id), occurrence_date TEXT,
      declaration_date TEXT, claim_type TEXT NOT NULL, claim_status TEXT NOT NULL, expert_id TEXT, liability TEXT,
      damage_amount_eur REAL CHECK (damage_amount_eur >= 0), indemnified_amount_eur REAL CHECK (indemnified_amount_eur >= 0),
      date_inconsistent INTEGER, days_to_declare REAL, indemnity_exceeds_damage INTEGER, indemnity_ratio REAL);
    """
    con.executescript(DDL)
    prep = lambda df: df.assign(**{x: df[x].dt.strftime("%Y-%m-%d") for x in df.select_dtypes("datetime64").columns}
                                ).assign(**{x: df[x].astype(str) for x in df.select_dtypes("category").columns})
    for name, df in [("dim_contract", c), ("dim_vehicle", v), ("fact_claim", k)]:
        out = prep(df)
        out = out.replace({np.nan: None}).astype({x: "object" for x in out.columns})
        out.to_sql(name, con, index=False, if_exists="append")      # append keeps the declared constraints
    integ = prep(integrated)
    integ.to_sql("integrated_contracts", con, index=False, if_exists="replace")
    for ddl in ["CREATE INDEX idx_contract_product ON dim_contract(product)",
                "CREATE INDEX idx_contract_risk ON dim_contract(risk_zone)",
                "CREATE INDEX idx_claim_contract ON fact_claim(contract_id)",
                "CREATE INDEX idx_claim_type ON fact_claim(claim_type)",
                "CREATE INDEX idx_integ_contract ON integrated_contracts(contract_id)"]:
        con.execute(ddl)
    con.commit()
    print("Foreign key violations:", con.execute("PRAGMA foreign_key_check").fetchall() or "none")
    print("Integrity check      :", con.execute("PRAGMA integrity_check").fetchone()[0])
    print("\nTables in SQLite:")
    print(pd.read_sql("SELECT name, type FROM sqlite_master WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%'", con).to_string(index=False))
    for t in ["dim_contract", "dim_vehicle", "fact_claim", "integrated_contracts"]:
        print(f"{t:22s} rows =", con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])
    print("\nSample verification query (claims by product):")
    print(pd.read_sql("""SELECT product, COUNT(claim_id) AS claims, ROUND(AVG(damage_amount_eur),2) AS avg_damage
                         FROM integrated_contracts GROUP BY product ORDER BY claims DESC""", con).to_string(index=False))
    con.close()



def load_mysql():
    """MySQL loading lives in partA_loading_mysql.py (mysql.connector only, no SQLAlchemy)."""
    import partA_loading_mysql
    partA_loading_mysql.main()


(load_mysql if DB_MODE == "mysql" else load_sqlite)()


# =============================================================================
# 4. ANALYSIS (Part A - Task 2)
# =============================================================================
banner("TASK 2 - ANALYSIS: summary statistics")
num_cols = ["client_age", "annual_premium_eur", "contract_duration_days", "current_value_eur",
            "power_hp", "vehicle_age", "damage_amount_eur", "indemnified_amount_eur"]
summary = integrated[num_cols].describe().T
summary["median"] = integrated[num_cols].median()
print(summary[["count", "mean", "median", "std", "min", "max"]].round(2).to_string())
summary.round(2).to_csv(f"{OUT_DIR}/summary_statistics.csv")

banner("TASK 2 - ANALYSIS: value counts for three key columns")
for col in ["product", "status", "risk_zone"]:
    vc = integrated[col].value_counts()
    vc = pd.DataFrame({"count": vc, "percent": (vc / vc.sum() * 100).round(1)})
    print(f"\n{col}\n{vc.to_string()}")

banner("TASK 2 - ANALYSIS: supporting aggregations")
prem_by_prod = integrated.groupby("product")["annual_premium_eur"].agg(["count", "mean", "median"]).round(1)
print("Premium by product\n", prem_by_prod.to_string())
claim_rate = integrated.groupby("product")["has_claim"].agg(["sum", "mean"]).rename(columns={"sum": "claims", "mean": "claim_rate"})
print("\nClaims by product\n", claim_rate.round(4).to_string())
by_type = (integrated[integrated.has_claim].groupby("claim_type")
           .agg(n=("claim_id", "count"), avg_damage=("damage_amount_eur", "mean"),
                avg_indemnity=("indemnified_amount_eur", "mean")).round(1).sort_values("n", ascending=False))
print("\nClaims by type\n", by_type.to_string())
status_zone = pd.crosstab(integrated["risk_zone"], integrated["status"], normalize="index").round(3) * 100
print("\nContract status by risk_zone (% of row)\n", status_zone.to_string())
rej = integrated[integrated.has_claim].claim_status.value_counts()
print("\nClaim status\n", rej.to_string())
print("\nCorrelation (premium vs vehicle value, power, age) - Auto only")
auto = integrated[integrated["product"] == "Auto"]
print(auto[["annual_premium_eur", "current_value_eur", "power_hp", "vehicle_age", "client_age"]].corr().round(3).to_string())

# ---- charts
plt.rcParams.update({"font.family": "serif", "font.size": 11})
fig, ax = plt.subplots(figsize=(7, 4.2))
order = prem_by_prod["mean"].sort_values().index
ax.bar(order, prem_by_prod.loc[order, "mean"], color="#2F5597")
for i, p in enumerate(order):
    ax.text(i, prem_by_prod.loc[p, "mean"] + 20, f"{prem_by_prod.loc[p, 'mean']:.0f}", ha="center")
ax.set_title("Figure A: Average annual premium by product"); ax.set_ylabel("Premium (EUR)"); ax.set_xlabel("Product")
plt.tight_layout(); plt.savefig(f"{OUT_DIR}/chart1_premium_by_product.png", dpi=200); plt.close()

fig, ax = plt.subplots(figsize=(7, 4.2))
bt = by_type.sort_values("n")
ax.barh(bt.index, bt["n"], color="#C55A11")
for i, (idx, r) in enumerate(bt.iterrows()):
    ax.text(r["n"] + 0.4, i, f"{int(r['n'])}  (avg {r['avg_damage']:.0f} EUR)", va="center", fontsize=9)
ax.set_xlim(0, bt["n"].max() * 1.45)
ax.set_title("Figure B: Number of claims and average damage by claim type"); ax.set_xlabel("Number of claims")
plt.tight_layout(); plt.savefig(f"{OUT_DIR}/chart2_claims_by_type.png", dpi=200); plt.close()

fig, ax = plt.subplots(figsize=(7, 4.2))
ax.hist(auto["annual_premium_eur"], bins=30, color="#548235", edgecolor="white")
ax.axvline(auto["annual_premium_eur"].median(), color="black", ls="--", label=f"Median {auto['annual_premium_eur'].median():.0f}")
ax.set_title("Figure C: Distribution of Auto annual premium"); ax.set_xlabel("Premium (EUR)"); ax.set_ylabel("Contracts"); ax.legend()
plt.tight_layout(); plt.savefig(f"{OUT_DIR}/chart3_auto_premium_hist.png", dpi=200); plt.close()

print("\nDone. Files written to ./output/")
