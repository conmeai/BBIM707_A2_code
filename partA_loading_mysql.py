"""
BBIM707 A2 | Part A - Task 1 III. LOADING into MySQL (uses ONLY mysql.connector + pandas; no SQLAlchemy needed)
Reads the cleaned CSVs produced by the transformation stage (output/*_clean.csv), creates the schema with
PK / FK / CHECK / NOT NULL constraints, loads the data, builds the unified table and runs integrity checks.

Run   : python partA_loading_mysql.py          (after partA_etl_analysis.py has created output/*_clean.csv)
Config: edit the CONFIG block below, or set environment variables MYSQL_HOST / MYSQL_USER / MYSQL_PASSWORD / MYSQL_PORT / MYSQL_DB
WARNING: the database named in DB_NAME is DROPPED and re-created on every run.
"""
import os
import mysql.connector
import pandas as pd

# ------------------------------------------------------------------ CONFIG
CONFIG = dict(host=os.environ.get("MYSQL_HOST", "localhost"), user=os.environ.get("MYSQL_USER", "root"),
              password=os.environ.get("MYSQL_PASSWORD", "YOUR_PASSWORD"), port=int(os.environ.get("MYSQL_PORT", "3306")))
DB_NAME = os.environ.get("MYSQL_DB", "insurance_dw")
OUT_DIR = "output"

DDL = [
    """CREATE TABLE dim_contract (
      contract_id VARCHAR(12) PRIMARY KEY, client_id VARCHAR(12) NOT NULL UNIQUE, client_name VARCHAR(80),
      product VARCHAR(10) NOT NULL, start_date DATE, end_date DATE, status VARCHAR(15) NOT NULL, risk_zone VARCHAR(10),
      client_age INT, channel VARCHAR(10), csp VARCHAR(20), gender VARCHAR(10), annual_premium_eur DECIMAL(10,2) NOT NULL,
      invalid_period TINYINT(1), age_imputed TINYINT(1), city VARCHAR(30), postal_code VARCHAR(10), contract_duration_days INT,
      start_year INT, age_group VARCHAR(10), premium_band VARCHAR(10),
      CONSTRAINT chk_product CHECK (product IN ('Auto','Home','Life','Health')),
      CONSTRAINT chk_age CHECK (client_age BETWEEN 18 AND 100), CONSTRAINT chk_premium CHECK (annual_premium_eur > 0)
    ) ENGINE=InnoDB""",
    """CREATE TABLE dim_vehicle (
      contract_id VARCHAR(12) PRIMARY KEY, brand VARCHAR(20) NOT NULL, model VARCHAR(20) NOT NULL, vehicle_year INT,
      fuel_type VARCHAR(12), vehicle_color VARCHAR(12), vehicle_usage VARCHAR(15), previous_claims_count INT,
      current_value_eur DECIMAL(10,2), power_hp INT, power_imputed TINYINT(1), year_imputed TINYINT(1),
      prev_claims_imputed TINYINT(1), vehicle_age INT, value_band VARCHAR(10),
      CONSTRAINT fk_vehicle_contract FOREIGN KEY (contract_id) REFERENCES dim_contract(contract_id),
      CONSTRAINT chk_prev CHECK (previous_claims_count >= 0), CONSTRAINT chk_value CHECK (current_value_eur > 0)
    ) ENGINE=InnoDB""",
    """CREATE TABLE fact_claim (
      claim_id VARCHAR(14) PRIMARY KEY, contract_id VARCHAR(12) NOT NULL, occurrence_date DATE, declaration_date DATE,
      claim_type VARCHAR(15) NOT NULL, claim_status VARCHAR(15) NOT NULL, expert_id VARCHAR(12), liability VARCHAR(15),
      damage_amount_eur DECIMAL(10,2), indemnified_amount_eur DECIMAL(10,2), date_inconsistent TINYINT(1),
      days_to_declare INT, indemnity_exceeds_damage TINYINT(1), indemnity_ratio DECIMAL(6,3),
      CONSTRAINT fk_claim_contract FOREIGN KEY (contract_id) REFERENCES dim_contract(contract_id),
      CONSTRAINT chk_damage CHECK (damage_amount_eur >= 0), CONSTRAINT chk_indem CHECK (indemnified_amount_eur >= 0)
    ) ENGINE=InnoDB""",
]
BOOL_COLS = ["invalid_period", "age_imputed", "power_imputed", "year_imputed", "prev_claims_imputed",
             "date_inconsistent", "indemnity_exceeds_damage"]


def read_clean(name):
    df = pd.read_csv(f"{OUT_DIR}/{name}_clean.csv")
    for col in BOOL_COLS:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().map({"true": 1, "false": 0})
    return df.astype(object).where(df.notna(), None)          # NaN -> None  (SQL NULL)


def insert(cur, table, df, batch=1000):
    cols = ", ".join(df.columns)
    sql = f"INSERT INTO {table} ({cols}) VALUES ({', '.join(['%s'] * len(df.columns))})"
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    for i in range(0, len(rows), batch):
        cur.executemany(sql, rows[i:i + batch])
    print(f"  loaded {len(rows):>6} rows into {table}")


def main():
    conn = mysql.connector.connect(**CONFIG)
    if conn.is_connected():
        print("Connected to MySQL successfully!")
    cur = conn.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
    cur.execute(f"CREATE DATABASE {DB_NAME} CHARACTER SET utf8mb4")
    cur.execute(f"USE {DB_NAME}")

    print("Creating schema (PK, FK, CHECK, NOT NULL)...")
    for stmt in DDL:
        cur.execute(stmt)

    print("Loading data (parents first, then children)...")
    contracts, vehicles, claims = read_clean("contracts"), read_clean("vehicles"), read_clean("claims")
    insert(cur, "dim_contract", contracts)
    insert(cur, "dim_vehicle", vehicles)
    insert(cur, "fact_claim", claims)
    conn.commit()

    print("Building the unified table integrated_contracts (LEFT JOINs keep all contracts)...")
    vcols = ", ".join(f"v.{c}" for c in vehicles.columns if c != "contract_id")
    kcols = ", ".join(f"k.{c}" for c in claims.columns if c != "contract_id")
    cur.execute(f"""CREATE TABLE integrated_contracts (PRIMARY KEY (contract_id)) ENGINE=InnoDB AS
        SELECT c.*, {vcols}, {kcols}, (v.contract_id IS NOT NULL) AS has_vehicle, (k.claim_id IS NOT NULL) AS has_claim
        FROM dim_contract c LEFT JOIN dim_vehicle v ON c.contract_id = v.contract_id
        LEFT JOIN fact_claim k ON c.contract_id = k.contract_id""")
    for ix in ["CREATE INDEX idx_contract_product ON dim_contract(product)",
               "CREATE INDEX idx_contract_risk ON dim_contract(risk_zone)",
               "CREATE INDEX idx_claim_contract ON fact_claim(contract_id)",
               "CREATE INDEX idx_claim_type ON fact_claim(claim_type)"]:
        cur.execute(ix)
    conn.commit()

    print("\nVERIFICATION")
    cur.execute("SHOW TABLES"); print("Tables:", [r[0] for r in cur.fetchall()])
    for t in ["dim_contract", "dim_vehicle", "fact_claim", "integrated_contracts"]:
        cur.execute(f"SELECT COUNT(*) FROM {t}"); print(f"  {t:22s} rows =", cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM fact_claim f LEFT JOIN dim_contract c ON f.contract_id = c.contract_id WHERE c.contract_id IS NULL")
    print("Orphan claims (FK check):", cur.fetchone()[0])
    cur.execute("""SELECT product, COUNT(claim_id) AS claims, ROUND(AVG(damage_amount_eur),2) AS avg_damage
                   FROM integrated_contracts GROUP BY product ORDER BY claims DESC""")
    print("\nClaims by product (verification query)")
    for r in cur.fetchall():
        print("  ", r)
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
