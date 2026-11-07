-- BBIM707 A2 | Part A - run these in MySQL Workbench AFTER `python partA_etl_analysis.py` has loaded the database.
-- (The Python script creates the schema, loads the data and builds integrated_contracts. This file is for
--  inspecting and verifying the result in Workbench and for screenshots.)
USE insurance_dw;

-- 1) Structure: tables, columns, data types, keys
SHOW TABLES;
DESCRIBE dim_contract;
DESCRIBE dim_vehicle;
DESCRIBE fact_claim;
SHOW CREATE TABLE fact_claim;                 -- shows PRIMARY KEY, FOREIGN KEY and CHECK constraints
SHOW INDEX FROM fact_claim;

-- 2) Row counts (expected 15000 / 5390 / 155 / 15000)
SELECT 'dim_contract' AS tbl, COUNT(*) AS n FROM dim_contract
UNION ALL SELECT 'dim_vehicle', COUNT(*) FROM dim_vehicle
UNION ALL SELECT 'fact_claim', COUNT(*) FROM fact_claim
UNION ALL SELECT 'integrated_contracts', COUNT(*) FROM integrated_contracts;

-- 3) Integrity checks (all should return 0)
SELECT COUNT(*) AS orphan_claims FROM fact_claim f LEFT JOIN dim_contract c ON f.contract_id = c.contract_id WHERE c.contract_id IS NULL;
SELECT COUNT(*) AS orphan_vehicles FROM dim_vehicle v LEFT JOIN dim_contract c ON v.contract_id = c.contract_id WHERE c.contract_id IS NULL;
SELECT COUNT(*) - COUNT(DISTINCT contract_id) AS duplicate_contract_keys FROM integrated_contracts;

-- 4) Proof that the constraints work (each statement should FAIL with an error - take a screenshot of the error)
-- INSERT INTO dim_contract (contract_id, client_id, product, status, annual_premium_eur) VALUES ('CTR_999999','CLI_999999','Boat','Active',100);
-- INSERT INTO fact_claim (claim_id, contract_id, claim_type, claim_status) VALUES ('CLM_X','CTR_NOT_EXIST','Fire','Open');

-- 5) Integrated view: sample rows (Auto contract with vehicle and claim)
SELECT contract_id, product, annual_premium_eur, brand, model, current_value_eur, claim_id, claim_type, damage_amount_eur
FROM integrated_contracts WHERE has_vehicle = 1 AND has_claim = 1 LIMIT 10;

-- 6) Analysis queries that match Part A Task 2
SELECT product, COUNT(*) AS contracts, ROUND(100*COUNT(*)/(SELECT COUNT(*) FROM integrated_contracts),1) AS pct,
       ROUND(AVG(annual_premium_eur),1) AS avg_premium FROM integrated_contracts GROUP BY product ORDER BY contracts DESC;
SELECT status, COUNT(*) AS contracts FROM integrated_contracts GROUP BY status ORDER BY contracts DESC;
SELECT risk_zone, COUNT(*) AS contracts FROM integrated_contracts GROUP BY risk_zone ORDER BY contracts DESC;
SELECT claim_type, COUNT(*) AS claims, ROUND(AVG(damage_amount_eur),1) AS avg_damage, ROUND(AVG(indemnified_amount_eur),1) AS avg_indemnity
FROM fact_claim GROUP BY claim_type ORDER BY claims DESC;
