# End-to-End E-Commerce Data Pipeline & Analytics Warehouse

An enterprise-grade, production-ready data engineering pipeline that ingests, cleanses, transforms, and models raw transactional e-commerce data into an optimized star-schema PostgreSQL data warehouse. Orchestrated end-to-end with Apache Airflow, containerized with Docker, validated through strict automated data-quality and reconciliation gates, and visualized using Power BI executive dashboards.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Business Problem Statement](#business-problem-statement)
- [Architecture & Data Flow](#architecture--data-flow)
- [Technology Stack](#technology-stack)
- [Repository Structure](#repository-structure)
- [Dataset Specifications](#dataset-specifications)
- [Data Quality Handling & Validation Rules](#data-quality-handling--validation-rules)
- [Data Warehouse Design & Star Schema](#data-warehouse-design--star-schema)
- [Orchestration (Apache Airflow DAG)](#orchestration-apache-airflow-dag)
- [Warehouse Reconciliation & Verified Metrics](#warehouse-reconciliation--verified-metrics)
- [Power BI Reporting & Analytics](#power-bi-reporting--analytics)
- [DAX Measures Reference](#dax-measures-reference)
- [Reliability, Idempotency & Transactions](#reliability-idempotency--transactions)
- [Setup & Execution Guide](#setup--execution-guide)
- [Engineering Decisions & Trade-Offs](#engineering-decisions--trade-offs)
- [Future Enhancements](#future-enhancements)
- [Technical Interview Talking Points](#technical-interview-talking-points)
- [License](#license)

---

## Project Overview

Modern e-commerce enterprises process millions of transactions daily across distributed touchpoints. Without rigorous data engineering practices, analytical reports suffer from silent data corruption: duplicate records inflate customer counts, orphan foreign keys distort inventory and product metrics, and invalid order quantities compromise revenue figures. 

This project implements a complete, reliable, and idempotent batch data pipeline that extracts raw transactional CSV files, loads them into an isolated staging schema in PostgreSQL, applies deterministic cleaning and Kimball star-schema dimensional modeling, executes automated multi-stage validation gates, and delivers certified analytical views to Power BI. The entire stack is containerized with Docker Compose and orchestrated seamlessly via Apache Airflow.

---

## Business Problem Statement

E-commerce leadership and business analysts require certified, trustworthy metrics to evaluate:
1. **Sales Performance:** Total revenue, order volumes, average order value (AOV), and unit sales velocity.
2. **Customer Demographics:** Geographic distribution, customer acquisition trends, and spending patterns across Indian states and cities.
3. **Product & Category Trends:** Revenue concentration across catalog categories (Electronics, Clothing, Home & Kitchen) and average selling price (ASP).
4. **Data Reliability & Pipeline Health:** Operational transparency into data pipeline integrity, source-to-warehouse reconciliation, and tracking of dirty or malformed upstream source records.

---

## Architecture & Data Flow

```text
+-----------------------------------------------------------------------------------+
|                                 DATA GENERATION                                   |
|   Python Generator (src/generate_data.py) -> 4 Raw CSV Datasets with Seed=42     |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                            APACHE AIRFLOW ORCHESTRATION                           |
|                       DAG: ecommerce_etl (LocalExecutor)                          |
+-----------------------------------------+-----------------------------------------+
                                          |
    [1. validate_source]                  |  Check file existence, headers & size
                                          v
    [2. load_staging]                     |  Atomic TRUNCATE & COPY into staging schema
                                          v
    [3. validate_staging]                 |  Assert 7 intentional anomalies & 3 constraints
                                          v
    [4. transform_dimensions]             |  Deduplicate, impute NULLs, build dimensions
                                          v
    [5. load_fact_sales]                  |  Lookup surrogate keys, filter bad records, load
                                          v
    [6. validate_warehouse]               |  Enforce read-only reconciliation & referential integrity
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                        POSTGRESQL DATA WAREHOUSE (Docker)                         |
|   Staging Schema:     stg_customers, stg_products, stg_orders, stg_order_items    |
|   Analytics Schema:   dim_customer, dim_product, dim_date, fact_sales             |
|   Analytical Views:   vw_data_quality_issues, vw_warehouse_reconciliation_display |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                              POWER BI DASHBOARDS                                  |
|   Page 1: Executive Sales Dashboard                                               |
|   Page 2: Customer & Product Analysis                                             |
|   Page 3: Data Quality & Pipeline Monitoring                                      |
+-----------------------------------------------------------------------------------+
```

---

## Technology Stack

| Component | Technology | Version | Purpose |
| :--- | :--- | :--- | :--- |
| **Language** | Python | 3.10+ | Synthetic data generation & Airflow operator logic |
| **Database** | PostgreSQL | 16 | Relational data warehouse, staging, and analytics schemas |
| **Orchestration** | Apache Airflow | 2.9.2 | Workflow scheduling, task dependency management, retries |
| **Containerization** | Docker & Docker Compose | v2+ | Local service provisioning (PostgreSQL, Airflow scheduler/webserver) |
| **Driver / Library** | Psycopg2 | 2.9+ | PostgreSQL native transactional database adapter |
| **Business Intelligence** | Microsoft Power BI | Latest | Dimensional reporting, DAX measures, and data pipeline observability |

---

## Repository Structure

```text
ecommerce-data-engineering/
├── README.md                           # Comprehensive project documentation
├── docker-compose.yml                  # Docker Compose configuration (Postgres 16, Airflow 2.9.2)
├── requirements.txt                    # Project Python dependencies
├── airflow/
│   ├── dags/
│   │   ├── ecommerce_etl.py            # Primary production ETL DAG (6 tasks with validation gates)
│   │   └── test_dag.py                 # Smoke test DAG for Airflow environment verification
│   └── logs/                           # Airflow execution and task logs
├── dashboard/
│   └── screenshots/                    # Power BI dashboard captures and visuals
├── data/
│   ├── raw/                            # Ingested raw CSV datasets (read-only for Airflow)
│   │   ├── customers.csv               # 2,015 records (includes 15 duplicates)
│   │   ├── products.csv                # 100 records (includes 2 NULL prices)
│   │   ├── orders.csv                  # 10,000 records (includes 60 NULL payments, 50 orphans)
│   │   └── order_items.csv             # 21,968 records (includes 150 invalid qty, 120 orphans)
│   └── processed/                      # Staging and intermediate archive storage
├── sql/
│   ├── vw_data_quality_issues.sql      # View aggregating the 7 staging data-quality anomalies
│   ├── vw_warehouse_reconciliation.sql # View comparing valid staging records vs fact_sales
│   └── vw_warehouse_reconciliation_display.sql # View unpivoting reconciliation metrics for Power BI
├── src/
│   └── generate_data.py                # Deterministic synthetic data generator with controlled DQ anomalies
└── tests/                              # Pipeline test suite
```

---

## Dataset Specifications

The raw dataset simulates an Indian e-commerce platform operating across 30 major tier-1/tier-2 cities and 17 states from January 2022 to January 2026. Generated deterministically with NumPy/Pandas (`RANDOM_SEED = 42`):

| Dataset | File Path | Total Rows | Schema Columns | Data Quality Characteristics |
| :--- | :--- | :---: | :--- | :--- |
| **Customers** | `data/raw/customers.csv` | **2,015** | `customer_id`, `customer_name`, `city`, `state`, `country`, `signup_date` | 2,000 unique customers + **15 duplicate customer records**; **15 NULL city values**. |
| **Products** | `data/raw/products.csv` | **100** | `product_id`, `product_name`, `category`, `price` | Electronics (20), Clothing (18), Home & Kitchen (16), Books (16), Sports (15), Beauty (15); **2 NULL prices**. |
| **Orders** | `data/raw/orders.csv` | **10,000** | `order_id`, `customer_id`, `order_date`, `payment_method`, `order_status` | Status: Delivered (75%), Shipped (15%), Pending (7%), Cancelled (3%); **60 NULL payment methods**; **50 orphan customer IDs** (`customer_id = 99999`). |
| **Order Items** | `data/raw/order_items.csv` | **21,968** | `order_id`, `product_id`, `quantity`, `unit_price` | Line items per order (1–5 items); **150 invalid quantities** (100 zeros, 50 negatives); **120 orphan product IDs** (`product_id = 9999`). |

---

## Data Quality Handling & Validation Rules

To demonstrate resilient data engineering, 7 real-world data quality anomalies were intentionally introduced into the raw source data. The pipeline detects, quantifies, and isolates these anomalies without failing upstream ingestion, while preventing any corrupted records from entering the analytics star schema:

### Intentional Anomalies Detected & Managed

| Issue # | Check Name | Staging Detection Condition | Expected Anomaly Count | Warehouse Transformation Handling |
| :---: | :--- | :--- | :---: | :--- |
| **1** | **Duplicate Customer IDs** | `HAVING COUNT(*) > 1` on `stg_customers` | **15** | Deduplicated deterministically in `dim_customer` using `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date, customer_name)`. Exactly 2,000 unique records loaded. |
| **2** | **Missing Customer Cities** | `city IS NULL` on `stg_customers` | **15** | Imputed as `'Unknown'` via `COALESCE(city, 'Unknown')` during `dim_customer` transformation. |
| **3** | **Missing Product Prices** | `price IS NULL` on `stg_products` | **2** | Catalog entries preserved in `dim_product` to track inventory; order items retain historical transaction `unit_price`. |
| **4** | **Missing Payment Methods** | `payment_method IS NULL` on `stg_orders` | **60** | Operational orders preserved in staging; valid line items are loaded into `fact_sales`. |
| **5** | **Orphan Customer IDs** | Orders referencing non-existent `customer_id` | **50** | Quarantined/excluded from `fact_sales` via strict inner join against `analytics.dim_customer`. |
| **6** | **Orphan Product IDs** | Order items referencing non-existent `product_id` | **120** | Quarantined/excluded from `fact_sales` via strict inner join against `analytics.dim_product`. |
| **7** | **Invalid Quantities** | `quantity <= 0` (zeros and negatives) | **150** | Quarantined/excluded from `fact_sales` via filtering predicate `WHERE oi.quantity > 0`. |

### Strict Integrity Constraints Enforced (0 Violations Allowed)

1. **Orphan Order References:** Every order item must reference an existing order (`stg_order_items.order_id` in `stg_orders`). Expected violations: **0**.
2. **Temporal Consistency (Order Before Signup):** No order date can precede the customer's account creation date (`o.order_date >= c.signup_date`). Expected violations: **0**.
3. **Unit Price Consistency:** Active catalog products must match transaction unit price (`oi.unit_price = p.price`). Expected violations: **0**.

---

## Data Warehouse Design & Star Schema

The data warehouse employs Ralph Kimball's dimensional modeling technique implemented across two distinct PostgreSQL schemas:

```text
                        +---------------------------+
                        |   analytics.dim_date      |
                        +---------------------------+
                        | PK  date_key (INT)        | <----------+
                        |     full_date (DATE)      |            |
                        |     year (INT)            |            |
                        |     quarter (INT)         |            |
                        |     month (INT)           |            |
                        |     month_name (VARCHAR)  |            |
                        |     week (INT)            |            |
                        |     day (INT)             |            |
                        |     day_name (VARCHAR)    |            |
                        +---------------------------+            |
                                                                 |
+---------------------------+       +----------------------------+--+
|  analytics.dim_customer   |       |    analytics.fact_sales       |
+---------------------------+       +-------------------------------+
| PK  customer_key (INT)    | <---+ | PK  sales_key (BIGINT)        |
| UK  customer_id (INT)     |     | | FK  customer_key (INT)        |
|     customer_name (VARCHAR|     | | FK  product_key (INT)         |
|     city (VARCHAR)        |     | | FK  date_key (INT)            |
|     state (VARCHAR)       |     | |     order_id (INT)            |
|     country (VARCHAR)     |     | |     quantity (INT)            |
|     signup_date (DATE)    |     | |     unit_price (NUMERIC)      |
+---------------------------+     | |     sales_amount (NUMERIC)    |
                                  | +-------------------------------+
                                  |                  |
+---------------------------+     |                  |
|   analytics.dim_product   |     |                  |
+---------------------------+     |                  |
| PK  product_key (INT)     | <---+------------------+
| UK  product_id (INT)      |
|     product_name (VARCHAR)|
|     category (VARCHAR)    |
|     price (NUMERIC)       |
+---------------------------+
```

### Table Specifications

1. **`staging.stg_*` Tables:**
   - Unconstrained landing tables (`stg_customers`, `stg_products`, `stg_orders`, `stg_order_items`) mirroring raw CSV schemas.
   - Truncated and bulk-loaded using PostgreSQL streaming `COPY ... FROM STDIN`.

2. **`analytics.dim_customer` (2,000 rows):**
   - **Surrogate Key:** `customer_key` (INTEGER GENERATED ALWAYS AS IDENTITY / SERIAL PRIMARY KEY).
   - **Natural Key:** `customer_id` (INTEGER UNIQUE).
   - Contains cleaned and deduplicated customer records with imputed `'Unknown'` cities.

3. **`analytics.dim_product` (100 rows):**
   - **Surrogate Key:** `product_key` (INTEGER PRIMARY KEY).
   - **Natural Key:** `product_id` (INTEGER UNIQUE).
   - Tracks 100 products spanning 6 e-commerce retail categories.

4. **`analytics.dim_date` (1,465 rows):**
   - **Surrogate Key:** `date_key` (INTEGER formatted as `YYYYMMDD`, e.g., `20240515`).
   - Contiguous calendar generated using PostgreSQL `generate_series()` covering the exact range between minimum order date (`2022-01-27`) and maximum order date (`2026-01-30`).

5. **`analytics.fact_sales` (21,603 rows):**
   - **Primary Key:** `sales_key` (BIGSERIAL PRIMARY KEY).
   - **Foreign Keys:** `customer_key`, `product_key`, `date_key` referencing dimensional surrogate keys.
   - **Degenerate Dimension:** `order_id` (enables distinct order tracking without an extra fact table).
   - **Measures:** `quantity` (INT), `unit_price` (NUMERIC(12,2)), `sales_amount` (`(quantity * unit_price)::NUMERIC(14,2)`).

---

## Orchestration (Apache Airflow DAG)

The ETL lifecycle is orchestrated by the DAG `ecommerce_etl` defined in [airflow/dags/ecommerce_etl.py](file:///c:/Users/snehi/Documents/Codex/ecommerce-data-engineering/airflow/dags/ecommerce_etl.py). It runs 6 strictly sequential tasks connected linearly:

```python
validate_source >> load_staging >> validate_staging >> transform_dimensions >> load_fact_sales >> validate_warehouse
```

### Task Descriptions

1. **`validate_source`:**
   - Pre-flight checks on filesystem: confirms `data/raw/` directory and all 4 CSV files exist.
   - Asserts non-zero file sizes and validates expected header columns.
2. **`load_staging`:**
   - Single atomic transaction: executes `TRUNCATE TABLE` across all 4 staging tables.
   - Streams CSV data via `cur.copy_expert(COPY FROM STDIN)`.
   - Asserts exact post-load row counts: Customers=2,015, Products=100, Orders=10,000, Order Items=21,968 before committing.
3. **`validate_staging`:**
   - Verifies the 7 intentional anomaly counts match expectations (logged as WARNING).
   - Enforces 0 violations across foreign key and date integrity constraints (fails pipeline if violated).
4. **`transform_dimensions`:**
   - Rebuilds `dim_customer`, `dim_product`, and `dim_date` transactionally.
   - Applies deterministic `ROW_NUMBER()` deduplication and `COALESCE` imputations.
   - Restores foreign key constraints on `fact_sales`.
5. **`load_fact_sales`:**
   - Executes idempotent `TRUNCATE analytics.fact_sales RESTART IDENTITY`.
   - Inserts valid staging rows matching customer, product, and date dimensions where `quantity > 0`.
   - Reconciles source vs. fact metrics within the transaction.
6. **`validate_warehouse`:**
   - Read-only (`conn.set_session(readonly=True)`) final validation gate.
   - Performs comprehensive checks: calendar continuity, referential integrity (0 orphan keys), coverage checks, and source-to-warehouse reconciliation assertions.

---

## Warehouse Reconciliation & Verified Metrics

The pipeline guarantees complete auditability through automated source-to-fact reconciliation. 

### Filtering Audit Trail
- **Raw Staging Order Items:** `21,968` rows
- **Excluded Corrupt Rows:** `365` rows
  - `150` rows with invalid quantities (`quantity <= 0`)
  - `120` rows with orphan product IDs (`product_id = 9999`)
  - `50` rows belonging to orphan customer orders (`customer_id = 99999`)
  - `45` remaining unmatchable line items
- **Certified Fact Table Rows:** **`21,603` rows**

### Exact Reconciliation Results

Querying `analytics.vw_warehouse_reconciliation_display`:

| Metric | Valid Source Dataset | Fact Warehouse Table | Variance / Difference | Validation Status |
| :--- | :---: | :---: | :---: | :---: |
| **Rows** | `21,603` | `21,603` | **`0`** | **PASS** |
| **Total Quantity** | `38,017` | `38,017` | **`0`** | **PASS** |
| **Total Sales** | `₹106,439,645.25` | `₹106,439,645.25` | **`₹0.00`** | **PASS** |

---

## Power BI Reporting & Analytics

The warehouse feeds a 3-page executive reporting suite in Power BI:

### Page 1: Executive Sales Dashboard
- **Executive KPI Cards:** Total Sales (`₹106.44M`), Total Orders (`9,897`), Total Quantity Sold (`38,017`), Average Order Value (`₹10,754.74`).
- **Revenue by Category:** Donut chart breakdown across Electronics, Clothing, Home & Kitchen, Books, Sports, and Beauty.
- **Monthly Revenue Trend:** 4-year chronological timeline showing sales seasonality and quarter-over-quarter growth.
- **Geographic Sales Distribution:** Map and bar visual displaying top revenue-generating Indian states (Maharashtra, Karnataka, Delhi, Telangana, Tamil Nadu).

### Page 2: Customer & Product Analysis
- **Customer Segmentation:** Customer acquisition rate by month/year from `dim_date` and `dim_customer[signup_date]`.
- **Top 10 Products by Revenue:** Ranked bar chart highlighting top-grossing items (e.g., *Smartphone Pro Max*, *Ultra-Slim Laptop*).
- **Average Selling Price (ASP) by Category:** Analysis of item pricing efficiency and margin distribution.
- **City-Level Performance:** Table visual detailing order counts, customer counts, and revenue per metropolitan area.

### Page 3: Data Quality & Pipeline Monitoring
- **Data Quality Anomaly Monitor:** Bar visual powered by `analytics.vw_data_quality_issues` displaying counts for Duplicate Customers (15), Missing Cities (15), Missing Prices (2), Missing Payment Methods (60), Orphan Customers (50), Orphan Products (120), and Invalid Quantities (150).
- **Source-to-Warehouse Reconciliation Card/Matrix:** Live visual powered by `analytics.vw_warehouse_reconciliation_display` showing zero difference across Rows, Quantity, and Sales.
- **Pipeline Health Status Card:** Execution gate audit confirming 100% referential integrity and Airflow task completion.

---

## DAX Measures Reference

Calculated measures implemented in Power BI for certified reporting:

```dax
// 1. Total Sales Amount
Total Sales = 
SUM(fact_sales[sales_amount])

// 2. Total Units Sold
Total Quantity = 
SUM(fact_sales[quantity])

// 3. Total Distinct Orders
Total Orders = 
DISTINCTCOUNT(fact_sales[order_id])

// 4. Total Active Customers
Total Customers = 
DISTINCTCOUNT(fact_sales[customer_key])

// 5. Total Products Sold
Total Products = 
DISTINCTCOUNT(fact_sales[product_key])

// 6. Average Order Value (AOV)
Average Order Value = 
DIVIDE([Total Sales], [Total Orders], 0)

// 7. Average Selling Price (ASP)
Average Selling Price = 
DIVIDE([Total Sales], [Total Quantity], 0)
```

---

## Reliability, Idempotency & Transactions

1. **Transactional Atomicity (`ACID`):**
   - Each Airflow task wraps its database interactions inside an explicit PostgreSQL transaction block (`with conn.cursor(): ... conn.commit()`).
   - If an error or assertion failure occurs, `conn.rollback()` executes automatically, preventing partial writes or corrupted states.

2. **Idempotency & Safe Re-Runs:**
   - Any task or entire DAG can be executed repeatedly with identical inputs without duplicating records or generating inconsistent metrics.
   - Dimensions use deterministic surrogate key generation (`RESTART IDENTITY`) and order-by sequences.
   - `fact_sales` is truncated and repopulated cleanly.

3. **Read-Only Gatekeeping:**
   - `validate_warehouse` explicitly enforces `conn.set_session(readonly=True)`. The final verification gate is strictly incapable of modifying data.

4. **Staging Immutability:**
   - Dimensions and fact transformations capture staging row counts before and after processing. Tasks assert that staging tables remain immutable throughout downstream operations.

---

## Setup & Execution Guide

### Prerequisites
- Windows 10/11 with PowerShell
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (configured with WSL2 backend)
- Python 3.10+ (for synthetic data generation)
- [Power BI Desktop](https://powerbi.microsoft.com/desktop/) (for opening reports)

### 1. Clone & Navigate to Repository
```powershell
cd C:\Users\snehi\Documents\Codex\ecommerce-data-engineering
```

### 2. Generate Synthetic Datasets (Optional / Pre-generated)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install pandas numpy
python src/generate_data.py
```

### 3. Launch Docker Services
Start PostgreSQL 16 and Apache Airflow 2.9.2:
```powershell
docker compose up -d
```

Verify that all three containers are healthy:
```powershell
docker ps
```
*Expected output shows `ecommerce-postgres`, `ecommerce-airflow-webserver`, and `ecommerce-airflow-scheduler` in `(healthy)` state.*

### 4. Trigger Airflow Pipeline
1. Open your browser and navigate to: `http://localhost:8080`
2. Log in using default credentials:
   - **Username:** `admin`
   - **Password:** `admin`
3. Locate DAG `ecommerce_etl` and unpause/trigger the run.
4. Confirm all 6 tasks succeed with green status:
   `validate_source` ➔ `load_staging` ➔ `validate_staging` ➔ `transform_dimensions` ➔ `load_fact_sales` ➔ `validate_warehouse`

### 5. Create / Verify SQL Views
Apply the analytical views to the warehouse:
```powershell
Get-Content sql/vw_data_quality_issues.sql -Raw | docker exec -i ecommerce-postgres psql -U ecommerce_user -d ecommerce_dw
Get-Content sql/vw_warehouse_reconciliation.sql -Raw | docker exec -i ecommerce-postgres psql -U ecommerce_user -d ecommerce_dw
Get-Content sql/vw_warehouse_reconciliation_display.sql -Raw | docker exec -i ecommerce-postgres psql -U ecommerce_user -d ecommerce_dw
```

Verify view results:
```powershell
docker exec ecommerce-postgres psql -U ecommerce_user -d ecommerce_dw -c "SELECT * FROM analytics.vw_warehouse_reconciliation_display;"
```

---

## Engineering Decisions & Trade-Offs

1. **ELT (Extract, Load, Transform) vs ETL:**
   - *Decision:* Extracted raw CSV data directly into a PostgreSQL `staging` schema using fast binary `COPY`, then used PostgreSQL's SQL engine for transformations.
   - *Trade-Off:* Leverages the database engine's native indexing, transactions, and join performance rather than handling heavy data frames in Python application memory.

2. **Surrogate Keys vs Natural Keys:**
   - *Decision:* Implemented integer surrogate keys (`customer_key`, `product_key`, `date_key`, `sales_key`) instead of relying solely on operational business keys.
   - *Trade-Off:* Adds join complexity during fact table loading, but decouples warehouse storage from upstream source ID mutations, improves indexing efficiency, and prepares the model for Slowly Changing Dimensions (SCD).

3. **Data Quarantine vs Pipeline Failure:**
   - *Decision:* Intentionally segregated operational anomalies (quarantining invalid quantities and orphan references) while allowing the certified fact table to load successfully.
   - *Trade-Off:* Eliminates brittle pipeline halts caused by minor operational defects in source applications while maintaining strict zero-tolerance gates on data integrity.

4. **Pre-computed `sales_amount` in Fact Table:**
   - *Decision:* Computed `(quantity * unit_price)::NUMERIC(14,2)` during fact table loading instead of relying on Power BI calculated columns.
   - *Trade-Off:* Increases warehouse storage slightly, but significantly improves Power BI report query performance and guarantees cross-platform calculation consistency.

---

## Future Enhancements

The following roadmap items represent planned production enhancements:
- **Slowly Changing Dimensions (SCD Type 2):** Implement historical tracking on `dim_customer` for customer location and profile changes using `start_date`, `end_date`, and `is_current` flags.
- **Incremental Loading & CDC:** Transition from batch TRUNCATE loads to Change Data Capture (CDC) or incremental watermark loading on `stg_orders` using Debezium or Airflow incremental execution intervals.
- **Automated Dead-Letter Queue (DLQ):** Route excluded rows (e.g., orphan products, negative quantities) to an `analytics.quarantine_rejected_records` table with explicit error reason codes.
- **dbt (data build tool) Migration:** Port dimensional transformations and SQL validation assertions to dbt models with automated documentation and lineage generation.
- **CI/CD Pipeline:** Configure GitHub Actions for automated SQL linting (SQLFluff), Python code formatting (Black/Flake8), and Dockerized integration tests.

---

## Technical Interview Talking Points

- **Idempotent Data Pipelines:** How to architect batch ETL pipelines that can be safely restarted or re-run for any execution date without creating duplicate rows or conflicting keys.
- **Staging Schema Isolation:** Why raw operational data should never be loaded directly into reporting tables, and how staging isolation protects analytics consumers.
- **Automated Reconciliation Gates:** Implementing automated source-to-target reconciliation checks inside Airflow DAGs to verify row counts, quantities, and financial totals before reporting delivery.
- **Kimball Dimensional Modeling:** Designing star schemas, surrogate keys, date dimension tables, and degenerate dimensions optimized for BI visualization engines.
- **Production Data Quality Management:** Handling dirty real-world data patterns (deduplication with window functions, handling NULLs with fallback defaults, and isolating orphan foreign keys).

---

## License

This project is licensed under the terms of the [MIT License](https://opensource.org/licenses/MIT).
