from datetime import datetime, timedelta
import csv
from decimal import Decimal
import logging
import os
from pathlib import Path
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

# Default task arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Source data configuration
RAW_DATA_DIR = Path("/opt/airflow/data/raw")

EXPECTED_FILES = {
    "customers.csv": [
        "customer_id",
        "customer_name",
        "city",
        "state",
        "country",
        "signup_date",
    ],
    "products.csv": [
        "product_id",
        "product_name",
        "category",
        "price",
    ],
    "orders.csv": [
        "order_id",
        "customer_id",
        "order_date",
        "payment_method",
        "order_status",
    ],
    "order_items.csv": [
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    ],
}

# PostgreSQL configuration
PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
PG_DB = os.environ.get("POSTGRES_DB", "ecommerce_dw")
PG_USER = os.environ.get("POSTGRES_USER", "ecommerce_user")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "ecommerce_password")

EXPECTED_ROW_COUNTS = {
    "staging.stg_customers": 2015,
    "staging.stg_products": 100,
    "staging.stg_orders": 10000,
    "staging.stg_order_items": 21968,
}


# -----------------------------------------------------------------------------
# TASK 1: validate_source
# -----------------------------------------------------------------------------
def validate_source() -> None:
    """
    Validate that raw CSV datasets exist, are non-empty, readable, and have
    the required header columns.
    """
    logging.info("=== Starting Task 1: validate_source ===")
    logging.info("Checking raw data directory: %s", RAW_DATA_DIR)

    if not RAW_DATA_DIR.exists():
        raise FileNotFoundError(f"Raw data directory does not exist: {RAW_DATA_DIR}")

    for filename, expected_cols in EXPECTED_FILES.items():
        file_path = RAW_DATA_DIR / filename
        logging.info("Validating file: %s", file_path)

        # 1. Existence check
        if not file_path.exists():
            raise FileNotFoundError(f"Required source file missing: {file_path}")

        # 2. Non-empty check
        file_size = file_path.stat().st_size
        if file_size == 0:
            raise ValueError(f"Source file is empty (0 bytes): {file_path}")
        logging.info("  -> File size: %s bytes (OK)", file_size)

        # 3. Readability & Header check
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None)
        except Exception as exc:
            raise IOError(f"Failed to read file {file_path}: {exc}") from exc

        if not header:
            raise ValueError(f"No header row found in {file_path}")

        # Clean any whitespace around column names
        actual_cols = [col.strip() for col in header]
        logging.info("  -> Found columns: %s", actual_cols)

        # 4. Expected columns check
        missing_cols = [col for col in expected_cols if col not in actual_cols]
        if missing_cols:
            raise ValueError(
                f"File {filename} is missing required columns: {missing_cols}. "
                f"Expected: {expected_cols}, Found: {actual_cols}"
            )

        logging.info("  -> Header column validation passed for %s", filename)

    logging.info("=== Task 1: validate_source completed successfully ===")


# -----------------------------------------------------------------------------
# TASK 2: load_staging
# -----------------------------------------------------------------------------
def load_staging() -> None:
    """
    Idempotently load raw CSV datasets into PostgreSQL staging tables.
    Uses a single transaction to TRUNCATE staging tables, stream COPY data,
    and verify exact row counts before committing.
    """
    logging.info("=== Starting Task 2: load_staging ===")
    logging.info("Connecting to PostgreSQL at %s:%s / %s", PG_HOST, PG_PORT, PG_DB)

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    staging_loads = [
        ("staging.stg_customers", RAW_DATA_DIR / "customers.csv"),
        ("staging.stg_products", RAW_DATA_DIR / "products.csv"),
        ("staging.stg_orders", RAW_DATA_DIR / "orders.csv"),
        ("staging.stg_order_items", RAW_DATA_DIR / "order_items.csv"),
    ]

    try:
        with conn.cursor() as cur:
            # 1. Truncate staging tables inside transaction
            logging.info("Truncating staging tables...")
            cur.execute(
                "TRUNCATE TABLE staging.stg_customers, staging.stg_products, "
                "staging.stg_orders, staging.stg_order_items;"
            )
            logging.info("  -> Staging tables truncated successfully.")

            # 2. Bulk load each CSV using COPY FROM STDIN
            for table_name, csv_path in staging_loads:
                logging.info("Loading %s into %s ...", csv_path.name, table_name)
                copy_sql = f"COPY {table_name} FROM STDIN WITH (FORMAT csv, HEADER true);"
                with open(csv_path, "r", encoding="utf-8") as f:
                    cur.copy_expert(sql=copy_sql, file=f)
                logging.info("  -> %s loaded into %s", csv_path.name, table_name)

            # 3. Verify row counts before committing
            logging.info("Verifying loaded row counts...")
            for table_name, expected_count in EXPECTED_ROW_COUNTS.items():
                cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                actual_count = cur.fetchone()[0]
                logging.info("  -> %s: actual=%d, expected=%d", table_name, actual_count, expected_count)
                if actual_count != expected_count:
                    raise ValueError(
                        f"Row count mismatch for {table_name}: "
                        f"expected {expected_count}, got {actual_count}"
                    )

        # 4. Commit transaction
        conn.commit()
        logging.info("Transaction committed successfully. All staging tables loaded and verified.")

    except Exception as exc:
        logging.error("Error during load_staging, rolling back transaction: %s", exc)
        conn.rollback()
        raise
    finally:
        conn.close()
        logging.info("PostgreSQL connection closed.")

    logging.info("=== Task 2: load_staging completed successfully ===")


# -----------------------------------------------------------------------------
# TASK 3: validate_staging
# -----------------------------------------------------------------------------
def validate_staging() -> None:
    """
    Perform read-only PostgreSQL data-quality validation on staging tables:
    1. Assert 7 intentional anomalies match expected counts (logged as WARNING).
    2. Assert 3 integrity constraints have 0 violations (logged as PASS, fails otherwise).
    3. Assert exact row counts for all 4 staging tables.
    """
    logging.info("=== Starting Task 3: validate_staging ===")
    logging.info("Connecting to PostgreSQL at %s:%s / %s", PG_HOST, PG_PORT, PG_DB)

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    # 1. Intentional anomalies configuration
    intentional_anomalies = [
        {
            "name": "Duplicate customer IDs",
            "query": """
                SELECT COUNT(*) FROM (
                    SELECT customer_id
                    FROM staging.stg_customers
                    GROUP BY customer_id
                    HAVING COUNT(*) > 1
                ) t;
            """,
            "expected": 15,
        },
        {
            "name": "Missing customer cities",
            "query": "SELECT COUNT(*) FROM staging.stg_customers WHERE city IS NULL;",
            "expected": 15,
        },
        {
            "name": "Missing product prices",
            "query": "SELECT COUNT(*) FROM staging.stg_products WHERE price IS NULL;",
            "expected": 2,
        },
        {
            "name": "Missing payment methods",
            "query": "SELECT COUNT(*) FROM staging.stg_orders WHERE payment_method IS NULL;",
            "expected": 60,
        },
        {
            "name": "Orphan customer IDs",
            "query": """
                SELECT COUNT(*)
                FROM staging.stg_orders o
                LEFT JOIN staging.stg_customers c ON o.customer_id = c.customer_id
                WHERE c.customer_id IS NULL;
            """,
            "expected": 50,
        },
        {
            "name": "Orphan product IDs",
            "query": """
                SELECT COUNT(*)
                FROM staging.stg_order_items oi
                LEFT JOIN staging.stg_products p ON oi.product_id = p.product_id
                WHERE p.product_id IS NULL;
            """,
            "expected": 120,
        },
        {
            "name": "Invalid quantities",
            "query": "SELECT COUNT(*) FROM staging.stg_order_items WHERE quantity <= 0;",
            "expected": 150,
        },
    ]

    # 2. Strict integrity checks (violations must be 0)
    integrity_checks = [
        {
            "name": "Orphan order references",
            "query": """
                SELECT COUNT(*)
                FROM staging.stg_order_items oi
                LEFT JOIN staging.stg_orders o ON oi.order_id = o.order_id
                WHERE o.order_id IS NULL;
            """,
            "expected": 0,
        },
        {
            "name": "Order-before-signup violations",
            "query": """
                SELECT COUNT(*)
                FROM staging.stg_orders o
                JOIN staging.stg_customers c ON o.customer_id = c.customer_id
                WHERE o.order_date < c.signup_date;
            """,
            "expected": 0,
        },
        {
            "name": "Unit-price mismatches",
            "query": """
                SELECT COUNT(*)
                FROM staging.stg_order_items oi
                JOIN staging.stg_products p ON oi.product_id = p.product_id
                WHERE p.price IS NOT NULL AND oi.unit_price != p.price;
            """,
            "expected": 0,
        },
    ]

    # 3. Row count checks
    row_count_checks = [
        ("stg_customers", "SELECT COUNT(*) FROM staging.stg_customers;", 2015),
        ("stg_products", "SELECT COUNT(*) FROM staging.stg_products;", 100),
        ("stg_orders", "SELECT COUNT(*) FROM staging.stg_orders;", 10000),
        ("stg_order_items", "SELECT COUNT(*) FROM staging.stg_order_items;", 21968),
    ]

    errors = []

    try:
        with conn.cursor() as cur:
            logging.info("=" * 50)
            logging.info("STAGING DATA QUALITY VALIDATION")
            logging.info("=" * 50)

            # Check Intentional Anomalies
            logging.info("INTENTIONAL ANOMALIES")
            for item in intentional_anomalies:
                cur.execute(item["query"])
                actual = cur.fetchone()[0]
                expected = item["expected"]
                if actual == expected:
                    logging.warning(
                        "[WARN] %s: %d / expected %d", item["name"], actual, expected
                    )
                else:
                    msg = f"[ERROR] {item['name']}: actual={actual} != expected={expected}"
                    logging.error(msg)
                    errors.append(msg)

            # Check Strict Integrity Constraints
            logging.info("INTEGRITY CHECKS")
            for item in integrity_checks:
                cur.execute(item["query"])
                actual = cur.fetchone()[0]
                expected = item["expected"]
                if actual == expected:
                    logging.info("[PASS] %s: %d", item["name"], actual)
                else:
                    msg = f"[FAIL] {item['name']}: actual={actual} (expected {expected})"
                    logging.error(msg)
                    errors.append(msg)

            # Check Staging Row Counts
            logging.info("ROW COUNTS")
            for table_alias, query, expected in row_count_checks:
                cur.execute(query)
                actual = cur.fetchone()[0]
                if actual == expected:
                    logging.info("[PASS] %s: %d", table_alias, actual)
                else:
                    msg = f"[FAIL] {table_alias} count mismatch: actual={actual} != expected={expected}"
                    logging.error(msg)
                    errors.append(msg)

            logging.info("=" * 50)

            if errors:
                logging.error("VALIDATION FAILED with %d error(s):", len(errors))
                for err in errors:
                    logging.error("  -> %s", err)
                raise ValueError(f"Staging validation failed with errors: {'; '.join(errors)}")

            logging.info("VALIDATION PASSED")
            logging.info("=" * 50)

    finally:
        conn.close()
        logging.info("PostgreSQL connection closed.")

    logging.info("=== Task 3: validate_staging completed successfully ===")


# -----------------------------------------------------------------------------
# PLACEHOLDER TASKS (To be implemented in subsequent phases)
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# TASK 4: transform_dimensions
# -----------------------------------------------------------------------------
def transform_dimensions() -> None:
    """
    Transform validated staging data into analytics dimensions:
    1. dim_customer: Deduplicate by customer_id (ROW_NUMBER), replace NULL city with 'Unknown'.
    2. dim_product: Load all 100 products, preserving NULL prices.
    3. dim_date: Generate contiguous calendar dates between MIN and MAX order_date.

    Fully transactional and idempotent. Staging tables and fact_sales are protected.
    """
    logging.info("=== Starting Task 4: transform_dimensions ===")
    logging.info("Connecting to PostgreSQL at %s:%s / %s", PG_HOST, PG_PORT, PG_DB)

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    try:
        with conn.cursor() as cur:
            # 1. Capture pre-transformation counts for staging and fact_sales
            cur.execute("SELECT COUNT(*) FROM staging.stg_customers;")
            pre_cust = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_products;")
            pre_prod = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_orders;")
            pre_ord = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_order_items;")
            pre_items = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM analytics.fact_sales;")
            pre_fact = cur.fetchone()[0]

            # 2. Safely manage foreign key constraints on fact_sales during dimension rebuild
            cur.execute("""
                ALTER TABLE analytics.fact_sales DROP CONSTRAINT IF EXISTS fk_fact_customer;
                ALTER TABLE analytics.fact_sales DROP CONSTRAINT IF EXISTS fk_fact_product;
                ALTER TABLE analytics.fact_sales DROP CONSTRAINT IF EXISTS fk_fact_date;
            """)

            # 3. Truncate dimensions
            cur.execute("TRUNCATE analytics.dim_customer RESTART IDENTITY;")
            cur.execute("TRUNCATE analytics.dim_product RESTART IDENTITY;")
            cur.execute("TRUNCATE analytics.dim_date;")

            # 4. Transform & load dim_customer
            cur.execute("""
                INSERT INTO analytics.dim_customer (
                    customer_id, customer_name, city, state, country, signup_date
                )
                SELECT 
                    customer_id,
                    customer_name,
                    COALESCE(city, 'Unknown') AS city,
                    state,
                    country,
                    signup_date
                FROM (
                    SELECT 
                        customer_id,
                        customer_name,
                        city,
                        state,
                        country,
                        signup_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY customer_id
                            ORDER BY signup_date, customer_name, city, state, country
                        ) AS rn
                    FROM staging.stg_customers
                ) deduplicated
                WHERE rn = 1
                ORDER BY customer_id;
            """)

            # 5. Transform & load dim_product
            cur.execute("""
                INSERT INTO analytics.dim_product (
                    product_id, product_name, category, price
                )
                SELECT 
                    product_id,
                    product_name,
                    category,
                    price
                FROM staging.stg_products
                ORDER BY product_id;
            """)

            # 6. Transform & load dim_date
            cur.execute("""
                WITH date_bounds AS (
                    SELECT MIN(order_date) AS start_date, MAX(order_date) AS end_date
                    FROM staging.stg_orders
                ),
                calendar_series AS (
                    SELECT generate_series(start_date::timestamp, end_date::timestamp, '1 day'::interval)::date AS d
                    FROM date_bounds
                )
                INSERT INTO analytics.dim_date (
                    date_key,
                    full_date,
                    year,
                    quarter,
                    month,
                    month_name,
                    week,
                    day,
                    day_name
                )
                SELECT 
                    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
                    d AS full_date,
                    EXTRACT(YEAR FROM d)::INTEGER AS year,
                    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
                    EXTRACT(MONTH FROM d)::INTEGER AS month,
                    TO_CHAR(d, 'FMMonth') AS month_name,
                    EXTRACT(WEEK FROM d)::INTEGER AS week,
                    EXTRACT(DAY FROM d)::INTEGER AS day,
                    TO_CHAR(d, 'FMDay') AS day_name
                FROM calendar_series
                ORDER BY d;
            """)

            # 7. Re-add foreign key constraints to fact_sales
            cur.execute("""
                ALTER TABLE analytics.fact_sales
                ADD CONSTRAINT fk_fact_customer FOREIGN KEY (customer_key) REFERENCES analytics.dim_customer(customer_key);
                ALTER TABLE analytics.fact_sales
                ADD CONSTRAINT fk_fact_product FOREIGN KEY (product_key) REFERENCES analytics.dim_product(product_key);
                ALTER TABLE analytics.fact_sales
                ADD CONSTRAINT fk_fact_date FOREIGN KEY (date_key) REFERENCES analytics.dim_date(date_key);
            """)

            # 8. Validations
            errors = []

            # Customer validations
            cur.execute("SELECT COUNT(*) FROM analytics.dim_customer;")
            dim_cust_rows = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM analytics.dim_customer WHERE city = 'Unknown';")
            dim_cust_unknown = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT customer_id FROM analytics.dim_customer GROUP BY customer_id HAVING COUNT(*) > 1
                ) t;
            """)
            dim_cust_dups = cur.fetchone()[0]

            # Product validations
            cur.execute("SELECT COUNT(*) FROM analytics.dim_product;")
            dim_prod_rows = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM analytics.dim_product WHERE price IS NULL;")
            dim_prod_null_price = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT product_id FROM analytics.dim_product GROUP BY product_id HAVING COUNT(*) > 1
                ) t;
            """)
            dim_prod_dups = cur.fetchone()[0]

            # Date validations
            cur.execute("SELECT COUNT(*) FROM analytics.dim_date;")
            dim_date_rows = cur.fetchone()[0]
            cur.execute("SELECT MIN(full_date)::TEXT, MAX(full_date)::TEXT FROM analytics.dim_date;")
            min_date, max_date = cur.fetchone()
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT full_date FROM analytics.dim_date GROUP BY full_date HAVING COUNT(*) > 1
                ) t;
            """)
            dim_date_dups = cur.fetchone()[0]

            # Post-check staging counts
            cur.execute("SELECT COUNT(*) FROM staging.stg_customers;")
            post_cust = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_products;")
            post_prod = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_orders;")
            post_ord = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_order_items;")
            post_items = cur.fetchone()[0]

            # Post-check fact table
            cur.execute("SELECT COUNT(*) FROM analytics.fact_sales;")
            post_fact = cur.fetchone()[0]

            logging.info("=" * 50)
            logging.info("DIMENSION TRANSFORMATION")
            logging.info("=" * 50)

            # Log Customer Dimension
            logging.info("CUSTOMER DIMENSION")
            if dim_cust_rows == 2000:
                logging.info("[PASS] dim_customer rows: %d", dim_cust_rows)
            else:
                msg = f"[ERROR] dim_customer rows: {dim_cust_rows} (expected 2000)"
                logging.error(msg)
                errors.append(msg)

            if dim_cust_unknown == 15:
                logging.info("[PASS] Unknown cities: %d", dim_cust_unknown)
            else:
                msg = f"[ERROR] Unknown cities: {dim_cust_unknown} (expected 15)"
                logging.error(msg)
                errors.append(msg)

            if dim_cust_dups == 0:
                logging.info("[PASS] Duplicate customer IDs: %d", dim_cust_dups)
            else:
                msg = f"[ERROR] Duplicate customer IDs: {dim_cust_dups} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            # Log Product Dimension
            logging.info("PRODUCT DIMENSION")
            if dim_prod_rows == 100:
                logging.info("[PASS] dim_product rows: %d", dim_prod_rows)
            else:
                msg = f"[ERROR] dim_product rows: {dim_prod_rows} (expected 100)"
                logging.error(msg)
                errors.append(msg)

            if dim_prod_null_price == 2:
                logging.info("[PASS] NULL prices: %d", dim_prod_null_price)
            else:
                msg = f"[ERROR] NULL prices: {dim_prod_null_price} (expected 2)"
                logging.error(msg)
                errors.append(msg)

            if dim_prod_dups == 0:
                logging.info("[PASS] Duplicate product IDs: %d", dim_prod_dups)
            else:
                msg = f"[ERROR] Duplicate product IDs: {dim_prod_dups} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            # Log Date Dimension
            logging.info("DATE DIMENSION")
            if dim_date_rows == 1465:
                logging.info("[PASS] dim_date rows: %d", dim_date_rows)
            else:
                msg = f"[ERROR] dim_date rows: {dim_date_rows} (expected 1465)"
                logging.error(msg)
                errors.append(msg)

            if min_date == "2022-01-27" and max_date == "2026-01-30":
                logging.info("[PASS] Date range: %s → %s", min_date, max_date)
            else:
                msg = f"[ERROR] Date range: {min_date} → {max_date} (expected 2022-01-27 → 2026-01-30)"
                logging.error(msg)
                errors.append(msg)

            if dim_date_dups == 0:
                logging.info("[PASS] Duplicate dates: %d", dim_date_dups)
            else:
                msg = f"[ERROR] Duplicate dates: {dim_date_dups} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            # Log Staging Immutability
            logging.info("STAGING IMMUTABILITY")
            if post_cust == 2015 and post_cust == pre_cust:
                logging.info("[PASS] stg_customers: %d", post_cust)
            else:
                msg = f"[ERROR] stg_customers count changed: {post_cust} (expected 2015)"
                logging.error(msg)
                errors.append(msg)

            if post_prod == 100 and post_prod == pre_prod:
                logging.info("[PASS] stg_products: %d", post_prod)
            else:
                msg = f"[ERROR] stg_products count changed: {post_prod} (expected 100)"
                logging.error(msg)
                errors.append(msg)

            if post_ord == 10000 and post_ord == pre_ord:
                logging.info("[PASS] stg_orders: %d", post_ord)
            else:
                msg = f"[ERROR] stg_orders count changed: {post_ord} (expected 10000)"
                logging.error(msg)
                errors.append(msg)

            if post_items == 21968 and post_items == pre_items:
                logging.info("[PASS] stg_order_items: %d", post_items)
            else:
                msg = f"[ERROR] stg_order_items count changed: {post_items} (expected 21968)"
                logging.error(msg)
                errors.append(msg)

            # Log Fact Table Safety
            logging.info("FACT TABLE SAFETY")
            if post_fact == 21603 and post_fact == pre_fact:
                logging.info("[PASS] fact_sales rows unchanged: %d", post_fact)
            else:
                msg = f"[ERROR] fact_sales row count changed: {post_fact} (expected 21603)"
                logging.error(msg)
                errors.append(msg)

            logging.info("=" * 50)

            if errors:
                logging.error("TRANSFORMATION FAILED with %d error(s):", len(errors))
                for err in errors:
                    logging.error("  -> %s", err)
                raise ValueError(f"Dimension transformation failed: {'; '.join(errors)}")

            # Commit if all checks passed
            conn.commit()
            logging.info("DIMENSION TRANSFORMATION PASSED")
            logging.info("=" * 50)

    except Exception as exc:
        logging.error("Error during transform_dimensions, rolling back transaction: %s", exc)
        conn.rollback()
        raise
    finally:
        conn.close()
        logging.info("PostgreSQL connection closed.")

    logging.info("=== Task 4: transform_dimensions completed successfully ===")


# -----------------------------------------------------------------------------
# TASK 5: load_fact_sales
# -----------------------------------------------------------------------------
def load_fact_sales() -> None:
    """
    Transform and load validated staging order-item data into analytics.fact_sales.
    Uses dimension lookups (dim_customer, dim_product, dim_date), filters out invalid
    quantities (quantity <= 0) and orphans, calculates sales_amount = quantity * unit_price,
    and performs full source-to-fact reconciliation and data quality validations.

    Fully transactional and idempotent. Staging tables and dimension tables are protected.
    """
    logging.info("=== Starting Task 5: load_fact_sales ===")
    logging.info("Connecting to PostgreSQL at %s:%s / %s", PG_HOST, PG_PORT, PG_DB)

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    try:
        with conn.cursor() as cur:
            # 1. Capture pre-load counts
            cur.execute("SELECT COUNT(*) FROM staging.stg_customers;")
            pre_cust = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_products;")
            pre_prod = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_orders;")
            pre_ord = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_order_items;")
            pre_items = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM analytics.dim_customer;")
            pre_dim_cust = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM analytics.dim_product;")
            pre_dim_prod = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM analytics.dim_date;")
            pre_dim_date = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM analytics.fact_sales;")
            pre_fact = cur.fetchone()[0]

            # 2. Idempotent truncate of fact table
            cur.execute("TRUNCATE analytics.fact_sales RESTART IDENTITY;")

            # 3. Insert transformed valid rows
            cur.execute("""
                INSERT INTO analytics.fact_sales (
                    order_id,
                    customer_key,
                    product_key,
                    date_key,
                    quantity,
                    unit_price,
                    sales_amount
                )
                SELECT 
                    oi.order_id,
                    c.customer_key,
                    p.product_key,
                    d.date_key,
                    oi.quantity,
                    oi.unit_price,
                    (oi.quantity * oi.unit_price)::NUMERIC(14,2) AS sales_amount
                FROM staging.stg_order_items oi
                JOIN staging.stg_orders o 
                    ON oi.order_id = o.order_id
                JOIN analytics.dim_customer c 
                    ON o.customer_id = c.customer_id
                JOIN analytics.dim_product p 
                    ON oi.product_id = p.product_id
                JOIN analytics.dim_date d 
                    ON o.order_date = d.full_date
                WHERE oi.quantity > 0
                ORDER BY oi.order_id, oi.product_id;
            """)

            errors = []

            # 4. Source-to-fact reconciliation queries
            cur.execute("SELECT COUNT(*) FROM staging.stg_order_items;")
            src_order_items = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM staging.stg_order_items oi
                JOIN staging.stg_orders o ON oi.order_id = o.order_id
                JOIN analytics.dim_customer c ON o.customer_id = c.customer_id
                JOIN analytics.dim_product p ON oi.product_id = p.product_id
                JOIN analytics.dim_date d ON o.order_date = d.full_date
                WHERE oi.quantity > 0;
            """)
            valid_source_rows = cur.fetchone()[0]
            excluded_rows = src_order_items - valid_source_rows

            # 5. Fact table metric queries
            cur.execute("""
                SELECT 
                    COUNT(*),
                    COUNT(DISTINCT order_id),
                    SUM(quantity),
                    SUM(sales_amount)
                FROM analytics.fact_sales;
            """)
            fact_rows, distinct_orders, total_quantity, total_sales = cur.fetchone()

            # 6. Data quality queries
            cur.execute("SELECT COUNT(*) FROM analytics.fact_sales WHERE quantity <= 0;")
            invalid_qty = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) 
                FROM analytics.fact_sales 
                WHERE sales_amount != (quantity * unit_price)::NUMERIC(14,2);
            """)
            calc_mismatches = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) 
                FROM analytics.fact_sales f 
                LEFT JOIN analytics.dim_customer c ON f.customer_key = c.customer_key 
                WHERE c.customer_key IS NULL;
            """)
            orphan_cust = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) 
                FROM analytics.fact_sales f 
                LEFT JOIN analytics.dim_product p ON f.product_key = p.product_key 
                WHERE p.product_key IS NULL;
            """)
            orphan_prod = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) 
                FROM analytics.fact_sales f 
                LEFT JOIN analytics.dim_date d ON f.date_key = d.date_key 
                WHERE d.date_key IS NULL;
            """)
            orphan_date = cur.fetchone()[0]

            # 7. Post-load staging counts
            cur.execute("SELECT COUNT(*) FROM staging.stg_customers;")
            post_cust = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_products;")
            post_prod = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_orders;")
            post_ord = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_order_items;")
            post_items = cur.fetchone()[0]

            # 8. Post-load dimension counts
            cur.execute("SELECT COUNT(*) FROM analytics.dim_customer;")
            post_dim_cust = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM analytics.dim_product;")
            post_dim_prod = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM analytics.dim_date;")
            post_dim_date = cur.fetchone()[0]

            # 9. Logging and assertions
            logging.info("=" * 50)
            logging.info("FACT SALES LOAD")
            logging.info("=" * 50)

            # SOURCE RECONCILIATION
            logging.info("SOURCE RECONCILIATION")
            if src_order_items == 21968:
                logging.info("[PASS] Source order_items: %d", src_order_items)
            else:
                msg = f"[ERROR] Source order_items: {src_order_items} (expected 21968)"
                logging.error(msg)
                errors.append(msg)

            if valid_source_rows == 21603:
                logging.info("[PASS] Valid rows for fact: %d", valid_source_rows)
            else:
                msg = f"[ERROR] Valid rows for fact: {valid_source_rows} (expected 21603)"
                logging.error(msg)
                errors.append(msg)

            if excluded_rows == 365:
                logging.info("[PASS] Excluded rows: %d", excluded_rows)
            else:
                msg = f"[ERROR] Excluded rows: {excluded_rows} (expected 365)"
                logging.error(msg)
                errors.append(msg)

            # FACT TABLE
            logging.info("FACT TABLE")
            if fact_rows == 21603:
                logging.info("[PASS] fact_sales rows: %d", fact_rows)
            else:
                msg = f"[ERROR] fact_sales rows: {fact_rows} (expected 21603)"
                logging.error(msg)
                errors.append(msg)

            if distinct_orders == 9897:
                logging.info("[PASS] Distinct orders: %d", distinct_orders)
            else:
                msg = f"[ERROR] Distinct orders: {distinct_orders} (expected 9897)"
                logging.error(msg)
                errors.append(msg)

            if total_quantity == 38017:
                logging.info("[PASS] Total quantity: %d", total_quantity)
            else:
                msg = f"[ERROR] Total quantity: {total_quantity} (expected 38017)"
                logging.error(msg)
                errors.append(msg)

            formatted_sales = f"{total_sales:.2f}" if total_sales is not None else "0.00"
            if Decimal(formatted_sales) == Decimal("106439645.25"):
                logging.info("[PASS] Total sales: %s", formatted_sales)
            else:
                msg = f"[ERROR] Total sales: {formatted_sales} (expected 106439645.25)"
                logging.error(msg)
                errors.append(msg)

            # DATA QUALITY
            logging.info("DATA QUALITY")
            if invalid_qty == 0:
                logging.info("[PASS] Invalid quantities: %d", invalid_qty)
            else:
                msg = f"[ERROR] Invalid quantities: {invalid_qty} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if calc_mismatches == 0:
                logging.info("[PASS] Sales amount mismatches: %d", calc_mismatches)
            else:
                msg = f"[ERROR] Sales amount mismatches: {calc_mismatches} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if orphan_cust == 0:
                logging.info("[PASS] Orphan customer keys: %d", orphan_cust)
            else:
                msg = f"[ERROR] Orphan customer keys: {orphan_cust} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if orphan_prod == 0:
                logging.info("[PASS] Orphan product keys: %d", orphan_prod)
            else:
                msg = f"[ERROR] Orphan product keys: {orphan_prod} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if orphan_date == 0:
                logging.info("[PASS] Orphan date keys: %d", orphan_date)
            else:
                msg = f"[ERROR] Orphan date keys: {orphan_date} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            # STAGING IMMUTABILITY
            logging.info("STAGING IMMUTABILITY")
            if post_cust == 2015 and post_cust == pre_cust:
                logging.info("[PASS] stg_customers: %d", post_cust)
            else:
                msg = f"[ERROR] stg_customers count changed: {post_cust} (expected 2015)"
                logging.error(msg)
                errors.append(msg)

            if post_prod == 100 and post_prod == pre_prod:
                logging.info("[PASS] stg_products: %d", post_prod)
            else:
                msg = f"[ERROR] stg_products count changed: {post_prod} (expected 100)"
                logging.error(msg)
                errors.append(msg)

            if post_ord == 10000 and post_ord == pre_ord:
                logging.info("[PASS] stg_orders: %d", post_ord)
            else:
                msg = f"[ERROR] stg_orders count changed: {post_ord} (expected 10000)"
                logging.error(msg)
                errors.append(msg)

            if post_items == 21968 and post_items == pre_items:
                logging.info("[PASS] stg_order_items: %d", post_items)
            else:
                msg = f"[ERROR] stg_order_items count changed: {post_items} (expected 21968)"
                logging.error(msg)
                errors.append(msg)

            # DIMENSION IMMUTABILITY
            logging.info("DIMENSION IMMUTABILITY")
            if post_dim_cust == 2000 and post_dim_cust == pre_dim_cust:
                logging.info("[PASS] dim_customer: %d", post_dim_cust)
            else:
                msg = f"[ERROR] dim_customer count changed: {post_dim_cust} (expected 2000)"
                logging.error(msg)
                errors.append(msg)

            if post_dim_prod == 100 and post_dim_prod == pre_dim_prod:
                logging.info("[PASS] dim_product: %d", post_dim_prod)
            else:
                msg = f"[ERROR] dim_product count changed: {post_dim_prod} (expected 100)"
                logging.error(msg)
                errors.append(msg)

            if post_dim_date == 1465 and post_dim_date == pre_dim_date:
                logging.info("[PASS] dim_date: %d", post_dim_date)
            else:
                msg = f"[ERROR] dim_date count changed: {post_dim_date} (expected 1465)"
                logging.error(msg)
                errors.append(msg)

            logging.info("=" * 50)

            if errors:
                logging.error("FACT LOAD FAILED with %d error(s):", len(errors))
                for err in errors:
                    logging.error("  -> %s", err)
                raise ValueError(f"Fact table load failed: {'; '.join(errors)}")

            # Commit if all checks passed
            conn.commit()
            logging.info("FACT SALES LOAD PASSED")
            logging.info("=" * 50)

    except Exception as exc:
        logging.error("Error during load_fact_sales, rolling back transaction: %s", exc)
        conn.rollback()
        raise
    finally:
        conn.close()
        logging.info("PostgreSQL connection closed.")

    logging.info("=== Task 5: load_fact_sales completed successfully ===")


# -----------------------------------------------------------------------------
# TASK 6: validate_warehouse
# -----------------------------------------------------------------------------
def validate_warehouse() -> None:
    """
    Comprehensive, read-only final warehouse validation gate.
    Validates star schema row counts, customer/product/date dimension data quality,
    calendar continuity, fact sales metrics, referential integrity, staging-to-warehouse
    reconciliation, and staging immutability.

    Operates strictly read-only and fails the task on any discrepancy.
    """
    logging.info("=== Starting Task 6: validate_warehouse ===")
    logging.info("Connecting to PostgreSQL at %s:%s / %s (READ ONLY)", PG_HOST, PG_PORT, PG_DB)

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    conn.set_session(readonly=True)

    errors = []

    try:
        with conn.cursor() as cur:
            # 1. Dimension row counts
            cur.execute("SELECT COUNT(*) FROM analytics.dim_customer;")
            dim_cust_rows = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM analytics.dim_product;")
            dim_prod_rows = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM analytics.dim_date;")
            dim_date_rows = cur.fetchone()[0]

            # 2. Customer quality
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT customer_id FROM analytics.dim_customer GROUP BY customer_id HAVING COUNT(*) > 1
                ) t;
            """)
            cust_dups = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM analytics.dim_customer
                WHERE customer_id IS NULL
                   OR customer_name IS NULL
                   OR city IS NULL
                   OR state IS NULL
                   OR country IS NULL
                   OR signup_date IS NULL;
            """)
            cust_nulls = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM analytics.dim_customer WHERE city = 'Unknown';")
            cust_unknown_cities = cur.fetchone()[0]

            # 3. Product quality
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT product_id FROM analytics.dim_product GROUP BY product_id HAVING COUNT(*) > 1
                ) t;
            """)
            prod_dups = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM analytics.dim_product
                WHERE product_id IS NULL
                   OR product_name IS NULL
                   OR category IS NULL;
            """)
            prod_nulls = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM analytics.dim_product WHERE price IS NULL;")
            prod_null_prices = cur.fetchone()[0]

            # 4. Date quality
            cur.execute("SELECT MIN(full_date)::TEXT, MAX(full_date)::TEXT FROM analytics.dim_date;")
            min_date, max_date = cur.fetchone()

            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT full_date FROM analytics.dim_date GROUP BY full_date HAVING COUNT(*) > 1
                ) t;
            """)
            date_dups = cur.fetchone()[0]

            cur.execute("""
                WITH bounds AS (
                    SELECT MIN(full_date) AS min_d, MAX(full_date) AS max_d FROM analytics.dim_date
                ),
                expected_series AS (
                    SELECT generate_series(min_d::timestamp, max_d::timestamp, '1 day'::interval)::date AS d FROM bounds
                )
                SELECT COUNT(*) 
                FROM expected_series e 
                LEFT JOIN analytics.dim_date d ON e.d = d.full_date 
                WHERE d.full_date IS NULL;
            """)
            missing_calendar_dates = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) 
                FROM analytics.dim_date 
                WHERE date_key != TO_CHAR(full_date, 'YYYYMMDD')::INTEGER;
            """)
            date_key_mismatches = cur.fetchone()[0]

            # 5. Fact table
            cur.execute("SELECT COUNT(*) FROM analytics.fact_sales;")
            fact_rows = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT sales_key FROM analytics.fact_sales GROUP BY sales_key HAVING COUNT(*) > 1
                ) t;
            """)
            sales_key_dups = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM analytics.fact_sales
                WHERE order_id IS NULL
                   OR customer_key IS NULL
                   OR product_key IS NULL
                   OR date_key IS NULL
                   OR quantity IS NULL
                   OR unit_price IS NULL
                   OR sales_amount IS NULL;
            """)
            fact_nulls = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT order_id) FROM analytics.fact_sales;")
            distinct_orders = cur.fetchone()[0]

            cur.execute("SELECT SUM(quantity), SUM(sales_amount) FROM analytics.fact_sales;")
            total_quantity, total_sales = cur.fetchone()

            cur.execute("SELECT COUNT(*) FROM analytics.fact_sales WHERE quantity <= 0;")
            invalid_qty = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM analytics.fact_sales
                WHERE sales_amount != (quantity * unit_price)::NUMERIC(14,2);
            """)
            sales_mismatches = cur.fetchone()[0]

            # 6. Referential integrity
            cur.execute("""
                SELECT COUNT(*) 
                FROM analytics.fact_sales f 
                LEFT JOIN analytics.dim_customer c ON f.customer_key = c.customer_key 
                WHERE c.customer_key IS NULL;
            """)
            orphan_cust = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) 
                FROM analytics.fact_sales f 
                LEFT JOIN analytics.dim_product p ON f.product_key = p.product_key 
                WHERE p.product_key IS NULL;
            """)
            orphan_prod = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) 
                FROM analytics.fact_sales f 
                LEFT JOIN analytics.dim_date d ON f.date_key = d.date_key 
                WHERE d.date_key IS NULL;
            """)
            orphan_date = cur.fetchone()[0]

            # 7. Fact coverage
            cur.execute("SELECT COUNT(DISTINCT customer_key) FROM analytics.fact_sales;")
            covered_cust = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT product_key) FROM analytics.fact_sales;")
            covered_prod = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT date_key) FROM analytics.fact_sales;")
            covered_dates = cur.fetchone()[0]

            if covered_cust <= 0:
                errors.append("Fact customer coverage is 0 (expected > 0)")
            if covered_prod <= 0:
                errors.append("Fact product coverage is 0 (expected > 0)")
            if covered_dates <= 0:
                errors.append("Fact date coverage is 0 (expected > 0)")

            # 8. Source-to-fact reconciliation
            cur.execute("SELECT COUNT(*) FROM staging.stg_order_items;")
            stg_order_items_count = cur.fetchone()[0]

            cur.execute("""
                SELECT 
                    COUNT(*),
                    SUM(oi.quantity),
                    SUM((oi.quantity * oi.unit_price)::NUMERIC(14,2))
                FROM staging.stg_order_items oi
                JOIN staging.stg_orders o ON oi.order_id = o.order_id
                JOIN analytics.dim_customer c ON o.customer_id = c.customer_id
                JOIN analytics.dim_product p ON oi.product_id = p.product_id
                JOIN analytics.dim_date d ON o.order_date = d.full_date
                WHERE oi.quantity > 0;
            """)
            valid_src_rows, valid_src_qty, valid_src_sales = cur.fetchone()

            row_diff = abs(fact_rows - valid_src_rows)
            qty_diff = abs(total_quantity - valid_src_qty)
            sales_diff = abs(total_sales - valid_src_sales)

            # 9. Staging immutability
            cur.execute("SELECT COUNT(*) FROM staging.stg_customers;")
            stg_cust = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_products;")
            stg_prod = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_orders;")
            stg_ord = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM staging.stg_order_items;")
            stg_items = cur.fetchone()[0]

            # 10. Logging and Assertions
            logging.info("=" * 50)
            logging.info("FINAL WAREHOUSE VALIDATION")
            logging.info("=" * 50)

            # DIMENSIONS
            logging.info("DIMENSIONS")
            if dim_cust_rows == 2000:
                logging.info("[PASS] dim_customer rows: %d", dim_cust_rows)
            else:
                msg = f"[ERROR] dim_customer rows: {dim_cust_rows} (expected 2000)"
                logging.error(msg)
                errors.append(msg)

            if dim_prod_rows == 100:
                logging.info("[PASS] dim_product rows: %d", dim_prod_rows)
            else:
                msg = f"[ERROR] dim_product rows: {dim_prod_rows} (expected 100)"
                logging.error(msg)
                errors.append(msg)

            if dim_date_rows == 1465:
                logging.info("[PASS] dim_date rows: %d", dim_date_rows)
            else:
                msg = f"[ERROR] dim_date rows: {dim_date_rows} (expected 1465)"
                logging.error(msg)
                errors.append(msg)

            # CUSTOMER QUALITY
            logging.info("CUSTOMER QUALITY")
            if cust_dups == 0:
                logging.info("[PASS] Duplicate customer IDs: %d", cust_dups)
            else:
                msg = f"[ERROR] Duplicate customer IDs: {cust_dups} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if cust_nulls == 0:
                logging.info("[PASS] Required NULLs: %d", cust_nulls)
            else:
                msg = f"[ERROR] Required NULLs in dim_customer: {cust_nulls} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if cust_unknown_cities == 15:
                logging.info("[PASS] Unknown cities: %d", cust_unknown_cities)
            else:
                msg = f"[ERROR] Unknown cities: {cust_unknown_cities} (expected 15)"
                logging.error(msg)
                errors.append(msg)

            # PRODUCT QUALITY
            logging.info("PRODUCT QUALITY")
            if prod_dups == 0:
                logging.info("[PASS] Duplicate product IDs: %d", prod_dups)
            else:
                msg = f"[ERROR] Duplicate product IDs: {prod_dups} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if prod_nulls == 0:
                logging.info("[PASS] Required NULLs: %d", prod_nulls)
            else:
                msg = f"[ERROR] Required NULLs in dim_product: {prod_nulls} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if prod_null_prices == 2:
                logging.info("[PASS] Intentional NULL prices: %d", prod_null_prices)
            else:
                msg = f"[ERROR] Intentional NULL prices: {prod_null_prices} (expected 2)"
                logging.error(msg)
                errors.append(msg)

            # DATE QUALITY
            logging.info("DATE QUALITY")
            if min_date == "2022-01-27" and max_date == "2026-01-30":
                logging.info("[PASS] Date range: %s → %s", min_date, max_date)
            else:
                msg = f"[ERROR] Date range: {min_date} → {max_date} (expected 2022-01-27 → 2026-01-30)"
                logging.error(msg)
                errors.append(msg)

            if date_dups == 0:
                logging.info("[PASS] Duplicate dates: %d", date_dups)
            else:
                msg = f"[ERROR] Duplicate dates: {date_dups} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if missing_calendar_dates == 0:
                logging.info("[PASS] Missing calendar dates: %d", missing_calendar_dates)
            else:
                msg = f"[ERROR] Missing calendar dates: {missing_calendar_dates} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if date_key_mismatches == 0:
                logging.info("[PASS] Date-key mismatches: %d", date_key_mismatches)
            else:
                msg = f"[ERROR] Date-key mismatches: {date_key_mismatches} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            # FACT TABLE
            logging.info("FACT TABLE")
            if fact_rows == 21603 and sales_key_dups == 0 and fact_nulls == 0:
                logging.info("[PASS] Fact rows: %d", fact_rows)
            else:
                msg = (
                    f"[ERROR] Fact rows: {fact_rows} (expected 21603), "
                    f"sales_key dups: {sales_key_dups}, fact NULLs: {fact_nulls}"
                )
                logging.error(msg)
                errors.append(msg)

            if distinct_orders == 9897:
                logging.info("[PASS] Distinct orders: %d", distinct_orders)
            else:
                msg = f"[ERROR] Distinct orders: {distinct_orders} (expected 9897)"
                logging.error(msg)
                errors.append(msg)

            if total_quantity == 38017:
                logging.info("[PASS] Total quantity: %d", total_quantity)
            else:
                msg = f"[ERROR] Total quantity: {total_quantity} (expected 38017)"
                logging.error(msg)
                errors.append(msg)

            formatted_sales = f"{total_sales:.2f}" if total_sales is not None else "0.00"
            if Decimal(formatted_sales) == Decimal("106439645.25"):
                logging.info("[PASS] Total sales: %s", formatted_sales)
            else:
                msg = f"[ERROR] Total sales: {formatted_sales} (expected 106439645.25)"
                logging.error(msg)
                errors.append(msg)

            if invalid_qty == 0:
                logging.info("[PASS] Invalid quantities: %d", invalid_qty)
            else:
                msg = f"[ERROR] Invalid quantities: {invalid_qty} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if sales_mismatches == 0:
                logging.info("[PASS] Sales calculation mismatches: %d", sales_mismatches)
            else:
                msg = f"[ERROR] Sales calculation mismatches: {sales_mismatches} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            # REFERENTIAL INTEGRITY
            logging.info("REFERENTIAL INTEGRITY")
            if orphan_cust == 0:
                logging.info("[PASS] Customer orphans: %d", orphan_cust)
            else:
                msg = f"[ERROR] Customer orphans: {orphan_cust} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if orphan_prod == 0:
                logging.info("[PASS] Product orphans: %d", orphan_prod)
            else:
                msg = f"[ERROR] Product orphans: {orphan_prod} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if orphan_date == 0:
                logging.info("[PASS] Date orphans: %d", orphan_date)
            else:
                msg = f"[ERROR] Date orphans: {orphan_date} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            # SOURCE RECONCILIATION
            logging.info("SOURCE RECONCILIATION")
            if stg_order_items_count == 21968:
                logging.info("[PASS] Staging order_items: %d", stg_order_items_count)
            else:
                msg = f"[ERROR] Staging order_items: {stg_order_items_count} (expected 21968)"
                logging.error(msg)
                errors.append(msg)

            if valid_src_rows == 21603:
                logging.info("[PASS] Valid source rows: %d", valid_src_rows)
            else:
                msg = f"[ERROR] Valid source rows: {valid_src_rows} (expected 21603)"
                logging.error(msg)
                errors.append(msg)

            if row_diff == 0:
                logging.info("[PASS] Source/fact row difference: %d", row_diff)
            else:
                msg = f"[ERROR] Source/fact row difference: {row_diff} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if qty_diff == 0:
                logging.info("[PASS] Source/fact quantity difference: %d", qty_diff)
            else:
                msg = f"[ERROR] Source/fact quantity difference: {qty_diff} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            if sales_diff == Decimal("0.00"):
                logging.info("[PASS] Source/fact sales difference: %s", sales_diff)
            else:
                msg = f"[ERROR] Source/fact sales difference: {sales_diff} (expected 0)"
                logging.error(msg)
                errors.append(msg)

            # STAGING
            logging.info("STAGING")
            if stg_cust == 2015:
                logging.info("[PASS] stg_customers: %d", stg_cust)
            else:
                msg = f"[ERROR] stg_customers count: {stg_cust} (expected 2015)"
                logging.error(msg)
                errors.append(msg)

            if stg_prod == 100:
                logging.info("[PASS] stg_products: %d", stg_prod)
            else:
                msg = f"[ERROR] stg_products count: {stg_prod} (expected 100)"
                logging.error(msg)
                errors.append(msg)

            if stg_ord == 10000:
                logging.info("[PASS] stg_orders: %d", stg_ord)
            else:
                msg = f"[ERROR] stg_orders count: {stg_ord} (expected 10000)"
                logging.error(msg)
                errors.append(msg)

            if stg_items == 21968:
                logging.info("[PASS] stg_order_items: %d", stg_items)
            else:
                msg = f"[ERROR] stg_order_items count: {stg_items} (expected 21968)"
                logging.error(msg)
                errors.append(msg)

            logging.info("=" * 50)

            if errors:
                logging.error("FINAL WAREHOUSE VALIDATION FAILED with %d error(s):", len(errors))
                for err in errors:
                    logging.error("  -> %s", err)
                raise ValueError(f"Warehouse validation failed: {'; '.join(errors)}")

            logging.info("WAREHOUSE VALIDATION PASSED")
            logging.info("=" * 50)

    finally:
        conn.close()
        logging.info("PostgreSQL connection closed.")

    logging.info("=== Task 6: validate_warehouse completed successfully ===")


# -----------------------------------------------------------------------------
# DAG DEFINITION
# -----------------------------------------------------------------------------
with DAG(
    dag_id="ecommerce_etl",
    default_args=default_args,
    description="End-to-End E-Commerce Data Warehouse ETL Pipeline",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "etl", "data-engineering"],
) as dag:

    task_validate_source = PythonOperator(
        task_id="validate_source",
        python_callable=validate_source,
    )

    task_load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=load_staging,
    )

    task_validate_staging = PythonOperator(
        task_id="validate_staging",
        python_callable=validate_staging,
    )

    task_transform_dimensions = PythonOperator(
        task_id="transform_dimensions",
        python_callable=transform_dimensions,
    )

    task_load_fact_sales = PythonOperator(
        task_id="load_fact_sales",
        python_callable=load_fact_sales,
    )

    task_validate_warehouse = PythonOperator(
        task_id="validate_warehouse",
        python_callable=validate_warehouse,
    )

    # Linear ETL pipeline dependency chain
    (
        task_validate_source
        >> task_load_staging
        >> task_validate_staging
        >> task_transform_dimensions
        >> task_load_fact_sales
        >> task_validate_warehouse
    )
