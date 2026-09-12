-- =============================================================================
-- View: analytics.vw_warehouse_reconciliation
-- Description: Source-to-warehouse reconciliation metrics for Power BI Page 3
--              (Data Quality & Pipeline Monitoring). Validates that valid staging
--              records match analytics.fact_sales records, quantities, and sales.
-- =============================================================================

CREATE OR REPLACE VIEW analytics.vw_warehouse_reconciliation AS
WITH source_data AS (
    SELECT 
        COUNT(*)::bigint AS source_valid_rows,
        SUM(oi.quantity)::bigint AS source_total_quantity,
        SUM((oi.quantity * oi.unit_price)::NUMERIC(14,2))::NUMERIC(14,2) AS source_total_sales
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
),
fact_data AS (
    SELECT 
        COUNT(*)::bigint AS fact_rows,
        SUM(quantity)::bigint AS fact_total_quantity,
        SUM(sales_amount)::NUMERIC(14,2) AS fact_total_sales
    FROM analytics.fact_sales
)
SELECT 
    s.source_valid_rows,
    f.fact_rows,
    (s.source_valid_rows - f.fact_rows)::bigint AS row_difference,
    s.source_total_quantity,
    f.fact_total_quantity,
    (s.source_total_quantity - f.fact_total_quantity)::bigint AS quantity_difference,
    s.source_total_sales,
    f.fact_total_sales,
    (s.source_total_sales - f.fact_total_sales)::NUMERIC(14,2) AS sales_difference
FROM source_data s
CROSS JOIN fact_data f;
