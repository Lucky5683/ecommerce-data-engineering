-- =============================================================================
-- View: analytics.vw_data_quality_issues
-- Description: Aggregates real data-quality issues from staging tables
--              for Power BI Page 3 (Data Quality & Pipeline Monitoring).
-- =============================================================================

CREATE OR REPLACE VIEW analytics.vw_data_quality_issues AS
SELECT 
    'Duplicate Customer IDs'::text AS issue,
    COUNT(*)::bigint AS issue_count
FROM (
    SELECT customer_id
    FROM staging.stg_customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
) AS dup_customers

UNION ALL

SELECT 
    'Missing Customer Cities'::text AS issue,
    COUNT(*)::bigint AS issue_count
FROM staging.stg_customers
WHERE city IS NULL

UNION ALL

SELECT 
    'Missing Product Prices'::text AS issue,
    COUNT(*)::bigint AS issue_count
FROM staging.stg_products
WHERE price IS NULL

UNION ALL

SELECT 
    'Missing Payment Methods'::text AS issue,
    COUNT(*)::bigint AS issue_count
FROM staging.stg_orders
WHERE payment_method IS NULL

UNION ALL

SELECT 
    'Orphan Customer IDs'::text AS issue,
    COUNT(*)::bigint AS issue_count
FROM staging.stg_orders o
LEFT JOIN staging.stg_customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL

UNION ALL

SELECT 
    'Orphan Product IDs'::text AS issue,
    COUNT(*)::bigint AS issue_count
FROM staging.stg_order_items oi
LEFT JOIN staging.stg_products p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL

UNION ALL

SELECT 
    'Invalid Quantities'::text AS issue,
    COUNT(*)::bigint AS issue_count
FROM staging.stg_order_items
WHERE quantity <= 0;
