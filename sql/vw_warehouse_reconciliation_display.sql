-- =============================================================================
-- View: analytics.vw_warehouse_reconciliation_display
-- Description: Unpivots reconciliation metrics into a display-friendly table
--              (Rows, Total Quantity, Total Sales as rows; Source, Warehouse,
--              Difference as columns) for Power BI Page 3.
-- =============================================================================

CREATE OR REPLACE VIEW analytics.vw_warehouse_reconciliation_display AS
WITH recon AS (
    SELECT 
        source_valid_rows,
        fact_rows,
        row_difference,
        source_total_quantity,
        fact_total_quantity,
        quantity_difference,
        source_total_sales,
        fact_total_sales,
        sales_difference
    FROM analytics.vw_warehouse_reconciliation
)
SELECT 
    t.metric,
    t.source_value,
    t.warehouse_value,
    t.difference
FROM recon r
CROSS JOIN LATERAL (
    VALUES
        (1, 'Rows'::text, r.source_valid_rows::numeric, r.fact_rows::numeric, r.row_difference::numeric),
        (2, 'Total Quantity'::text, r.source_total_quantity::numeric, r.fact_total_quantity::numeric, r.quantity_difference::numeric),
        (3, 'Total Sales'::text, r.source_total_sales::numeric, r.fact_total_sales::numeric, r.sales_difference::numeric)
) AS t(sort_order, metric, source_value, warehouse_value, difference)
ORDER BY t.sort_order;
