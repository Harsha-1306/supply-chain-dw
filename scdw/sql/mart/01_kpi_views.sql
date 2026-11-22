-- MART: the reporting layer Power BI connects to (role SCDW_REPORTER has SELECT on MART only).
-- KPI definitions are documented in docs/kpi_definitions.md and mirrored by the DAX measures in powerbi/.

-- OTIF and lead time per month, for closed PO lines (fully received, nothing in transit).
CREATE OR REPLACE VIEW MART.V_KPI_OTIF_MONTHLY AS
SELECT
    DATE_TRUNC('month', promised_date)                AS month,
    COUNT(*)                                          AS closed_lines,
    ROUND(AVG(is_on_time * 1.0), 4)                   AS on_time_rate,
    ROUND(AVG(is_in_full * 1.0), 4)                   AS in_full_rate,
    ROUND(AVG(is_otif * 1.0), 4)                      AS otif_rate,
    ROUND(AVG(lead_time_days * 1.0), 1)               AS avg_lead_time_days,
    SUM(line_value_eur)                               AS spend_eur
FROM CORE.FACT_PO_LINE
WHERE is_closed = 1
GROUP BY DATE_TRUNC('month', promised_date);

-- Supplier scorecard over all closed lines.
CREATE OR REPLACE VIEW MART.V_KPI_SUPPLIER_SCORECARD AS
SELECT
    s.supplier_id,
    s.supplier_name,
    s.country,
    s.tier_name,
    COUNT(*)                                          AS closed_lines,
    ROUND(AVG(f.is_otif * 1.0), 4)                    AS otif_rate,
    ROUND(AVG(f.is_on_time * 1.0), 4)                 AS on_time_rate,
    ROUND(AVG(f.is_in_full * 1.0), 4)                 AS in_full_rate,
    ROUND(AVG(f.lead_time_days * 1.0), 1)             AS avg_lead_time_days,
    SUM(f.line_value_eur)                             AS spend_eur,
    CASE
        WHEN AVG(f.is_otif * 1.0) >= 0.90 THEN 'A'
        WHEN AVG(f.is_otif * 1.0) >= 0.75 THEN 'B'
        ELSE 'C'
    END                                               AS rating
FROM CORE.FACT_PO_LINE f
JOIN CORE.DIM_SUPPLIER s ON s.supplier_id = f.supplier_id
WHERE f.is_closed = 1
GROUP BY s.supplier_id, s.supplier_name, s.country, s.tier_name;

-- Daily inventory health per warehouse and product category.
CREATE OR REPLACE VIEW MART.V_KPI_INVENTORY_HEALTH AS
SELECT
    i.snapshot_date,
    w.warehouse_id,
    w.warehouse_name,
    p.category,
    COUNT(*)                                          AS sku_positions,
    ROUND(AVG(i.is_stockout * 1.0), 4)                AS stockout_rate,
    ROUND(AVG(i.is_below_reorder_point * 1.0), 4)     AS below_reorder_point_rate,
    SUM(i.on_hand_qty)                                AS on_hand_qty,
    SUM(i.stock_value_eur)                            AS stock_value_eur
FROM CORE.FACT_INVENTORY_SNAPSHOT i
JOIN CORE.DIM_WAREHOUSE w ON w.warehouse_id = i.warehouse_id
JOIN CORE.DIM_PRODUCT p   ON p.sku = i.sku
GROUP BY i.snapshot_date, w.warehouse_id, w.warehouse_name, p.category;

-- Inbound performance per warehouse and month.
CREATE OR REPLACE VIEW MART.V_KPI_WAREHOUSE_INBOUND AS
SELECT
    DATE_TRUNC('month', f.delivered_date)             AS month,
    w.warehouse_id,
    w.warehouse_name,
    COUNT(*)                                          AS deliveries,
    SUM(f.qty_shipped)                                AS qty_received,
    ROUND(AVG(f.is_late * 1.0), 4)                    AS late_delivery_rate,
    ROUND(AVG(f.transit_days * 1.0), 1)               AS avg_transit_days
FROM CORE.FACT_SHIPMENT f
JOIN CORE.DIM_WAREHOUSE w ON w.warehouse_id = f.warehouse_id
WHERE f.status = 'DELIVERED' AND f.is_orphan = 0
GROUP BY DATE_TRUNC('month', f.delivered_date), w.warehouse_id, w.warehouse_name;

-- Data-quality scorecard: latest result of every check, so the dashboard shows whether the numbers can be trusted.
CREATE OR REPLACE VIEW MART.V_DQ_SCORECARD AS
SELECT run_id, batch_id, check_name, table_name, severity, status, metric, threshold, details, checked_at
FROM AUDIT.DQ_RESULTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY check_name, table_name ORDER BY checked_at DESC) = 1;

-- Load statistics per batch and table.
CREATE OR REPLACE VIEW MART.V_LOAD_SUMMARY AS
SELECT batch_id, table_name, raw_rows, exact_duplicate_rows, key_duplicate_rows, rejected_rows, merged_keys,
       status, loaded_at
FROM AUDIT.LOAD_LOG;
