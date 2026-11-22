-- CORE facts. Currency conversion uses static monthly-average rates kept in CORE.FX_RATES
-- (in production this table would be fed from the treasury/ECB feed).

CREATE OR REPLACE TABLE CORE.FX_RATES AS
SELECT 'EUR' AS currency, CAST(1.0 AS DECIMAL(10,4)) AS rate_to_eur
UNION ALL SELECT 'USD', CAST(0.92 AS DECIMAL(10,4))
UNION ALL SELECT 'GBP', CAST(1.17 AS DECIMAL(10,4));

-- Grain: one row per shipment.
CREATE OR REPLACE TABLE CORE.FACT_SHIPMENT AS
SELECT
    s.shipment_id,
    s.po_id,
    s.po_line,
    COALESCE(ds.supplier_id, 'UNKNOWN')  AS supplier_id,
    COALESCE(dp.sku, 'UNKNOWN')          AS sku,
    COALESCE(dw.warehouse_id, 'UNKNOWN') AS warehouse_id,
    s.carrier,
    s.status,
    s.ship_date,
    s.delivered_date,
    po.promised_date,
    s.qty_shipped,
    DATEDIFF('day', s.ship_date, s.delivered_date)                    AS transit_days,
    CASE WHEN s.delivered_date > po.promised_date THEN 1 ELSE 0 END   AS is_late,
    CASE WHEN po.po_id IS NULL THEN 1 ELSE 0 END                      AS is_orphan
FROM STAGING.shipments s
LEFT JOIN STAGING.purchase_orders po ON po.po_id = s.po_id AND po.po_line = s.po_line
LEFT JOIN CORE.DIM_SUPPLIER ds  ON ds.supplier_id = po.supplier_id
LEFT JOIN CORE.DIM_PRODUCT dp   ON dp.sku = po.sku
LEFT JOIN CORE.DIM_WAREHOUSE dw ON dw.warehouse_id = po.warehouse_id
WHERE s.status <> 'CANCELLED';

-- Grain: one row per purchase-order line, with its delivery outcome (OTIF building blocks).
CREATE OR REPLACE TABLE CORE.FACT_PO_LINE AS
WITH deliveries AS (
    SELECT
        po_id,
        po_line,
        COUNT(*)                                                         AS shipment_count,
        SUM(CASE WHEN status = 'DELIVERED' THEN qty_shipped ELSE 0 END)  AS qty_delivered,
        MIN(delivered_date)                                              AS first_delivered_date,
        MAX(delivered_date)                                              AS last_delivered_date,
        MAX(CASE WHEN status = 'IN_TRANSIT' THEN 1 ELSE 0 END)           AS has_open_shipment
    FROM STAGING.shipments
    WHERE status <> 'CANCELLED'
    GROUP BY po_id, po_line
)
SELECT
    po.po_id,
    po.po_line,
    COALESCE(ds.supplier_id, 'UNKNOWN')  AS supplier_id,
    COALESCE(dp.sku, 'UNKNOWN')          AS sku,
    COALESCE(dw.warehouse_id, 'UNKNOWN') AS warehouse_id,
    po.order_date,
    po.promised_date,
    po.qty_ordered,
    po.unit_price,
    po.currency,
    CAST(po.qty_ordered * po.unit_price * fx.rate_to_eur AS DECIMAL(14,2))  AS line_value_eur,
    COALESCE(d.shipment_count, 0)                                           AS shipment_count,
    COALESCE(d.qty_delivered, 0)                                            AS qty_delivered,
    d.first_delivered_date,
    d.last_delivered_date,
    CASE WHEN d.last_delivered_date IS NOT NULL AND COALESCE(d.has_open_shipment, 0) = 0
         THEN 1 ELSE 0 END                                                  AS is_closed,
    DATEDIFF('day', po.order_date, d.last_delivered_date)                   AS lead_time_days,
    CASE WHEN d.last_delivered_date <= po.promised_date THEN 1 ELSE 0 END   AS is_on_time,
    CASE WHEN COALESCE(d.qty_delivered, 0) >= po.qty_ordered THEN 1 ELSE 0 END AS is_in_full,
    CASE WHEN d.last_delivered_date <= po.promised_date
          AND COALESCE(d.qty_delivered, 0) >= po.qty_ordered THEN 1 ELSE 0 END AS is_otif
FROM STAGING.purchase_orders po
LEFT JOIN deliveries d           ON d.po_id = po.po_id AND d.po_line = po.po_line
LEFT JOIN CORE.FX_RATES fx       ON fx.currency = po.currency
LEFT JOIN CORE.DIM_SUPPLIER ds   ON ds.supplier_id = po.supplier_id
LEFT JOIN CORE.DIM_PRODUCT dp    ON dp.sku = po.sku
LEFT JOIN CORE.DIM_WAREHOUSE dw  ON dw.warehouse_id = po.warehouse_id;

-- Grain: one row per warehouse, SKU and snapshot day.
CREATE OR REPLACE TABLE CORE.FACT_INVENTORY_SNAPSHOT AS
SELECT
    i.snapshot_date,
    COALESCE(dw.warehouse_id, 'UNKNOWN') AS warehouse_id,
    COALESCE(dp.sku, 'UNKNOWN')          AS sku,
    i.on_hand_qty,
    i.reorder_point,
    CAST(i.on_hand_qty * COALESCE(dp.unit_cost, 0) AS DECIMAL(14,2)) AS stock_value_eur,
    CASE WHEN i.on_hand_qty = 0 THEN 1 ELSE 0 END                    AS is_stockout,
    CASE WHEN i.on_hand_qty < i.reorder_point THEN 1 ELSE 0 END      AS is_below_reorder_point
FROM STAGING.inventory_snapshots i
LEFT JOIN CORE.DIM_WAREHOUSE dw ON dw.warehouse_id = i.warehouse_id
LEFT JOIN CORE.DIM_PRODUCT dp   ON dp.sku = i.sku;
