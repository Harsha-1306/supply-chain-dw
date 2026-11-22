-- CORE dimensions: conformed master data with an explicit UNKNOWN member, so facts never drop rows
-- whose foreign key has no master record (those are counted by the referential-integrity check instead).

CREATE OR REPLACE TABLE CORE.DIM_SUPPLIER AS
SELECT
    supplier_id,
    supplier_name,
    country,
    tier,
    CASE tier WHEN 1 THEN 'Strategic' WHEN 2 THEN 'Preferred' ELSE 'Transactional' END AS tier_name
FROM STAGING.suppliers
UNION ALL
SELECT 'UNKNOWN', 'Unknown supplier', 'XX', 0, 'Unknown';

CREATE OR REPLACE TABLE CORE.DIM_PRODUCT AS
SELECT
    sku,
    product_name,
    category,
    unit_cost,
    uom
FROM STAGING.products
UNION ALL
SELECT 'UNKNOWN', 'Unknown product', 'Unknown', 0, 'EA';

CREATE OR REPLACE TABLE CORE.DIM_WAREHOUSE AS
SELECT
    warehouse_id,
    warehouse_name,
    city,
    country,
    capacity_units
FROM STAGING.warehouses
UNION ALL
SELECT 'UNKNOWN', 'Unknown warehouse', 'Unknown', 'XX', 0;
