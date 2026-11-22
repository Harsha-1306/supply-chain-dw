# Power BI – Supply Chain KPI report

The report sits on the Snowflake star schema built by the pipeline. Everything needed to build it is in this
folder: connection settings, model, DAX measures (`measures.dax`) and theme (`theme.json`).
`../docs/dashboard_preview.png` shows the four pages rendered from the same MART views.

## Connect

1. Power BI Desktop → **Get data → Snowflake**.
2. Server: `<account>.snowflakecomputing.com`, Warehouse: `SCDW_BI_WH`, role `SCDW_REPORTER`
   (user `SVC_SCDW_POWERBI`, see `snowflake/00_setup.sql`).
3. Storage mode: **Import** for the facts/dimensions (≈180k rows – small, fast visuals), refreshed after each
   pipeline run by the Airflow task `publish.refresh_power_bi`. Switch the facts to DirectQuery only if
   intraday freshness is needed.
4. In the Power BI service, set up a gateway connection (or the cloud Snowflake connector) with the same role.

## Model (star schema)

| Table | Source | Relationship |
|---|---|---|
| FACT_PO_LINE | CORE.FACT_PO_LINE | `promised_date` → DIM_DATE[date_day]; `supplier_id`, `sku`, `warehouse_id` → dims |
| FACT_SHIPMENT | CORE.FACT_SHIPMENT | `delivered_date` → DIM_DATE (active), `ship_date` (inactive) |
| FACT_INVENTORY_SNAPSHOT | CORE.FACT_INVENTORY_SNAPSHOT | `snapshot_date` → DIM_DATE; `sku`, `warehouse_id` → dims |
| DIM_SUPPLIER / DIM_PRODUCT / DIM_WAREHOUSE / DIM_DATE | CORE | one-to-many, single direction |
| V_DQ_SCORECARD, V_LOAD_SUMMARY | MART | disconnected (data-quality page) |

Mark DIM_DATE as the date table (`date_day`). All relationships are single-direction from dimension to fact.

## Pages

1. **Overview** – cards: OTIF %, Avg Lead Time, Spend EUR, Open PO Value; OTIF vs target line by month;
   slicers for supplier tier, country, warehouse, category.
2. **Supplier scorecard** – table with OTIF % (conditional colour from `OTIF Status Color`), On Time %, In
   Full %, lead time, spend, rank; drill-through to PO lines of a supplier.
3. **Inventory & inbound** – stock-out % and below-ROP % by warehouse (clustered bar), stock value (latest
   snapshot – semi-additive measure), late delivery % by carrier.
4. **Data quality** – checks passed %, failed checks, records processed / duplicates removed / rows rejected,
   table of the latest result per check, data freshness. Lets users see whether today's numbers can be trusted.

## Refresh automation

`scdw/powerbi.py` calls the Power BI REST API (`POST /groups/{workspace}/datasets/{dataset}/refreshes`) with a
service principal. Set `PBI_TENANT_ID`, `PBI_CLIENT_ID`, `PBI_CLIENT_SECRET`, `PBI_WORKSPACE_ID`,
`PBI_DATASET_ID` in the Airflow environment; without them the task is skipped.
