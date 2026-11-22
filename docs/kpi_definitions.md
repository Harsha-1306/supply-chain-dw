# KPI definitions

Same definitions in SQL (`scdw/sql/mart/01_kpi_views.sql`) and DAX (`powerbi/measures.dax`).

| KPI | Definition | Grain / filter |
|---|---|---|
| **On time** | Last delivery date ≤ promised date | Closed PO lines (fully received, nothing in transit), by promised month |
| **In full** | Delivered quantity ≥ ordered quantity | Closed PO lines |
| **OTIF** | On time **and** in full | Closed PO lines; target 90 % |
| **Lead time** | Days from order date to last delivery | Closed PO lines, average |
| **Spend** | Ordered qty × unit price × FX rate to EUR | All PO lines |
| **Supplier rating** | A: OTIF ≥ 90 %, B: ≥ 75 %, C: below | Per supplier, all closed lines |
| **Late delivery rate** | Delivered after the PO line's promised date | Delivered shipments with a known PO |
| **Transit time** | Days from ship date to delivery | Delivered shipments |
| **Stock-out rate** | Share of SKU-warehouse positions with 0 on hand | Daily snapshots |
| **Below reorder point** | On hand < reorder point | Daily snapshots |
| **Stock value** | On hand × unit cost (latest snapshot in the period – semi-additive) | Daily snapshots |
