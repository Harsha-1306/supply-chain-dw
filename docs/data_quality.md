# Data-quality checks

Every result is written to `AUDIT.DQ_RESULTS` and shown on the Power BI data-quality page
(`MART.V_DQ_SCORECARD`). **Critical** checks stop the Airflow run (`AirflowFailException`, no retries – bad data
does not fix itself); **warnings** are recorded and visible but do not block.

| Stage | Check | Severity | Rule |
|---|---|---|---|
| Before load | `schema` | critical | Each CSV header contains every contract column, no duplicate headers. Extra columns → warning (ignored). Checked for the whole batch before anything is loaded. |
| Staging | `reject_rate` | critical | Rows failing type / required / min / allowed-value rules are moved to `STAGING.REJECTED_ROWS` with a reason. Above the contract's `max_reject_rate` (2–5 %) the batch is stopped before MERGE. |
| Staging | `duplicates_in_batch` | warning | Counts exact duplicate rows and duplicate business keys in the batch; resolved by keeping the newest version. |
| Staging | `unique_primary_key` | critical | After MERGE, exactly one row per business key in STAGING. |
| Staging | `reconciliation` | critical | Every valid key of the batch is present in STAGING after MERGE. Per table: `raw = merged keys + duplicate rows + rejected rows`. |
| Post-load | `orphan_supplier` | warn > 0 %, fail > 1 % | PO lines whose supplier is not in master data (mapped to `UNKNOWN`). |
| Post-load | `orphan_shipment` | warn > 0 %, fail > 1 % | Shipments referencing an unknown PO line. |
| Post-load | `delivered_before_shipped` | fail > 0.1 % | Delivery date earlier than ship date. |
| Post-load | `over_delivery` | fail > 1 % | Delivered quantity > 110 % of ordered. |
| Post-load | `fact_null_keys` | critical | No NULL keys in any fact table. |
| Post-load | `volume` | warning | Validated records in STAGING ≥ 100,000. |

## Result of the reference run (185,477 generated records)

| Table | Raw rows | Exact dup rows | Duplicate keys resolved | Rejected | Loaded keys |
|---|---:|---:|---:|---:|---:|
| purchase_orders | 55,990 | 550 | 825 | 165 | 55,000 |
| shipments | 61,576 | 600 | 1,200 | 180 | 60,196 |
| inventory_snapshots | 65,124 | 324 | 324 | 0 | 64,800 |
| products | 2,525 | 25 | 25 | 0 | 2,500 |
| suppliers / warehouses | 262 | 0 | 0 | 0 | 262 |

The test-suite asserts these numbers against what the generator injected (exact duplicates, superseded
versions, invalid rows, orphans, schema drift), so the checks are verified to catch every planted problem.
