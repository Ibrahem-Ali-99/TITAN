# Three-minute demo script

1. Open the overview. Say that the amber label means the commerce data is synthetic, and that the ingestion cards are PostgreSQL rows rather than the published warehouse.
2. Open Data Sources. Point at REST and CSV as the intake, and say resume is closed unless the admin token is set.
3. Open Streaming. Show quarantine and the conflict counter. Say offsets commit after the database write, and a repeated event id does not become a second order.
4. Open Warehouse. Read the publication id and the three currency totals. Say the currencies are not summed.
5. Open Data Quality. Show a passed gate and, if it is on screen, the injected failure that did not become the active publication.
6. Open ML & Predictions. Read `seasonal_naive`. Say the gradient-boosting MAE was higher on the same held-out week, and that run is still stored as evaluated.
7. Open Monitoring. Separate consumer lag from the Spark checkpoint line, and point at a real pipeline row.
8. If the throughput panels are empty, run `uv run python -m scripts.demo_traffic --orders 8 --interval 6` to publish unique order/payment/inventory cycles across current minute buckets. For the separate publication walkthrough, run `uv run python -m scripts.demo_day7` and refresh the warehouse page when it prints a new publication id.

Do not describe the 100-event or 5,000-event samples as sustained pipeline throughput.
