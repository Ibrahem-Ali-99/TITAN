# ClickHouse disaster recovery (Upgrade 4D)

TITAN uses a verified rebuild strategy rather than treating ClickHouse data or
`wh_active` as the authority. PostgreSQL `warehouse_loads` and
`lake_publications` provide publication and lineage metadata; MinIO Silver
Parquet is the verified analytical input; ClickHouse facts and the serving pointer
are reconstructible projections. Native ClickHouse volume restore is not the
default because it can restore stale or incomplete serving metadata without the
PostgreSQL publication and quality-gate context.

`scripts/clickhouse_dr_recovery.py` runs in an isolated full-pipeline Compose
project. It first proves an interrupted candidate does not change `wh_active`,
empties the disposable ClickHouse tables, rebuilds from the selected published
Silver version through the normal warehouse loader/dbt/quality path, and verifies
fact counts, distinct business data, currency-separated revenue, and Silver
lineage. It then repeats the rebuild to prove no duplicate facts are introduced.
Only a validated candidate is promoted through the existing atomic staging-table
exchange. A failed or incompatible candidate remains inactive.

Recovery requires a compatible ClickHouse schema, PostgreSQL publication metadata,
and intact verified MinIO Silver objects. Spark checkpoints, Kafka offsets,
PostgreSQL event tables, and MinIO recovery are coordinated dependencies, not
part of this ClickHouse phase. This is a logical rebuild with an RPO bounded by
the available Silver publication and an RTO dependent on Parquet size, dbt, and
ClickHouse load time; it is not point-in-time ClickHouse recovery.

Machine-readable evidence is stored in `benchmarks/clickhouse-dr.json`.
