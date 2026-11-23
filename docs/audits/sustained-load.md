# TITAN Sustained Load & Reliability Benchmark

Measured 2026-11-23 against the isolated `titan-clone` Compose project. The development project and its `titan_*` volumes were not stopped, reset, or modified.

## Isolation

The benchmark used project name `titan-clone`, ports 28000/29092/25435/28123/29000, `titan-clone_*` volumes, clone Kafka topics and consumer group, clone MinIO prefixes, and clone Spark checkpoints. All six long-running clone services were healthy before downstream processing. No GPU was used: this workload is Kafka, SQL, and I/O bound.

## Workload and method

Reusable runner: `scripts/sustained_load.py`. It uses deterministic `generate_orders` events, deterministic event IDs, one explicit Kafka acknowledgement per event, pacing to the requested rate, direct PostgreSQL durable-row reconciliation, and 0.5-second API lag sampling. The run used seed `20261123`, 10-second phases at 10, 50, and 100 events/s. The complete raw phase evidence is `benchmarks/sustained-load-2026-11-23.json`.

Producer acknowledgement is not treated as persistence. Each phase waited for the expected durable `commerce_events` count and recorded the observed delta.

## Results

| Target | Attempted / acknowledged | Producer elapsed | Ack p50 / p95 / p99 | Durable delta | Max lag | Final lag |
|---:|---:|---:|---:|---:|---:|---:|
| 10/s | 100 / 100 | 10.001 s | 42.7 / 46.8 / 49.1 ms | 70 within 90 s wait | 0 | 0 |
| 50/s | 500 / 500 | 23.156 s | 44.1 / 48.2 / 49.5 ms | 500 | 1 | 0 |
| 100/s | 1,000 / 1,000 | 10.001 s | 0.9 / 1.3 / 2.4 ms | 1,000 | 1 | 0 |

The original 10/s result was a measurement defect, not loss. The phase reused seed `20261123` after 30 of its 100 deterministic event IDs already existed from an earlier overlapping attempt. Therefore the row-count delta was 70 even though all 100 acknowledgements were accounted for; the original runner incorrectly assumed every acknowledgement must increase `commerce_events`. The corrected runner tracks each event ID before and after the phase and reports preexisting, newly persisted, and unaccounted IDs separately. No escalation beyond 100/s was attempted.

## Persistence-anomaly investigation

Direct PostgreSQL lookup of all 100 original phase IDs found 100/100 durable `commerce_events` rows. The first 30 had `ingested_at` around 12:30:41Z, before the 13:12 phase; the remaining 70 were ingested during the phase around 13:12:13–20Z. Their final disposition was accepted. None were quarantined, lost, or merely represented by a duplicate-only outcome.

The isolated Kafka topic audit showed order-event watermarks and committed `titan-ingestion` offsets equal at the end of the investigation: partitions 0/1/2 were `(737, 679, 726)` for both high watermark and committed offset, so lag was zero. The Kafka CLI inside the container could not be used because the clone advertises its host listener; the non-mutating host `confluent_kafka` audit read the same offsets successfully.

The corrected runner performs `/health/ready` and monitoring-lag readiness checks before producing, then queries the exact event IDs rather than using a total-row target. It also records signed acknowledgement-to-`ingested_at` latency. A negative value means the consumer committed the database row before the producer callback completed; it is not a loss or a clock-adjustment claim.

Corrected 10/s repetitions, each 100 unique acknowledged IDs:

| Condition | Seed | Preexisting | Newly persisted | Accounted | Ack p50/p95/p99 | Ack-to-persistence p50/p95/p99 | Max/final lag |
|---|---:|---:|---:|---:|---:|---:|---:|
| Warm, reused IDs | 20261124 | 100 | 0 | 100 | 41.5/47.9/54.1 ms | n/a | 1/0 |
| Cold, ingestion restarted and healthy | 20261201 | 0 | 100 | 100 | 44.9/49.0/54.5 ms | -0.51/0.20/0.40 ms | 1/0 |
| Warm, unique IDs | 20261202 | 0 | 100 | 100 | 1.15/1.47/7.21 ms | 0.25/3.21/12.19 ms | 5/0 |

The raw corrected runs are `benchmarks/sustained-load-warm-2026-11-23.json`, `benchmarks/sustained-load-cold-2026-11-23.json`, and `benchmarks/sustained-load-warm2-2026-11-23.json`. These results account for every acknowledged event ID; there is no confirmed event loss.

## Downstream timings

After ingestion settled, isolated Spark and warehouse jobs were run against the clone:

- Bronze available-now processing: 20.405 s; checkpoint publication succeeded.
- Silver available-now processing: 15.045 s; publication id `fadf5451-daa9-4a8c-a7ea-d10c1315add5`.
- Warehouse publication: 16.309 s for the first successful post-load publication; 18.557 s for the final republish after the duplicate replay. Publication id `a874fb3d-3669-4bca-8082-3b98d06997bc`.

The first two publication attempts failed before mutation because the benchmark shell supplied the wrong ClickHouse credentials and a repository-local dbt log path with an access error. The retry used the clone credentials and a temporary Windows log directory; it published successfully. These are benchmark-environment setup failures, not data-integrity failures.

## Resource snapshot

Read-only `docker stats --no-stream` after the sustained run:

| Service | CPU | Memory | Network I/O | Block I/O |
|---|---:|---:|---:|---:|
| Kafka | 154.83% | 513.7 MiB / 768 MiB | 7.65 / 8.65 MB | 77.5 / 41.1 MB |
| ClickHouse | 9.75% | 281.8 MiB / 1.5 GiB | 3.04 / 33.9 MB | 127 / 1.47 GB |
| API | 4.62% | 111.7 MiB / 512 MiB | 37.7 / 6.88 MB | 4.6 MB / 0 |
| Ingestion | 0.08% | 59.85 MiB / 512 MiB | 7.85 / 6.72 MB | 21.3 / 25.2 MB |
| PostgreSQL | 0.30% | 44.43 MiB / 512 MiB | 5.76 / 9.57 MB | 17.7 / 21.3 MB |
| MinIO | 1.40% | 80.59 MiB / 256 MiB | 851 KB / 2.34 MB | 22.1 / 3.29 MB |

This is a point-in-time snapshot, not a peak profile. The observed bottleneck was Kafka CPU during the per-event producer/flush workload; no resource limit was exhausted.

## Correctness and recovery

The original final state was 2,145 persisted commerce events and 1,788 accepted logical orders. After the investigation repetitions, final clone state was 2,345 persisted commerce events, 1 quarantined record, 1 order conflict, 0 duplicate operations, 152 duplicate-delivery observations, 1,988 accepted logical orders, and ingestion lag 0. The explicit duplicate replay and warm reused-ID run added observations but no duplicate business rows.

The final warehouse publication `5103f03d-2342-4c15-bfd5-0ab8690c5051` was ready with 1,988 orders, 187 payments, 169 inventory updates, and currency-separated accepted revenue: EUR 131,220.81; GBP 113,651.45; USD 134,149.68. It carried the refreshed Silver publication and passed the serving quality gate. No partial publication became active.

The earlier “576 Bronze offsets versus 574 persisted events” difference is explained by source populations, not unexplained loss: 574 persisted commerce rows plus 1 quarantined malformed record plus 1 duplicate-delivery observation equals 576 durable consumer outcomes. A duplicate delivery is an observation of a repeated event ID, not a second business row; quarantine is retained evidence and is not applied as a business event.

## Limitations and decision

The run used 10-second bounded phases and a single workstation, so it does not establish long-duration capacity, peak resource usage, or production-scale failure rates. API request latency was not sampled as a separate series by this first runner revision; monitoring requests remained responsive and the API container stayed healthy. No defect in idempotency, lag recovery, publication atomicity, or currency reconciliation was found, so no production code optimization was justified by this test.

Reproduce with:

```powershell
$env:PYTHONPATH='scripts;services/event-generator/src;packages/contracts/src;packages/messaging/src;packages/store/src'
$env:API_BASE_URL='http://localhost:28000'
$env:KAFKA_BOOTSTRAP_SERVERS='localhost:29092'
$env:DATABASE_URL='postgresql://titan:titan_dev_password@127.0.0.1:25435/titan'
.venv\Scripts\python.exe scripts\sustained_load.py --rates 10 50 100 --duration-seconds 10
```
