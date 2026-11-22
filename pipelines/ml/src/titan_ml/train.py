"""Train a one-step product demand model from the active ClickHouse publication.

Evaluation rows use actual earlier days as lags. The forward week is recursive and
does not read actual quantities from those future dates.
"""

import io
import json
import os
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import uuid4

import clickhouse_connect
import joblib
import psycopg
from minio import Minio
from sklearn.ensemble import HistGradientBoostingRegressor

from titan_ml.features import (
    FEATURE_VERSION,
    anomaly_scores,
    complete_panel,
    feature_rows,
    mae,
    seasonal_naive,
    split_rows,
    wape,
)
from titan_ml.selection import choose_strategy, recursive_seasonal

HYPERPARAMETERS = {
    "max_depth": 3,
    "learning_rate": 0.1,
    "max_iter": 80,
    "random_state": 51022,
}


def train_demand_model() -> str:
    database_url = _database_url()
    client = _clickhouse()
    try:
        publication = _active_publication(client)
        raw = _demand_rows(client)
    finally:
        client.close()
    panel = complete_panel(raw)
    rows = feature_rows(panel)
    run_id = str(uuid4())
    try:
        train, test = split_rows(rows)
    except ValueError as exc:
        _insert_run(database_url, run_id, publication, "failed", None, None, None, str(exc))
        raise
    model, codes = _fit(train)
    naive = seasonal_naive(test)
    predicted = _predict(model, codes, test)
    actual = [float(row["target"]) for row in test]
    model_mae = mae(actual, predicted)
    naive_mae = mae(actual, naive)
    choice = choose_strategy(model_mae, naive_mae)
    metrics = {
        "mae_model": round(model_mae, 6),
        "mae_seasonal_naive": round(naive_mae, 6),
        "wape_model": _round(wape(actual, predicted)),
        "wape_seasonal_naive": _round(wape(actual, naive)),
        "test_rows": len(test),
        "train_rows": len(train),
        "better_than_baseline": model_mae < naive_mae,
        "serving_strategy": choice.strategy,
        "split": "chronological last 7 dates; one-step features",
    }
    artifact = _store_artifact(run_id, model, codes)
    train_dates = [row["activity_date"] for row in train if isinstance(row["activity_date"], date)]
    test_dates = [row["activity_date"] for row in test if isinstance(row["activity_date"], date)]
    served = naive if choice.strategy == "seasonal_naive" else predicted
    _insert_run(
        database_url,
        run_id,
        publication,
        "serving",
        metrics,
        artifact,
        (min(train_dates), max(train_dates), min(test_dates), max(test_dates)),
        choice.rationale,
        serving_strategy=choice.strategy,
        decision_rationale=choice.rationale,
    )
    _retire_previous(database_url, run_id)
    _store_predictions(database_url, run_id, test, served, "one_step_evaluation")
    _store_predictions(database_url, run_id, test, predicted, "candidate_one_step")
    if choice.strategy == "seasonal_naive":
        _store_forward_naive(database_url, run_id, panel)
    else:
        _store_forward(database_url, run_id, model, codes, panel)
    _store_anomalies(database_url, run_id, panel)
    return run_id


def _fit(
    train: list[dict[str, object]],
) -> tuple[HistGradientBoostingRegressor, dict[str, int]]:
    seen: dict[str, int] = {}
    for row in train:
        product_id = str(row["product_id"])
        if product_id not in seen:
            seen[product_id] = len(seen)
    model = HistGradientBoostingRegressor(**HYPERPARAMETERS)
    model.fit(_matrix(train, seen), [float(row["target"]) for row in train])
    return model, seen


def _predict(
    model: HistGradientBoostingRegressor,
    codes: dict[str, int],
    rows: list[dict[str, object]],
) -> list[float]:
    values = model.predict(_matrix(rows, codes))
    return [max(0.0, float(value)) for value in values]


def _matrix(rows: list[dict[str, object]], codes: dict[str, int]) -> list[list[float]]:
    matrix = []
    for row in rows:
        product_id = str(row["product_id"])
        matrix.append(
            [
                float(row["lag_1"]),
                float(row["lag_7"]),
                float(row["rolling_7"]),
                float(row["day_of_week"]),
                float(codes.get(product_id, -1)),
            ]
        )
    return matrix


def _store_forward(
    database_url: str,
    run_id: str,
    model: HistGradientBoostingRegressor,
    codes: dict[str, int],
    panel: list[tuple[str, date, int]],
) -> None:
    """Walk seven days past the last actual date. Predictions feed the next lags."""
    by_product: dict[str, list[tuple[date, int]]] = {}
    for product_id, activity_date, quantity in panel:
        by_product.setdefault(product_id, []).append((activity_date, quantity))
    now = datetime.now(UTC)
    with psycopg.connect(database_url) as connection:
        for product_id, series in by_product.items():
            series = sorted(series)
            quantities = [quantity for _, quantity in series]
            cursor = series[-1][0]
            for _step in range(7):
                cursor += timedelta(days=1)
                history = quantities
                lag_1 = history[-1] if history else 0
                lag_7 = history[-7] if len(history) >= 7 else 0
                window = history[-7:]
                rolling = sum(window) / len(window) if window else 0.0
                row = {
                    "product_id": product_id,
                    "lag_1": lag_1,
                    "lag_7": lag_7,
                    "rolling_7": rolling,
                    "day_of_week": cursor.weekday(),
                }
                predicted = _predict(model, codes, [row])[0]
                quantities.append(int(round(predicted)))
                connection.execute(
                    """
                    insert into ml_predictions (
                        run_id, product_id, target_date, predicted_quantity,
                        actual_quantity, kind, generated_at
                    ) values (%s, %s, %s, %s, null, 'recursive_forecast', %s)
                    """,
                    (run_id, product_id, cursor.isoformat(), f"{predicted:.3f}", now),
                )
        connection.commit()


def _store_forward_naive(
    database_url: str,
    run_id: str,
    panel: list[tuple[str, date, int]],
) -> None:
    """Seven recursive seasonal steps. Each step reads the prior prediction, not a future actual."""
    by_product: dict[str, list[tuple[date, int]]] = {}
    for product_id, activity_date, quantity in panel:
        by_product.setdefault(product_id, []).append((activity_date, quantity))
    now = datetime.now(UTC)
    with psycopg.connect(database_url) as connection:
        for product_id, series in by_product.items():
            series = sorted(series)
            forecasts = recursive_seasonal([float(quantity) for _, quantity in series], 7)
            cursor = series[-1][0]
            for predicted in forecasts:
                cursor += timedelta(days=1)
                connection.execute(
                    """
                    insert into ml_predictions (
                        run_id, product_id, target_date, predicted_quantity,
                        actual_quantity, kind, generated_at
                    ) values (%s, %s, %s, %s, null, 'recursive_forecast', %s)
                    """,
                    (run_id, product_id, cursor.isoformat(), f"{predicted:.3f}", now),
                )
        connection.commit()


def _store_predictions(
    database_url: str,
    run_id: str,
    test: list[dict[str, object]],
    predicted: list[float],
    kind: str,
) -> None:
    now = datetime.now(UTC)
    with psycopg.connect(database_url) as connection:
        for row, value in zip(test, predicted, strict=True):
            activity_date = row["activity_date"]
            if not isinstance(activity_date, date):
                continue
            connection.execute(
                """
                insert into ml_predictions (
                    run_id, product_id, target_date, predicted_quantity,
                    actual_quantity, kind, generated_at
                ) values (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    row["product_id"],
                    activity_date.isoformat(),
                    f"{value:.3f}",
                    str(int(row["target"])),
                    kind,
                    now,
                ),
            )
        connection.commit()


def _store_anomalies(
    database_url: str,
    run_id: str,
    panel: list[tuple[str, date, int]],
) -> None:
    with psycopg.connect(database_url) as connection:
        for row in anomaly_scores(panel):
            connection.execute(
                """
                insert into ml_anomalies (
                    run_id, product_id, activity_date, observed, baseline,
                    score, flagged, method, detail
                ) values (%s, %s, %s, %s, %s, %s, %s, 'rolling_median_mad', %s)
                """,
                (
                    run_id,
                    row["product_id"],
                    row["activity_date"],
                    row["observed"],
                    row["baseline"],
                    None if row["score"] is None else f"{float(row['score']):.3f}",
                    1 if row["flagged"] else 0,
                    row["detail"],
                ),
            )
        connection.commit()


def _insert_run(
    database_url: str,
    run_id: str,
    publication: str,
    status: str,
    metrics: dict[str, object] | None,
    artifact: str | None,
    windows: tuple[date, date, date, date] | None,
    detail: str | None,
    serving_strategy: str | None = None,
    decision_rationale: str | None = None,
) -> None:
    train_start = test_start = train_end = test_end = None
    if windows is not None:
        train_start, train_end, test_start, test_end = (item.isoformat() for item in windows)
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            insert into ml_runs (
                run_id, status, warehouse_publication_id, algorithm, hyperparameters_json,
                feature_version, train_start, train_end, test_start, test_end,
                metrics_json, artifact_uri, trained_at, detail,
                serving_strategy, decision_rationale
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                status,
                publication,
                "HistGradientBoostingRegressor",
                json.dumps(HYPERPARAMETERS),
                FEATURE_VERSION,
                train_start,
                train_end,
                test_start,
                test_end,
                None if metrics is None else json.dumps(metrics),
                artifact,
                datetime.now(UTC),
                detail,
                serving_strategy,
                decision_rationale,
            ),
        )
        connection.commit()


def _retire_previous(database_url: str, run_id: str) -> None:
    """Keep the previous decision row and its metrics. Do not delete it."""
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            update ml_runs
            set status = 'evaluated',
                detail = coalesce(detail, '') || ' Retained after a later serving decision.'
            where status in ('promoted', 'serving') and run_id <> %s
            """,
            (run_id,),
        )
        connection.commit()


def _store_artifact(
    run_id: str,
    model: HistGradientBoostingRegressor,
    codes: dict[str, int],
) -> str:
    buffer = io.BytesIO()
    joblib.dump(
        {"model": model, "product_codes": codes, "feature_version": FEATURE_VERSION},
        buffer,
    )
    payload = buffer.getvalue()
    endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
    host = endpoint.removeprefix("https://").removeprefix("http://")
    client = Minio(
        host,
        access_key=os.environ.get("S3_ACCESS_KEY", os.environ.get("MINIO_ROOT_USER", "titan")),
        secret_key=os.environ.get(
            "S3_SECRET_KEY",
            os.environ.get("MINIO_ROOT_PASSWORD", "titan_dev_password"),
        ),
        secure=endpoint.startswith("https://"),
    )
    bucket = os.environ.get("LAKE_BUCKET", "titan-lake")
    key = f"models/{run_id}/model.joblib"
    client.put_object(bucket, key, io.BytesIO(payload), length=len(payload))
    return f"s3://{bucket}/{key}"


def _demand_rows(client: Any) -> list[tuple[str, date, int]]:
    result = client.query(
        """
        select product_id, toDate(occurred_at) as activity_date, sum(quantity) as quantity
        from fct_order_items
        group by product_id, activity_date
        """
    )
    rows = []
    for product_id, activity_date, quantity in result.result_rows:
        if isinstance(activity_date, datetime):
            activity_date = activity_date.date()
        rows.append((str(product_id), activity_date, int(quantity)))
    return rows


def _active_publication(client: Any) -> str:
    result = client.query("select warehouse_publication_id from wh_active limit 1")
    if not result.result_rows:
        raise RuntimeError("no active warehouse publication")
    return str(result.result_rows[0][0])


def _clickhouse() -> Any:
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "titan"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "titan_dev_password"),
        database=os.environ.get("CLICKHOUSE_DATABASE", "titan"),
    )


def _database_url() -> str:
    url = os.environ.get(
        "DATABASE_URL", "postgresql://titan:titan_dev_password@localhost:5435/titan"
    )
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 6)


def main() -> None:
    print(train_demand_model())


if __name__ == "__main__":
    main()
