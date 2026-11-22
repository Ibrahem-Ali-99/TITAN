"""Read the published ClickHouse warehouse. Staging rows are not served as current."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import clickhouse_connect
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from titan_api.config import ApiSettings
from titan_store.models import LakePublication, PipelineRun, QualityRun, WarehouseLoad

CURRENCIES = {"USD", "EUR", "GBP"}


class WarehouseUnavailable(RuntimeError):
    pass


class LoadView(BaseModel):
    load_id: str
    status: str
    warehouse_publication_id: str
    silver_publication_id: str
    started_at: datetime
    finished_at: datetime | None
    order_count: int | None
    error_detail: str | None


class RunView(BaseModel):
    id: int
    job_name: str
    state: str
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int | None
    detail: str | None
    error_detail: str | None


class WarehouseStatus(BaseModel):
    ready: bool
    reason: str | None
    warehouse_publication_id: str | None
    silver_publication_id: str | None
    clickhouse_publication_id: str | None
    published_at: datetime | None
    freshness_seconds: int | None
    order_count: int | None
    payment_count: int | None
    inventory_count: int | None
    item_count: int | None
    accepted_revenue: dict[str, str]
    recent_loads: list[LoadView]
    recent_runs: list[RunView]


class RevenuePoint(BaseModel):
    window_start: datetime
    currency: str
    order_count: int
    accepted_revenue: str


class RevenueResponse(BaseModel):
    warehouse_publication_id: str
    silver_publication_id: str
    freshness_seconds: int
    grain: str
    points: list[RevenuePoint]


class OrderRow(BaseModel):
    order_id: str
    customer_id: str
    currency: str
    total_amount: str
    occurred_at: datetime


class OrdersResponse(BaseModel):
    warehouse_publication_id: str
    returned: int
    orders: list[OrderRow]


class ProductRow(BaseModel):
    product_id: str
    currency: str
    quantity: int
    revenue: str


class ProductsResponse(BaseModel):
    warehouse_publication_id: str
    returned: int
    products: list[ProductRow]


class AnalyticsOverview(BaseModel):
    warehouse_publication_id: str
    silver_publication_id: str
    published_at: datetime
    freshness_seconds: int
    order_count: int
    payment_count: int
    inventory_count: int
    item_count: int
    accepted_revenue: dict[str, str]
    hourly: list[RevenuePoint]
    synthetic: bool = True


def load_status(session: Session, settings: ApiSettings) -> WarehouseStatus:
    published = _published(session)
    loads = _loads(session)
    runs = _runs(session)
    clickhouse_id, error = _active_id(settings)
    if published is None:
        return _empty(loads, runs, clickhouse_id, "no warehouse publication is published")
    if error is not None:
        return _empty(loads, runs, clickhouse_id, error, published)
    if clickhouse_id != published.warehouse_publication_id:
        return _empty(
            loads,
            runs,
            clickhouse_id,
            "postgres and ClickHouse publication ids do not match",
            published,
        )
    gap = _serving_gap(session, settings, published)
    if gap is not None:
        return _empty(loads, runs, clickhouse_id, gap, published)
    return WarehouseStatus(
        ready=True,
        reason=None,
        warehouse_publication_id=published.warehouse_publication_id,
        silver_publication_id=published.silver_publication_id,
        clickhouse_publication_id=clickhouse_id,
        published_at=published.finished_at,
        freshness_seconds=_age(published.finished_at),
        order_count=published.order_count,
        payment_count=published.payment_count,
        inventory_count=published.inventory_count,
        item_count=published.item_count,
        accepted_revenue=_revenue(published.accepted_revenue_json),
        recent_loads=loads,
        recent_runs=runs,
    )


def load_overview(session: Session, settings: ApiSettings) -> AnalyticsOverview:
    published, client = _require(session, settings)
    try:
        points = _revenue_points(client, "hour", None, None, 168)
    finally:
        client.close()
    finished = published.finished_at
    if finished is None:
        raise WarehouseUnavailable("published warehouse load has no finished_at")
    return AnalyticsOverview(
        warehouse_publication_id=published.warehouse_publication_id,
        silver_publication_id=published.silver_publication_id,
        published_at=finished,
        freshness_seconds=_age(finished),
        order_count=published.order_count or 0,
        payment_count=published.payment_count or 0,
        inventory_count=published.inventory_count or 0,
        item_count=published.item_count or 0,
        accepted_revenue=_revenue(published.accepted_revenue_json),
        hourly=points,
    )


def load_revenue(
    session: Session,
    settings: ApiSettings,
    grain: str,
    currency: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> RevenueResponse:
    if grain not in {"hour", "day"}:
        raise ValueError("grain must be hour or day")
    if currency is not None and currency not in CURRENCIES:
        raise ValueError("currency must be USD, EUR, or GBP")
    published, client = _require(session, settings)
    try:
        points = _revenue_points(client, grain, currency, (start, end), limit)
    finally:
        client.close()
    finished = published.finished_at
    if finished is None:
        raise WarehouseUnavailable("published warehouse load has no finished_at")
    return RevenueResponse(
        warehouse_publication_id=published.warehouse_publication_id,
        silver_publication_id=published.silver_publication_id,
        freshness_seconds=_age(finished),
        grain=grain,
        points=points,
    )


def load_orders(
    session: Session,
    settings: ApiSettings,
    currency: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
) -> OrdersResponse:
    if currency is not None and currency not in CURRENCIES:
        raise ValueError("currency must be USD, EUR, or GBP")
    published, client = _require(session, settings)
    clauses = ["1 = 1"]
    params: dict[str, Any] = {"limit": limit}
    if currency is not None:
        clauses.append("currency = {currency:String}")
        params["currency"] = currency
    _time_clause(clauses, params, start, end)
    try:
        result = client.query(
            f"""
            select order_id, customer_id, currency, total_amount, occurred_at
            from fct_orders
            where {" and ".join(clauses)}
            order by occurred_at desc
            limit {{limit:UInt32}}
            """,
            parameters=params,
        )
        orders = [
            OrderRow(
                order_id=str(row[0]),
                customer_id=str(row[1]),
                currency=str(row[2]),
                total_amount=_plain(row[3]),
                occurred_at=_utc(row[4]),
            )
            for row in result.result_rows
        ]
    finally:
        client.close()
    return OrdersResponse(
        warehouse_publication_id=published.warehouse_publication_id,
        returned=len(orders),
        orders=orders,
    )


def load_products(
    session: Session,
    settings: ApiSettings,
    currency: str | None,
    limit: int,
) -> ProductsResponse:
    if currency is not None and currency not in CURRENCIES:
        raise ValueError("currency must be USD, EUR, or GBP")
    published, client = _require(session, settings)
    clauses = ["1 = 1"]
    params: dict[str, Any] = {"limit": limit}
    if currency is not None:
        clauses.append("currency = {currency:String}")
        params["currency"] = currency
    try:
        result = client.query(
            f"""
            select product_id, currency, quantity, revenue
            from product_sales
            where {" and ".join(clauses)}
            order by quantity desc
            limit {{limit:UInt32}}
            """,
            parameters=params,
        )
        products = [
            ProductRow(
                product_id=str(row[0]),
                currency=str(row[1]),
                quantity=int(row[2]),
                revenue=_plain(row[3]),
            )
            for row in result.result_rows
        ]
    finally:
        client.close()
    return ProductsResponse(
        warehouse_publication_id=published.warehouse_publication_id,
        returned=len(products),
        products=products,
    )


def _serving_gap(session: Session, settings: ApiSettings, published: WarehouseLoad) -> str | None:
    """Refuse to call a publication ready when its checks or fact counts are incomplete."""
    quality = session.scalar(
        select(QualityRun.status)
        .where(QualityRun.warehouse_publication_id == published.warehouse_publication_id)
        .order_by(QualityRun.started_at.desc())
        .limit(1)
    )
    if quality != "passed":
        return "the active publication has no passing quality run"
    dbt = session.scalar(
        select(PipelineRun.id)
        .where(
            PipelineRun.job_name == "warehouse_models",
            PipelineRun.state == "succeeded",
            PipelineRun.detail == published.load_id,
        )
        .limit(1)
    )
    if dbt is None:
        return "the active publication has no successful dbt run"
    silver = session.scalar(
        select(LakePublication.status).where(
            LakePublication.publication_id == published.silver_publication_id
        )
    )
    if silver != "published":
        return "the source Silver publication is not published"
    try:
        client = _client(settings)
        try:
            counts = _fact_counts(client, published.warehouse_publication_id)
        finally:
            client.close()
    except Exception:
        return "warehouse fact counts could not be read"
    expected = {
        "orders": published.order_count,
        "payments": published.payment_count,
        "inventory": published.inventory_count,
        "items": published.item_count,
    }
    if counts != expected:
        return "warehouse fact counts do not match the published load"
    return None


def _fact_counts(client: Any, publication_id: str) -> dict[str, int]:
    tables = {
        "orders": "wh_orders",
        "payments": "wh_payments",
        "inventory": "wh_inventory",
        "items": "wh_order_items",
    }
    found: dict[str, int] = {}
    for name, table in tables.items():
        result = client.query(
            f"select count() from {table} where warehouse_publication_id = {{pub:String}}",
            parameters={"pub": publication_id},
        )
        found[name] = int(result.result_rows[0][0])
    return found


def _require(session: Session, settings: ApiSettings) -> tuple[WarehouseLoad, Any]:
    status = load_status(session, settings)
    if not status.ready or status.warehouse_publication_id is None:
        raise WarehouseUnavailable(status.reason or "warehouse is not ready")
    return _published(session) or _missing(), _client(settings)


def _missing() -> WarehouseLoad:
    raise WarehouseUnavailable("warehouse is not ready")


def _published(session: Session) -> WarehouseLoad | None:
    return session.scalar(
        select(WarehouseLoad)
        .where(WarehouseLoad.status == "published", WarehouseLoad.finished_at.is_not(None))
        .order_by(WarehouseLoad.finished_at.desc())
        .limit(1)
    )


def _loads(session: Session) -> list[LoadView]:
    rows = session.scalars(
        select(WarehouseLoad).order_by(WarehouseLoad.started_at.desc()).limit(8)
    ).all()
    return [
        LoadView(
            load_id=row.load_id,
            status=row.status,
            warehouse_publication_id=row.warehouse_publication_id,
            silver_publication_id=row.silver_publication_id,
            started_at=row.started_at,
            finished_at=row.finished_at,
            order_count=row.order_count,
            error_detail=row.error_detail,
        )
        for row in rows
    ]


def _runs(session: Session) -> list[RunView]:
    rows = session.scalars(select(PipelineRun).order_by(PipelineRun.id.desc()).limit(20)).all()
    return [
        RunView(
            id=row.id,
            job_name=row.job_name,
            state=row.state,
            started_at=row.started_at,
            finished_at=row.finished_at,
            duration_ms=row.duration_ms,
            detail=row.detail,
            error_detail=row.error_detail,
        )
        for row in rows
    ]


def _client(settings: ApiSettings) -> Any:
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        send_receive_timeout=10,
    )


def _active_id(settings: ApiSettings) -> tuple[str | None, str | None]:
    try:
        client = _client(settings)
        try:
            result = client.query("select warehouse_publication_id from wh_active limit 1")
        finally:
            client.close()
    except Exception as exc:
        return None, f"clickhouse is unavailable: {exc.__class__.__name__}"
    if not result.result_rows:
        return None, None
    return str(result.result_rows[0][0]), None


def _revenue_points(
    client: Any,
    grain: str,
    currency: str | None,
    window: tuple[datetime | None, datetime | None] | None,
    limit: int,
) -> list[RevenuePoint]:
    table = "fct_revenue_hour" if grain == "hour" else "fct_revenue_day"
    stamp = "window_start" if grain == "hour" else "activity_date"
    clauses = ["1 = 1"]
    params: dict[str, Any] = {"limit": limit}
    if currency is not None:
        clauses.append("currency = {currency:String}")
        params["currency"] = currency
    if window is not None:
        _time_clause(clauses, params, window[0], window[1], stamp)
    result = client.query(
        f"""
        select {stamp}, currency, order_count, accepted_revenue
        from {table}
        where {" and ".join(clauses)}
        order by {stamp}, currency
        limit {{limit:UInt32}}
        """,
        parameters=params,
    )
    return [
        RevenuePoint(
            window_start=_utc(row[0]),
            currency=str(row[1]),
            order_count=int(row[2]),
            accepted_revenue=_plain(row[3]),
        )
        for row in result.result_rows
    ]


def _time_clause(
    clauses: list[str],
    params: dict[str, Any],
    start: datetime | None,
    end: datetime | None,
    column: str = "occurred_at",
) -> None:
    if start is not None:
        clauses.append(f"{column} >= {{start:DateTime64(6)}}")
        params["start"] = _naive(start)
    if end is not None:
        clauses.append(f"{column} < {{end:DateTime64(6)}}")
        params["end"] = _naive(end)


def _empty(
    loads: list[LoadView],
    runs: list[RunView],
    clickhouse_id: str | None,
    reason: str,
    published: WarehouseLoad | None = None,
) -> WarehouseStatus:
    return WarehouseStatus(
        ready=False,
        reason=reason,
        warehouse_publication_id=None if published is None else published.warehouse_publication_id,
        silver_publication_id=None if published is None else published.silver_publication_id,
        clickhouse_publication_id=clickhouse_id,
        published_at=None if published is None else published.finished_at,
        freshness_seconds=None,
        order_count=None,
        payment_count=None,
        inventory_count=None,
        item_count=None,
        accepted_revenue={},
        recent_loads=loads,
        recent_runs=runs,
    )


def _revenue(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        return {}
    return {str(key): str(value) for key, value in parsed.items()}


def _age(finished: datetime | None) -> int | None:
    if finished is None:
        return None
    moment = finished if finished.tzinfo is not None else finished.replace(tzinfo=UTC)
    return max(0, int((datetime.now(UTC) - moment).total_seconds()))


def _naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _plain(value: object) -> str:
    return format(Decimal(str(value)).quantize(Decimal("0.01")), "f")


def _utc(value: datetime | date) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return datetime(value.year, value.month, value.day, tzinfo=UTC)
