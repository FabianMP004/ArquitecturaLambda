import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────

INFLUX_URL   = "http://localhost:8086"
INFLUX_TOKEN = "ZY-PlYbVQq8ZoqEliZ9qv9i-Ptso9Uc7j-j-j4qCs1CAktFBaVcsT_UHIScRwahmTzTp2VWqEDoEhQYQDTioxw=="
INFLUX_ORG   = "lambda_org"
INFLUX_BUCKET = "lambda_batch"

PROCESSED_DIR = Path("datalake/processed")

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("serving_layer")


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _parse_day(day_str: str) -> datetime:
    """'2026-02-25'  →  UTC datetime."""
    return datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _parse_hour(hour_str: str) -> datetime:
    """'2026-02-25 14:00:00'  →  UTC datetime."""
    return datetime.strptime(hour_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def read_parquet(name: str) -> pd.DataFrame | None:
    path = PROCESSED_DIR / name
    if not path.exists():
        log.warning("Parquet folder not found, skipping: %s", path)
        return None
    try:
        df = pd.read_parquet(path)
        log.info("Read %d rows from %s", len(df), name)
        return df
    except Exception:
        log.exception("Failed to read parquet: %s", name)
        return None

def points_revenue_per_day(df: pd.DataFrame) -> list[Point]:
    points = []
    for _, row in df.iterrows():
        ts = _parse_day(str(row["day"]))
        p = (Point("revenue_per_day")
             .field("revenue", float(row["revenue_dia"]))
             .field("num_orders", int(row["num_orders"]))
             .time(ts, WritePrecision.S))
        points.append(p)
    return points


def points_revenue_per_hour(df: pd.DataFrame) -> list[Point]:
    points = []
    for _, row in df.iterrows():
        ts = _parse_hour(str(row["hour"]))
        p = (Point("revenue_per_hour")
             .field("revenue", float(row["revenue_hora"]))
             .field("num_orders", int(row["num_orders"]))
             .time(ts, WritePrecision.S))
        points.append(p)
    return points


def points_events_per_day(df: pd.DataFrame) -> list[Point]:
    points = []
    for _, row in df.iterrows():
        ts = _parse_day(str(row["day"]))
        p = (Point("events_per_day")
             .field("event_count", int(row["eventos_por_dia"]))
             .time(ts, WritePrecision.S))
        points.append(p)
    return points


def points_events_per_hour(df: pd.DataFrame) -> list[Point]:
    points = []
    for _, row in df.iterrows():
        ts = _parse_hour(str(row["hour"]))
        p = (Point("events_per_hour")
             .field("event_count", int(row["eventos_por_hora"]))
             .time(ts, WritePrecision.S))
        points.append(p)
    return points


def points_events_by_type(df: pd.DataFrame) -> list[Point]:
    ts = _now()
    points = []
    for _, row in df.iterrows():
        p = (Point("events_by_type")
             .tag("event_type", str(row["event_type"]))
             .field("total", int(row["total_eventos"]))
             .time(ts, WritePrecision.S))
        points.append(p)
    return points


def points_top_viewed_products(df: pd.DataFrame) -> list[Point]:
    ts = _now()
    return [
        Point("top_viewed_products")
        .tag("product_id", str(int(row["product_id"])))
        .field("views", int(row["views"]))
        .time(ts, WritePrecision.S)
        for _, row in df.iterrows()
    ]


def points_top_viewed_per_hour(df: pd.DataFrame) -> list[Point]:
    points = []
    for _, row in df.iterrows():
        ts = _parse_hour(str(row["hour"]))
        p = (Point("top_viewed_per_hour")
             .tag("product_id", str(int(row["product_id"])))
             .field("views", int(row["views"]))
             .time(ts, WritePrecision.S))
        points.append(p)
    return points


def points_top_products_by_qty(df: pd.DataFrame) -> list[Point]:
    ts = _now()
    return [
        Point("top_products_by_qty")
        .tag("product_id", str(int(row["product_id"])))
        .field("total_qty", float(row["total_qty"]))
        .time(ts, WritePrecision.S)
        for _, row in df.iterrows()
    ]


def points_aov(df: pd.DataFrame) -> list[Point]:
    ts = _now()
    return [
        Point("aov")
        .field("avg_order_value", float(row["avg_order_value"]))
        .time(ts, WritePrecision.S)
        for _, row in df.iterrows()
    ]


def points_avg_items_per_order(df: pd.DataFrame) -> list[Point]:
    ts = _now()
    return [
        Point("avg_items_per_order")
        .field("avg_items", float(row["avg_items_per_order"]))
        .time(ts, WritePrecision.S)
        for _, row in df.iterrows()
    ]


def points_top_users(df: pd.DataFrame) -> list[Point]:
    ts = _now()
    return [
        Point("top_users")
        .tag("user_id", str(int(row["user_id"])))
        .field("total_events", int(row["total_eventos"]))
        .time(ts, WritePrecision.S)
        for _, row in df.iterrows()
    ]


def points_funnel_counts(df: pd.DataFrame) -> list[Point]:
    ts = _now()
    return [
        Point("funnel_counts")
        .tag("stage", str(row["stage"]))
        .field("count", int(row["count"]))
        .time(ts, WritePrecision.S)
        for _, row in df.iterrows()
    ]


def points_funnel_ratios(df: pd.DataFrame) -> list[Point]:
    ts = _now()
    return [
        Point("funnel_ratios")
        .tag("ratio", str(row["ratio"]))
        .field("value", float(row["value"]))
        .time(ts, WritePrecision.S)
        for _, row in df.iterrows()
    ]


# ──────────────────────────────────────────────
# METRIC REGISTRY
# Maps parquet folder name → writer function
# ──────────────────────────────────────────────

METRICS: dict[str, callable] = {
    "revenue_per_day":      points_revenue_per_day,
    "revenue_per_hour":     points_revenue_per_hour,
    "events_per_day":       points_events_per_day,
    "events_per_hour":      points_events_per_hour,
    "events_by_type":       points_events_by_type,
    "top_viewed_products":  points_top_viewed_products,
    "top_viewed_per_hour":  points_top_viewed_per_hour,
    "top_products_by_qty":  points_top_products_by_qty,
    "aov":                  points_aov,
    "avg_items_per_order":  points_avg_items_per_order,
    "top_users":            points_top_users,
    "funnel_counts":        points_funnel_counts,
    "funnel_ratios":        points_funnel_ratios,
}


# ──────────────────────────────────────────────
# BUCKET BOOTSTRAP
# ──────────────────────────────────────────────

def ensure_bucket(client: InfluxDBClient):
    """Create lambda_batch bucket if it doesn't exist."""
    buckets_api = client.buckets_api()
    existing = [b.name for b in buckets_api.find_buckets().buckets]
    if INFLUX_BUCKET not in existing:
        org = client.organizations_api().find_organizations(org=INFLUX_ORG)[0]
        buckets_api.create_bucket(bucket_name=INFLUX_BUCKET, org_id=org.id)
        log.info("Created InfluxDB bucket: %s", INFLUX_BUCKET)
    else:
        log.info("Bucket already exists: %s", INFLUX_BUCKET)


# ──────────────────────────────────────────────
# MAIN SYNC LOOP
# ──────────────────────────────────────────────

def run_sync():
    log.info("=== Serving Layer sync started ===")
    log.info("Source : %s", PROCESSED_DIR.resolve())
    log.info("Target : %s / bucket=%s", INFLUX_URL, INFLUX_BUCKET)

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    ensure_bucket(client)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    total_points = 0
    errors = 0

    for metric_name, builder_fn in METRICS.items():
        df = read_parquet(metric_name)
        if df is None or df.empty:
            continue

        try:
            points = builder_fn(df)
            if not points:
                log.warning("No points generated for: %s", metric_name)
                continue

            write_api.write(bucket=INFLUX_BUCKET, record=points)
            log.info("  ✓ %-30s → %d points written", metric_name, len(points))
            total_points += len(points)

        except InfluxDBError as e:
            log.error("  ✗ InfluxDB error for %s: %s", metric_name, e)
            errors += 1
        except Exception:
            log.exception("  ✗ Unexpected error for: %s", metric_name)
            errors += 1

    client.close()
    log.info("=== Sync complete: %d points written, %d errors ===", total_points, errors)


if __name__ == "__main__":
    run_sync()