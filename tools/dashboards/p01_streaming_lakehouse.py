"""Project 01: Real-Time Streaming Lakehouse (ride-hailing events)."""

import math

from common import bump24, col, five_min_labels, hour_labels, pct_change, quantile, r0, r1, r2, r4, status

META = dict(
    num="01",
    slug="streaming-lakehouse",
    seed=101,
    title="Real-Time Streaming Lakehouse",
    short="Streaming Lakehouse",
    category="Real-time streaming",
    tagline="Ride-hailing events streamed from Kafka through Spark Structured Streaming into Apache Iceberg on S3, queryable within seconds.",
    tile_stack="Kafka · Spark Structured Streaming · Iceberg · Airflow",
    summary=(
        "Rider and driver apps publish trip requests, location pings, trip state changes and payments to Kafka. "
        "A Spark Structured Streaming job reads them in 10-second micro-batches, handles late events with a watermark "
        "and writes exactly once into Apache Iceberg tables on Amazon S3, so analysts can query a trip seconds after it happens. "
        "Airflow keeps the tables healthy with hourly compaction and snapshot expiry."
    ),
    stack=["Apache Kafka", "Spark Structured Streaming", "Apache Iceberg", "Amazon S3", "Apache Airflow"],
    window="Tuesday, Oct 6, 2026 · 24 hours, US Central",
    csv="trips_5min.csv",
    csv_desc="Trips, fares, events and freshness in 5-minute intervals",
    how_note="From an app event to a queryable Iceberg row in about 10 seconds at p95.",
    model_note="Three Iceberg tables in the `rides` namespace. The CSV download is the 5-minute aggregate behind the charts.",
    code_note="The streaming job, the table definitions and the Airflow DAG that maintains the tables.",
)

ZONES = ["Downtown", "Airport", "University", "Midtown", "Riverside", "Old Town", "Harbor", "Tech Park", "Stadium", "Lakeside"]
ZONE_SHARE = [0.141, 0.101, 0.085, 0.082, 0.071, 0.068, 0.06, 0.056, 0.048, 0.042]
DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def weekday_profile(h):
    """Completed trips per 5 minutes on a weekday, before noise."""
    return (150 + 1750 * bump24(h, 8.25, 1.15) + 900 * bump24(h, 12.6, 2.3)
            + 2100 * bump24(h, 17.9, 1.7) + 620 * bump24(h, 21.6, 2.0) + 280 * bump24(h, 0.4, 1.4))


def day_profile(day, h):
    if day <= 3:  # Mon-Thu
        return weekday_profile(h) * (0.97 + 0.02 * day)
    if day == 4:  # Fri: softer morning, big night
        return weekday_profile(h) * 1.04 + 1300 * bump24(h, 22.8, 1.8)
    if day == 5:  # Sat
        return 260 + 1200 * bump24(h, 12.8, 3.0) + 1500 * bump24(h, 19.5, 2.2) + 2100 * bump24(h, 23.4, 1.9)
    return 220 + 900 * bump24(h, 11.8, 2.6) + 1250 * bump24(h, 17.6, 2.4) + 500 * bump24(h, 0.6, 1.5)  # Sun


def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def build(g):
    labels = five_min_labels()
    completed, last_week, requested, fare, eta, events, late, p50, p95 = ([] for _ in range(9))
    for i in range(288):
        h = i / 12.0
        base = weekday_profile(h)
        c = base * g.jitter(0.045)
        lw = base * 0.94 * g.jitter(0.05)
        load = base / 2400.0
        rate = 0.955 - 0.05 * load + g.gauss(0.006)
        f = 16.4 + 3.1 * bump24(h, 8.25, 1.2) + 4.0 * bump24(h, 17.9, 1.5) + 3.2 * bump24(h, 0.5, 1.6) + g.gauss(0.35)
        e = 4.1 + 2.6 * bump24(h, 8.25, 1.1) + 3.4 * bump24(h, 17.9, 1.4) + g.gauss(0.25)
        ev = c * 44.2 * g.jitter(0.03) + 2600 * g.jitter(0.05)
        q50 = 5.9 + 0.9 * load + g.gauss(0.22)
        q95 = 9.0 + 1.9 * load + abs(g.gauss(0.45))
        if i in (100, 101):  # 08:20 and 08:25: broker 2 restarts during the morning peak
            q50, q95 = (9.8, 16.4) if i == 100 else (8.9, 18.9)
        elif i in (99, 102, 103):
            q50, q95 = q50 + 1.1, {99: 12.9, 102: 13.6, 103: 11.2}[i]
        completed.append(r0(c))
        last_week.append(r0(lw))
        requested.append(r0(c / rate))
        fare.append(r2(f))
        eta.append(r1(e))
        events.append(r0(ev))
        late.append(r0(ev * (0.0042 + g.gauss(0.0004))))
        p50.append(r1(q50))
        p95.append(r1(q95))

    total = sum(completed)
    total_lw = sum(last_week)
    total_req = sum(requested)
    total_ev = sum(events)
    avg_fare = sum(c * f for c, f in zip(completed, fare)) / total
    lw_fare = avg_fare / 1.019
    hourly = [sum(completed[h * 12:(h + 1) * 12]) for h in range(24)]
    hourly_ev = [sum(events[h * 12:(h + 1) * 12]) for h in range(24)]
    hourly_fare = [sum(completed[k] * fare[k] for k in range(h * 12, (h + 1) * 12)) / hourly[h] for h in range(24)]
    med_fresh = quantile(p50, 0.5)
    breaches = sum(1 for v in p95 if v > 15)

    kpis = [
        dict(label="Trips completed", value=total, fmt="int", delta=pct_change(total, total_lw), deltaLabel="vs Sep 29", spark=hourly),
        dict(label="Events processed", value=total_ev, fmt="compact", delta=pct_change(total_ev, total_ev / 1.061), deltaLabel="vs Sep 29", spark=hourly_ev),
        dict(label="Completion rate", value=r4(total / total_req), fmt="pct", delta=-0.0043, deltaLabel="vs Sep 29"),
        dict(label="Average fare", value=r2(avg_fare), fmt="usd2", delta=pct_change(avg_fare, lw_fare), deltaLabel="vs Sep 29", spark=[r2(v) for v in hourly_fare]),
        dict(label="Median freshness", value=r1(med_fresh), fmt="sec", status="good", statusLabel="Under the 15 s SLA"),
        dict(label="SLA breaches", value=breaches, fmt="int", status="warning", statusLabel="08:20–08:30, broker restart"),
    ]

    # Demand heatmap: average completed trips per hour over the last four weeks
    heat = []
    for d in range(7):
        for h in range(24):
            v = sum(day_profile(d, h + m / 12.0) for m in range(12)) * g.jitter(0.03)
            heat.append([h, d, r0(v)])

    # Fare distribution (log-normal around a $15.60 median)
    edges = [0, 5, 10, 15, 20, 25, 30, 40, 60, float("inf")]
    names = ["$0–5", "$5–10", "$10–15", "$15–20", "$20–25", "$25–30", "$30–40", "$40–60", "$60+"]
    mu, sigma = math.log(15.6), 0.55
    shares = []
    for lo, hi in zip(edges, edges[1:]):
        a = norm_cdf((math.log(lo) - mu) / sigma) if lo > 0 else 0.0
        b = norm_cdf((math.log(hi) - mu) / sigma) if hi != float("inf") else 1.0
        shares.append(b - a)
    fare_counts = [r0(total * s * g.jitter(0.02)) for s in shares]

    zone_trips = sorted((r0(total * s * g.jitter(0.015)) for s in ZONE_SHARE), reverse=True)

    # Peak consumer lag per partition; partitions led by broker 2 spike during its restart
    lag = []
    for p in range(12):
        if p in (2, 5, 8, 11):
            lag.append(r0(g.uni(39000, 48500)))
        else:
            lag.append(r0(g.uni(5200, 9400)))

    charts = dict(
        trips=dict(
            kind="line", x=labels, fmt="int", labelEvery=35, xLabel="Time",
            desc="Completed trips per 5 minutes on Oct 6 with the same Tuesday a week earlier for comparison. Peaks at 08:15 and 18:00.",
            series=[dict(name="Oct 6", data=completed, area=True), dict(name="Sep 29", data=last_week, context=True)],
        ),
        freshness=dict(
            kind="line", x=labels, fmt="sec", labelEvery=35, xLabel="Time", yMin=0, yMax=20,
            desc="Seconds from event time to a committed Iceberg row, p50 and p95 per 5 minutes, with the 15 second SLA.",
            series=[dict(name="p50", data=p50), dict(name="p95", data=p95)],
            thresholds=[dict(value=15, label="SLA 15 s", status="critical")],
        ),
        zones=dict(
            kind="bar", horizontal=True, x=ZONES, fmt="compact", valueLabels=True, xLabel="Pickup zone",
            desc="Completed trips by pickup zone for the ten busiest zones.",
            series=[dict(name="Trips", data=zone_trips)],
        ),
        lag=dict(
            kind="bar", x=["p%d" % p for p in range(12)], fmt="compact", xLabel="Partition",
            desc="Highest consumer lag each Kafka partition reached on Oct 6. Partitions 2, 5, 8 and 11 crossed the 20K alert during the broker restart.",
            series=[dict(name="Peak lag (events)", data=lag)],
            thresholds=[dict(value=20000, label="Alert 20K", status="warning")],
        ),
        demand=dict(
            kind="heatmap", x=hour_labels(), y=DAYS, values=heat, fmt="int", labelEvery=2, unitLabel="trips per hour",
            yLabel="Day", lowLabel="Fewer trips", highLabel="More trips",
            desc="Average completed trips per hour by weekday and hour over the last four weeks.",
        ),
        fares=dict(
            kind="bar", x=names, fmt="compact", valueLabels=True, xLabel="Fare", labelEvery=0,
            desc="Completed trips by fare band on Oct 6. Most trips cost between 10 and 20 dollars.",
            series=[dict(name="Trips", data=fare_counts)],
        ),
    )

    tables = dict(
        iceberg=dict(
            columns=[col("t", "Table", "code"), col("mode", "Write mode"), col("snap", "Snapshots", "int"),
                     col("files", "Data files", "int"), col("size", "Avg file", "mb"), col("last", "Last compaction"),
                     col("st", "Status", "status")],
            rows=[
                dict(t="rides.trip_events", mode="Append", snap=8640, files=3912, size=184, last="23:15", st=status("good", "Healthy")),
                dict(t="rides.driver_locations", mode="Append", snap=8640, files=11408, size=22, last="23:15", st=status("warning", "Compaction behind")),
                dict(t="rides.trips", mode="MERGE", snap=8640, files=2264, size=168, last="23:15", st=status("good", "Healthy")),
                dict(t="rides.payments", mode="Append", snap=8640, files=1187, size=141, last="23:15", st=status("good", "Healthy")),
                dict(t="rides.trips_5min", mode="MERGE", snap=288, files=96, size=12, last="03:15", st=status("good", "Healthy")),
            ],
        ),
        maintenance=dict(
            columns=[col("task", "Task", "code"), col("t", "Table", "code"), col("start", "Started"),
                     col("dur", "Duration", "dur"), col("res", "Result"), col("st", "Status", "status")],
            rows=[
                dict(task="rewrite_data_files", t="trip_events", start="23:15", dur=252, res="312 files → 9", st=status("good", "Succeeded")),
                dict(task="rewrite_data_files", t="driver_locations", start="23:15", dur=708, res="1,946 files → 31", st=status("warning", "Ran long")),
                dict(task="rewrite_data_files", t="trips", start="23:15", dur=187, res="228 files → 7", st=status("good", "Succeeded")),
                dict(task="expire_snapshots", t="all tables", start="23:16", dur=66, res="8,612 snapshots expired", st=status("good", "Succeeded")),
                dict(task="rewrite_position_deletes", t="trips", start="22:15", dur=131, res="96 delete files → 4", st=status("good", "Succeeded")),
                dict(task="remove_orphan_files", t="all tables", start="03:00", dur=221, res="214 orphan files removed", st=status("good", "Succeeded")),
            ],
        ),
    )

    header = ["interval_start", "trips_requested", "trips_completed", "completion_rate", "avg_fare_usd", "avg_eta_min",
              "events", "late_events", "freshness_p50_s", "freshness_p95_s", "trips_same_day_last_week"]
    rows = []
    for i in range(288):
        rows.append(["2026-10-06T%s:00-05:00" % labels[i], requested[i], completed[i], r4(completed[i] / requested[i]),
                     fare[i], eta[i], events[i], late[i], p50[i], p95[i], last_week[i]])
    return dict(kpis=kpis, charts=charts, tables=tables), header, rows


PANELS = [
    dict(chart="trips", span=8, h=300, title="Trips completed per 5 minutes", sub="Oct 6 against the same Tuesday last week"),
    dict(chart="zones", span=4, h=300, title="Busiest pickup zones", sub="Completed trips, top ten zones"),
    dict(chart="freshness", span=6, h=270, title="End-to-end freshness", sub="Event time to committed Iceberg row, p50 and p95"),
    dict(chart="lag", span=6, h=270, title="Peak consumer lag by partition", sub="Highest lag per Kafka partition today"),
    dict(chart="demand", span=7, h=300, title="Demand by weekday and hour", sub="Average completed trips per hour, last four weeks"),
    dict(chart="fares", span=5, h=300, title="Fare distribution", sub="Completed trips by fare band"),
    dict(table="iceberg", span=12, title="Iceberg table health", sub="Snapshots retained for 24 hours; compaction targets 256 MB files"),
    dict(table="maintenance", span=12, title="Latest maintenance runs", sub="Airflow DAG `iceberg_maintenance`"),
]

FLOW = [
    ("Produce", "Rider and driver apps", "Trip requests, state changes, payments and a driver location ping every 4 seconds, as Avro events."),
    ("Ingest", "Apache Kafka", "Four topics with 12 partitions each and 7-day retention. Schema Registry blocks incompatible schema changes."),
    ("Process", "Spark Structured Streaming", "10-second micro-batches, deduplication on `event_id` and a 10-minute watermark for late events."),
    ("Store", "Apache Iceberg on S3", "Hidden hourly partitions and atomic snapshot commits, so readers never see half a batch."),
    ("Maintain", "Apache Airflow", "Hourly compaction, snapshot expiry and orphan-file cleanup, with an alert when a table falls behind."),
    ("Query", "Trino and Athena", "Analysts and this dashboard read the same tables, seconds behind the apps."),
]

STEPS = [
    ("Exactly once, end to end.", "Raw events use Iceberg's streaming sink, which records each batch's epoch in the snapshot, so a replayed batch is skipped. Trip state is upserted with `MERGE` on `trip_id`, which is safe to run twice."),
    ("Event time, not arrival time.", "Aggregations follow `event_ts` with a 10-minute watermark. Events later than that go to `rides.late_events`, and a nightly job folds them in."),
    ("Small files are expected, then fixed.", "Committing every 10 seconds creates many small files. Hourly `rewrite_data_files` runs compact them to about 256 MB."),
    ("Freshness is measured, not assumed.", "Every row carries `ingest_ts`. The job records commit time minus event time per batch, which feeds the freshness chart and its alert."),
]

MODELS = [
    dict(name="rides.trip_events", kind="Iceberg · append · hours(event_ts)", columns=[
        ("event_id", "string", "Unique id from the app; used to drop duplicates"),
        ("trip_id", "string", "Trip the event belongs to"),
        ("event_type", "string", "`requested`, `accepted`, `picked_up`, `completed`, `cancelled`"),
        ("rider_id / driver_id", "string", "Pseudonymous ids"),
        ("zone_id", "int", "Pickup zone"),
        ("fare_usd", "decimal(8,2)", "Set on `completed` events"),
        ("event_ts", "timestamp", "When it happened in the app"),
        ("ingest_ts", "timestamp", "When the row was committed"),
    ]),
    dict(name="rides.trips", kind="Iceberg · MERGE · days(requested_ts)", columns=[
        ("trip_id", "string", "Primary key"),
        ("status", "string", "Latest state of the trip"),
        ("requested_ts", "timestamp", "Request time"),
        ("pickup_ts / dropoff_ts", "timestamp", "Null until the trip reaches that state"),
        ("pickup_zone / dropoff_zone", "int", "Zone ids"),
        ("distance_km", "decimal(6,2)", "Trip distance"),
        ("fare_usd", "decimal(8,2)", "Final fare"),
        ("updated_ts", "timestamp", "Event time of the latest change; MERGE keeps the newest"),
    ]),
    dict(name="rides.trips_5min", kind="Iceberg · MERGE · the CSV download", columns=[
        ("interval_start", "timestamp", "Start of the 5-minute window"),
        ("trips_requested / trips_completed", "bigint", "Counts in the window"),
        ("avg_fare_usd / avg_eta_min", "double", "Averages over completed trips"),
        ("events / late_events", "bigint", "Events processed, and how many arrived after their window"),
        ("freshness_p50_s / freshness_p95_s", "double", "Event time to commit, in seconds"),
    ]),
]

CODE = [
    dict(file="stream_trips.py", lang="python",
         caption="Reads trip events from Kafka, appends them to `rides.trip_events` and upserts trip state into `rides.trips` every 10 seconds.",
         code='''
"""Kafka -> Iceberg. Raw events are appended; trip state is upserted with MERGE."""
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.avro.functions import from_avro

CHECKPOINTS = "s3://rides-lakehouse/checkpoints"
SCHEMA = open("schemas/trip_event.avsc").read()

spark = (
    SparkSession.builder.appName("stream-trips")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.lake.warehouse", "s3://rides-lakehouse/warehouse")
    .getOrCreate()
)

events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "broker-1:9092,broker-2:9092,broker-3:9092")
    .option("subscribe", "rides.trip-events")
    .option("maxOffsetsPerTrigger", 400_000)
    .load()
    # Confluent wire format: skip the magic byte and 4-byte schema id
    .select(from_avro(F.expr("substring(value, 6, length(value) - 5)"), SCHEMA).alias("e"))
    .select("e.*")
    .withColumn("event_ts", F.expr("timestamp_millis(event_time_ms)"))
    .withColumn("ingest_ts", F.current_timestamp())
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicatesWithinWatermark(["event_id"])
)

# 1) Raw events: the Iceberg sink commits each epoch once, even after a restart.
raw = (
    events.drop("event_time_ms").writeStream.format("iceberg")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", f"{CHECKPOINTS}/trip_events")
    .toTable("lake.rides.trip_events")
)


# 2) Trip state: keep the newest event per trip, then MERGE (idempotent on retry).
def upsert_trips(batch, batch_id):
    newest = Window.partitionBy("trip_id").orderBy(F.col("event_ts").desc())
    (batch.where("trip_id IS NOT NULL")
          .withColumn("rn", F.row_number().over(newest))
          .where("rn = 1")
          .createOrReplaceTempView("updates"))
    batch.sparkSession.sql("""
        MERGE INTO lake.rides.trips t
        USING updates u ON t.trip_id = u.trip_id
        WHEN MATCHED AND u.event_ts > t.updated_ts THEN UPDATE SET
            status = u.event_type,
            pickup_ts = coalesce(t.pickup_ts, CASE WHEN u.event_type = 'picked_up' THEN u.event_ts END),
            dropoff_ts = CASE WHEN u.event_type = 'completed' THEN u.event_ts ELSE t.dropoff_ts END,
            fare_usd = coalesce(u.fare_usd, t.fare_usd),
            updated_ts = u.event_ts
        WHEN NOT MATCHED THEN INSERT (trip_id, rider_id, status, requested_ts, pickup_zone, updated_ts)
            VALUES (u.trip_id, u.rider_id, u.event_type, u.event_ts, u.zone_id, u.event_ts)
    """)


state = (
    events.writeStream.foreachBatch(upsert_trips)
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", f"{CHECKPOINTS}/trips")
    .start()
)

spark.streams.awaitAnyTermination()
'''),
    dict(file="tables.sql", lang="sql",
         caption="Iceberg table definitions. Hidden partitioning means queries filter on `event_ts` and Iceberg prunes the files.",
         code='''
-- Raw events: append-only, partitioned by hour without a separate partition column.
CREATE TABLE IF NOT EXISTS lake.rides.trip_events (
  event_id    STRING    NOT NULL,
  trip_id     STRING,
  event_type  STRING    NOT NULL,
  rider_id    STRING,
  driver_id   STRING,
  zone_id     INT,
  fare_usd    DECIMAL(8, 2),
  event_ts    TIMESTAMP NOT NULL,
  ingest_ts   TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (hours(event_ts))
TBLPROPERTIES (
  'format-version' = '2',
  'write.target-file-size-bytes' = '268435456',
  'write.distribution-mode' = 'hash',
  'history.expire.max-snapshot-age-ms' = '86400000',   -- keep 24 hours of snapshots
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '50'
);

-- Current state of every trip, upserted each micro-batch. Merge-on-read keeps MERGE cheap.
CREATE TABLE IF NOT EXISTS lake.rides.trips (
  trip_id       STRING NOT NULL,
  rider_id      STRING,
  driver_id     STRING,
  status        STRING,
  requested_ts  TIMESTAMP,
  pickup_ts     TIMESTAMP,
  dropoff_ts    TIMESTAMP,
  pickup_zone   INT,
  dropoff_zone  INT,
  distance_km   DECIMAL(6, 2),
  fare_usd      DECIMAL(8, 2),
  updated_ts    TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (days(requested_ts), bucket(16, trip_id))
TBLPROPERTIES (
  'format-version' = '2',
  'write.merge.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'history.expire.max-snapshot-age-ms' = '86400000'
);
'''),
    dict(file="iceberg_maintenance.py", lang="python",
         caption="Hourly Airflow DAG: compact small files, then expire old snapshots. Orphan files are removed once a night.",
         code='''
"""Hourly Iceberg upkeep for the rides tables."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator

TABLES = ["rides.trip_events", "rides.driver_locations", "rides.trips", "rides.payments"]

COMPACT = """
CALL lake.system.rewrite_data_files(
  table => '{table}',
  strategy => 'binpack',
  options => map(
    'target-file-size-bytes', '268435456',
    'min-input-files', '8',
    'partial-progress.enabled', 'true'))
"""
EXPIRE = "CALL lake.system.expire_snapshots(table => '{table}', retain_last => 100)"
ORPHANS = "CALL lake.system.remove_orphan_files(table => '{table}')"

with DAG(
    dag_id="iceberg_maintenance",
    schedule="15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["iceberg", "maintenance"],
) as dag:
    nightly = ShortCircuitOperator(
        task_id="only_at_3am",
        python_callable=lambda data_interval_end, **_: data_interval_end.hour == 3,
    )
    for table in TABLES:
        name = table.split(".")[-1]
        compact = SparkSqlOperator(task_id=f"compact_{name}", sql=COMPACT.format(table=table), conn_id="spark_lake")
        expire = SparkSqlOperator(task_id=f"expire_{name}", sql=EXPIRE.format(table=table), conn_id="spark_lake")
        orphans = SparkSqlOperator(task_id=f"orphans_{name}", sql=ORPHANS.format(table=table), conn_id="spark_lake")
        compact >> expire
        nightly >> orphans
'''),
]

HIGHLIGHTS = [
    ("A broker restart, absorbed",
     "At 08:20, during the morning peak, Kafka broker 2 restarted. Lag on its four partitions passed 40K events and p95 freshness broke the 15 s SLA for two intervals. The job caught up in about six minutes with no lost or duplicated rows."),
    ("Small files under control",
     "Ten-second commits create about 8,600 snapshots a day per table. Hourly compaction keeps average files near the 256 MB target. `driver_locations` is flagged because its compaction is running long and needs more executors."),
    ("Late data has a home",
     "About 0.4% of events arrive late but inside the 10-minute watermark, and are merged normally. The rare events beyond it go to `rides.late_events` and are folded in by a nightly batch job."),
    ("Readers never see half a batch",
     "Each micro-batch is one Iceberg commit. Queries read the last committed snapshot, and MERGE on `trip_id` keeps `rides.trips` correct when a batch is retried."),
]
