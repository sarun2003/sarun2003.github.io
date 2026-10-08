"""Project 05: Data quality and observability platform."""

import datetime as dt

from common import bump24, col, day_label, day_list, r0, r4, status

META = dict(
    num="05",
    slug="data-quality-observability",
    seed=505,
    title="Data Quality and Observability Platform",
    short="Data Quality Platform",
    category="Data quality",
    tagline="Freshness SLAs, volume anomaly detection, schema checks and a dead-letter queue across 148 tables, with every incident routed to the team that owns the data.",
    tile_stack="Great Expectations · PySpark · Monte Carlo · Grafana",
    summary=(
        "Bad data is cheaper to catch at the door than in a board deck. This platform watches 148 tables across the warehouse and the lake. "
        "Great Expectations suites validate each batch inside the PySpark job that writes it, Monte Carlo learns every table's normal freshness and volume "
        "and flags anomalies no rule anticipated, and records that cannot be parsed go to a dead-letter queue instead of disappearing. All results land in one "
        "metrics store that Grafana and this dashboard read, and incidents go to the owning team with the failing rows attached."
    ),
    stack=["Great Expectations", "PySpark", "Monte Carlo", "Prometheus", "Grafana"],
    window="Last 30 days · Sep 7 – Oct 6, 2026",
    csv="check_results_daily.csv",
    csv_desc="Checks run and failed per day for each quality dimension",
    how_note="Rules for what we know can break, learning for what we don't, and a home for every rejected record.",
    model_note="Check results and rejected records are data too: both are tables with an owner and a retention policy.",
    code_note="Validation inside the write path, the dead-letter routing, and the freshness alert rule.",
)

END = dt.date(2026, 10, 6)
DIMS = ["Validity", "Freshness", "Volume", "Distribution", "Uniqueness", "Schema"]
DIM_RUNS = [1040, 1776, 1776, 120, 312, 1184]
DIM_FAIL = [21.5, 11.8, 8.6, 5.9, 3.1, 2.2]


def build(g):
    days = day_list(END, 30)
    labels = [day_label(d) for d in days]
    runs = [[0] * 30 for _ in DIMS]
    fails = [[0] * 30 for _ in DIMS]
    for i, d in enumerate(days):
        for k in range(len(DIMS)):
            runs[k][i] = r0(DIM_RUNS[k] * (1 + 0.002 * i) * g.jitter(0.01))
            f = DIM_FAIL[k] * g.jitter(0.28) * (0.85 if i >= 15 else 1.0)
            if d == dt.date(2026, 10, 2) and DIMS[k] in ("Volume", "Freshness"):
                f += 21 if DIMS[k] == "Volume" else 14
            if d == dt.date(2026, 10, 5) and DIMS[k] in ("Volume", "Uniqueness"):
                f += 9
            if d == dt.date(2026, 9, 24) and DIMS[k] == "Validity":
                f += 26
            if d == dt.date(2026, 9, 30) and DIMS[k] == "Uniqueness":
                f += 12
            fails[k][i] = r0(f)
    total_runs = sum(sum(r) for r in runs)
    total_fails = sum(sum(f) for f in fails)
    daily_fail = [sum(fails[k][i] for k in range(len(DIMS))) for i in range(30)]

    # Hourly volume for analytics.fct_orders, last 7 days, with a learned expected range
    vol_labels, vol, lower, upper, points = [], [], [], [], []
    for di, d in enumerate(days[-7:]):
        weekend = d.weekday() >= 5
        for h in range(24):
            base = (5200 + 21000 * bump24(h, 12.8, 2.6) + 17000 * bump24(h, 20.2, 2.0)) * (1.12 if weekend else 1.0)
            v = base * g.jitter(0.045)
            idx = di * 24 + h
            label = "%s %02d:00" % (day_label(d), h)
            if d == dt.date(2026, 10, 2) and 12 <= h <= 14:
                v = base * [0.2, 0.16, 0.31][h - 12]
                points.append([idx, r0(v), "Drop: upstream API outage"])
            if d == dt.date(2026, 10, 5) and h == 14:
                v = base * 1.95
                points.append([idx, r0(v), "Spike: duplicate batch"])
            vol_labels.append(label)
            vol.append(r0(v))
            lower.append(r0(base * 0.86))
            upper.append(r0(base * 1.14))

    kpis = [
        dict(label="Tables monitored", value=148, fmt="int", note="Warehouse and lake, 23 owners"),
        dict(label="Checks run", value=total_runs, fmt="compact", delta=0.061, deltaLabel="vs prior 30 days", spark=[sum(runs[k][i] for k in range(len(DIMS))) for i in range(30)]),
        dict(label="Check pass rate", value=r4(1 - total_fails / total_runs), fmt="pct2", delta=0.0011, deltaLabel="vs prior 30 days"),
        dict(label="Open incidents", value=3, fmt="int", status="critical", statusLabel="1 critical, 2 warnings"),
        dict(label="Mean time to detect", value=7, fmt="min", delta=-0.222, upIsGood=False, deltaLabel="vs prior 30 days"),
        dict(label="Mean time to resolve", value=6720, fmt="dur", delta=-0.138, upIsGood=False, deltaLabel="vs prior 30 days"),
    ]

    dlq = [("Schema mismatch", 18402), ("Missing required field", 11960), ("Invalid timestamp", 7315),
           ("Unknown enum value", 5880), ("Payload too large", 2412), ("Not valid JSON", 2244)]

    fresh = [
        ("analytics.fct_inventory_snapshot", 174, 120, "critical", "Breached by 54 min"),
        ("analytics.payments", 26, 30, "warning", "Close to SLA"),
        ("analytics.shipments", 41, 60, "good", "On time"),
        ("analytics.fct_orders", 22, 60, "good", "On time"),
        ("finance.invoices", 95, 240, "good", "On time"),
    ]

    def mins(m):
        return "%d min" % m if m < 120 else ("%dh %02dm" % (m // 60, m % 60) if m % 60 else "%dh" % (m // 60))

    charts = dict(
        volume=dict(
            kind="line", x=vol_labels, fmt="compact", labelEvery=23, tickLabel="day", xLabel="Hour",
            desc="Rows loaded per hour into analytics.fct_orders over the last 7 days, with the expected range learned from history. Three midday hours on Oct 2 fell far below it and one hour on Oct 5 spiked above it.",
            series=[dict(name="Rows loaded", data=vol)],
            band=dict(lower=lower, upper=upper, name="Expected range"),
            points=dict(name="Anomaly", items=points),
        ),
        freshness=dict(
            kind="meters", fmt="pct", statusChips=True, codeLabels=True, marker=1 / 1.5,
            items=[dict(label=t, value=a / s, fill=min(a / s, 1.5) / 1.5, text="%s of %s" % (mins(a), mins(s)), status=st, statusLabel=lab)
                   for t, a, s, st, lab in fresh],
        ),
        failures=dict(
            kind="bar", x=labels, fmt="int", stack=True, labelEvery=6, xLabel="Day",
            desc="Failed checks per day, stacked by quality dimension. Spikes match the incidents on Sep 24, Sep 30, Oct 2 and Oct 5.",
            series=[dict(name=n, data=fails[k]) for k, n in enumerate(DIMS)],
        ),
        dlq=dict(
            kind="bar", horizontal=True, x=[d[0] for d in dlq], fmt="compact", valueLabels=True, xLabel="Reason",
            desc="Records sent to the dead-letter queue in the last 30 days, by reason.",
            series=[dict(name="Records", data=[d[1] for d in dlq])],
        ),
    )

    tables = dict(
        incidents=dict(
            columns=[col("id", "Incident", "code"), col("opened", "Opened"), col("t", "Table", "code"), col("check", "Check"),
                     col("sev", "Severity", "status"), col("ttd", "Detected in", "min"), col("owner", "Owner"), col("state", "State")],
            rows=[
                dict(id="INC-2291", opened="Oct 6, 06:12", t="analytics.fct_inventory_snapshot", check="Freshness", sev=status("critical", "Critical"), ttd=9, owner="Supply chain", state="Open"),
                dict(id="INC-2290", opened="Oct 5, 14:07", t="analytics.fct_orders", check="Volume +95%", sev=status("serious", "High"), ttd=7, owner="Commerce", state="Resolved in 1h 41m"),
                dict(id="INC-2289", opened="Oct 5, 09:30", t="marketing.campaign_spend", check="Schema: column dropped", sev=status("warning", "Warning"), ttd=4, owner="Marketing", state="Open"),
                dict(id="INC-2288", opened="Oct 4, 11:15", t="analytics.payments", check="Freshness", sev=status("warning", "Warning"), ttd=12, owner="Payments", state="Open"),
                dict(id="INC-2286", opened="Oct 2, 12:06", t="analytics.fct_orders", check="Volume −82%", sev=status("critical", "Critical"), ttd=6, owner="Commerce", state="Resolved in 3h 05m"),
                dict(id="INC-2283", opened="Sep 30, 08:02", t="crm.accounts", check="Uniqueness: account_id", sev=status("serious", "High"), ttd=11, owner="Sales ops", state="Resolved in 2h 20m"),
                dict(id="INC-2279", opened="Sep 27, 17:45", t="events.sessions", check="Null rate 14%", sev=status("warning", "Warning"), ttd=21, owner="Product analytics", state="Resolved in 55m"),
                dict(id="INC-2275", opened="Sep 24, 02:10", t="finance.invoices", check="Validity: negative totals", sev=status("serious", "High"), ttd=3, owner="Finance", state="Resolved in 1h 28m"),
            ],
        ),
        suites=dict(
            columns=[col("suite", "Suite", "code"), col("asset", "Table", "code"), col("n", "Expectations", "int"),
                     col("last", "Last run"), col("ok", "Success", "pct"), col("st", "Status", "status")],
            rows=[
                dict(suite="orders.critical", asset="analytics.fct_orders", n=42, last="Oct 6, 23:55", ok=1.0, st=status("good", "Passing")),
                dict(suite="payments.critical", asset="analytics.payments", n=31, last="Oct 6, 23:50", ok=1.0, st=status("good", "Passing")),
                dict(suite="invoices.finance", asset="finance.invoices", n=28, last="Oct 6, 05:00", ok=1.0, st=status("good", "Passing")),
                dict(suite="inventory.snapshot", asset="analytics.fct_inventory_snapshot", n=26, last="Oct 6, 02:40", ok=25 / 26, st=status("warning", "1 failed")),
                dict(suite="sessions.daily", asset="events.sessions", n=24, last="Oct 6, 04:10", ok=1.0, st=status("good", "Passing")),
                dict(suite="customers.pii", asset="crm.customers", n=19, last="Oct 6, 22:00", ok=1.0, st=status("good", "Passing")),
                dict(suite="campaign_spend", asset="marketing.campaign_spend", n=15, last="Oct 6, 09:31", ok=14 / 15, st=status("warning", "1 failed")),
            ],
        ),
    )

    header = ["date", "dimension", "checks_run", "checks_failed", "pass_rate"]
    rows = []
    for i, d in enumerate(days):
        for k, n in enumerate(DIMS):
            rows.append([d.isoformat(), n, runs[k][i], fails[k][i], r4(1 - fails[k][i] / runs[k][i])])
    return dict(kpis=kpis, charts=charts, tables=tables), header, rows


PANELS = [
    dict(chart="volume", span=8, h=380, title="Volume vs expected: analytics.fct_orders", sub="Rows loaded per hour, last 7 days, with the learned expected range"),
    dict(chart="freshness", span=4, title="Freshness against SLA", sub="Time since last update. The white mark is each table's SLA."),
    dict(chart="failures", span=7, h=290, title="Failed checks per day", sub="By quality dimension"),
    dict(chart="dlq", span=5, h=290, title="Dead-letter queue by reason", sub="Records rejected in the last 30 days"),
    dict(table="incidents", span=12, title="Incidents", sub="Opened automatically from checks and anomaly monitors"),
    dict(table="suites", span=12, title="Great Expectations suites", sub="Critical suites block the write when they fail"),
]

FLOW = [
    ("Validate on write", "Great Expectations + PySpark", "Each batch is checked before it is published: nulls, ranges, uniqueness, row counts. Critical suites block the write."),
    ("Route bad records", "Dead-letter queue", "Records that fail parsing or a contract go to a Kafka DLQ topic with the reason attached, so nothing disappears."),
    ("Learn normal", "Monte Carlo", "Learns each table's usual freshness, volume and schema, and flags changes nobody wrote a rule for."),
    ("Measure", "Prometheus", "Check results, table ages and DLQ counts become metrics labelled with the table and its owner."),
    ("Alert", "Grafana", "Freshness SLAs alert the owning team's channel with a runbook link; dashboards per domain."),
    ("Respond", "Incidents", "Each alert opens an incident with the failing rows, the lineage and the reports downstream."),
]

STEPS = [
    ("Contracts at the producer.", "Critical suites run inside the job that writes the data, so a failure stops publication instead of paging someone after the numbers are already wrong."),
    ("Rules plus learning.", "Hand-written checks cover what we know can break; anomaly monitors cover the rest, like the Oct 2 volume drop that no rule anticipated."),
    ("Nothing silently dropped.", "Every rejected record lands in the DLQ with its reason, topic and offset. A daily job reports counts by producer and replays records once they are fixed."),
    ("Owners, not channels.", "Every table has an owning team in its metadata, so alerts reach the people who can fix the problem, with a runbook link."),
]

MODELS = [
    dict(name="dq.check_results", kind="Iceberg table · one row per check run", columns=[
        ("run_id", "string", "Validation run"),
        ("table_name / owner", "string", "Table checked and the team that owns it"),
        ("check_name / dimension", "string", "For example `order_id_unique`, Uniqueness"),
        ("passed", "boolean", "Result"),
        ("observed_value / threshold", "double", "What was measured and the limit"),
        ("run_ts", "timestamp", "When the check ran"),
    ]),
    dict(name="dlq.records", kind="Kafka topic, mirrored to a table", columns=[
        ("source_topic / partition / offset", "string / int / bigint", "Exactly where the record came from"),
        ("reason", "string", "Why it was rejected"),
        ("producer", "string", "App or service that sent it"),
        ("payload", "string", "The original record, untouched"),
        ("rejected_at", "timestamp", "When it was routed to the DLQ"),
    ]),
    dict(name="check_results_daily", kind="Aggregate · the CSV download", columns=[
        ("date", "date", "Day"),
        ("dimension", "string", "Validity, Freshness, Volume, Distribution, Uniqueness or Schema"),
        ("checks_run / checks_failed", "int", "Counts for the day"),
        ("pass_rate", "double", "Share of checks that passed"),
    ]),
]

CODE = [
    dict(file="validate_orders.py", lang="python",
         caption="Runs the critical suite on each batch inside the PySpark job. Failing batches go to quarantine; results become metrics.",
         code='''
import great_expectations as gx
import great_expectations.expectations as gxe
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pyspark.sql import functions as F

context = gx.get_context(mode="file", project_root_dir="gx")

suite = context.suites.add_or_update(gx.ExpectationSuite(
    name="orders.critical",
    expectations=[
        gxe.ExpectColumnValuesToNotBeNull(column="order_id"),
        gxe.ExpectColumnValuesToBeUnique(column="order_id"),
        gxe.ExpectColumnValuesToBeBetween(column="total_usd", min_value=0, max_value=50_000),
        gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["placed", "paid", "shipped", "cancelled"]),
        gxe.ExpectTableRowCountToBeBetween(min_value=1_000, max_value=2_000_000),
    ],
))

batch_def = (
    context.data_sources.add_or_update_spark(name="spark")
    .add_dataframe_asset(name="orders_batch")
    .add_batch_definition_whole_dataframe("per_run")
)
validation = context.validation_definitions.add_or_update(
    gx.ValidationDefinition(name="orders.critical", data=batch_def, suite=suite)
)


def publish_if_valid(df, run_id: str) -> bool:
    result = validation.run(batch_parameters={"dataframe": df})

    registry = CollectorRegistry()
    passed = Gauge("dq_checks_passed", "Checks passed", ["table", "suite"], registry=registry)
    failed = Gauge("dq_checks_failed", "Checks failed", ["table", "suite"], registry=registry)
    stats = result.statistics
    passed.labels("analytics.fct_orders", "orders.critical").set(stats["successful_expectations"])
    failed.labels("analytics.fct_orders", "orders.critical").set(stats["unsuccessful_expectations"])
    push_to_gateway("pushgateway:9091", job="dq-orders", registry=registry)

    if result.success:
        df.writeTo("lake.analytics.fct_orders").append()
        return True

    # Keep the batch for inspection and replay; downstream readers never see it.
    df.withColumn("run_id", F.lit(run_id)).writeTo("lake.quarantine.fct_orders").append()
    return False
'''),
    dict(file="route_to_dlq.py", lang="python",
         caption="Structured Streaming: valid events continue, anything that cannot be parsed or breaks the contract goes to the DLQ with a reason.",
         code='''
from pyspark.sql import functions as F

from schemas import ORDER_EVENT  # the contract, plus a _corrupt_record string field

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders.events")
    .load()
    .select("topic", "partition", "offset", F.col("value").cast("string").alias("payload"))
)

checked = (
    raw.withColumn("e", F.from_json("payload", ORDER_EVENT,
                                    {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}))
    .withColumn("reason",
        F.when(F.length("payload") > 512_000, "Payload too large")
         .when(F.col("e._corrupt_record").isNotNull(), "Not valid JSON")
         .when(F.col("e.order_id").isNull() | F.col("e.status").isNull(), "Missing required field")
         .when(F.col("e.event_ts").isNull(), "Invalid timestamp")
         .when(~F.col("e.status").isin("placed", "paid", "shipped", "cancelled"), "Unknown enum value"))
)


def route(batch, batch_id):
    batch.persist()
    (batch.where("reason IS NULL").select("e.*").drop("_corrupt_record")
          .writeTo("lake.staging.order_events").append())
    (batch.where("reason IS NOT NULL")
          .select(
              F.col("payload").alias("value"),
              F.array(
                  F.struct(F.lit("reason").alias("key"), F.col("reason").cast("binary").alias("value")),
                  F.struct(F.lit("source").alias("key"),
                           F.concat_ws(":", "topic", "partition", "offset").cast("binary").alias("value")),
              ).alias("headers"))
          .write.format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("topic", "dlq.orders.events")
          .option("includeHeaders", "true")
          .save())
    batch.unpersist()


(checked.writeStream.foreachBatch(route)
    .option("checkpointLocation", "s3://lake/checkpoints/order_events")
    .trigger(processingTime="30 seconds")
    .start())
'''),
    dict(file="freshness-rules.yml", lang="yaml",
         caption="Prometheus alerting rule behind the freshness meters: fires when a table is older than its own SLA.",
         code='''
groups:
  - name: data-freshness
    rules:
      - record: dq:table_age_seconds
        expr: time() - dq_table_last_loaded_timestamp_seconds

      - alert: TableFreshnessSLABreached
        expr: dq:table_age_seconds > on (table) dq_table_freshness_sla_seconds
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "{{ $labels.table }} has not loaded for {{ $value | humanizeDuration }}"
          owner: "{{ $labels.owner }}"
          runbook_url: "https://runbooks.example.com/data/freshness"

      - alert: TableFreshnessNearSLA
        expr: dq:table_age_seconds > on (table) (0.8 * dq_table_freshness_sla_seconds)
        for: 10m
        labels:
          severity: warning
'''),
]

HIGHLIGHTS = [
    ("Caught before the dashboards were",
     "At noon on Oct 2 an upstream API outage cut hourly order volume by about 80%. The anomaly monitor opened INC-2286 six minutes later, before anyone looked at a report, and a backfill closed it in three hours."),
    ("A duplicate load, not a sales spike",
     "At 14:00 on Oct 5, volume jumped 95%. The `order_id` uniqueness check showed a retried batch had loaded twice; it was removed before finance saw inflated numbers."),
    ("Detection got faster",
     "Mean time to detect fell from 9 to 7 minutes after freshness checks for critical tables moved from hourly to every 10 minutes."),
    ("The DLQ is a to-do list",
     "Of 48K rejected records, 38% were schema mismatches from one mobile app version. It was fixed in the next release and 17K records were replayed."),
]
