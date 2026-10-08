"""Project 02: Data Lakehouse with Medallion Architecture (Databricks)."""

import datetime as dt

from common import col, day_label, day_list, pct_change, quantile, r0, r1, r4, status

META = dict(
    num="02",
    slug="medallion-lakehouse",
    seed=202,
    title="Data Lakehouse with Medallion Architecture",
    short="Medallion Lakehouse",
    category="Lakehouse",
    tagline="Retail orders, inventory and web events ingested with Auto Loader and refined bronze → silver → gold by a Lakeflow pipeline on Databricks.",
    tile_stack="Databricks · Delta Lake · Auto Loader · Lakeflow",
    summary=(
        "Order files from the web store, point-of-sale exports from 140 stores, nightly inventory snapshots and web events land in cloud storage all day. "
        "Auto Loader picks up each new file exactly once and writes it untouched to bronze Delta tables. A Lakeflow Declarative Pipeline (formerly Delta Live Tables) "
        "cleans, deduplicates and conforms the data into silver, where data quality expectations quarantine bad rows instead of failing the run, "
        "then builds the gold tables that sales, inventory and marketing reporting read."
    ),
    stack=["Databricks", "Delta Lake", "Auto Loader", "Lakeflow Declarative Pipelines", "Unity Catalog"],
    window="Last 30 days · Sep 7 – Oct 6, 2026",
    csv="gold_daily_sales.csv",
    csv_desc="Gold daily sales by category",
    how_note="Each layer has one job: bronze keeps everything, silver makes it trustworthy, gold makes it useful.",
    model_note="Tables live in Unity Catalog under `retail`. The CSV download is `gold.daily_sales` for the 30-day window.",
    code_note="The pipeline itself, the bundle that deploys it, and the event-log query behind the expectations table.",
)

END = dt.date(2026, 10, 6)
CATS = ["Grocery", "Home", "Apparel", "Electronics", "Beauty"]
CAT_SHARE = [0.31, 0.21, 0.19, 0.18, 0.11]
CAT_PRICE = [4.1, 9.8, 11.5, 24.0, 7.9]  # average line value, USD


def build(g):
    days = day_list(END, 30)
    labels = [day_label(d) for d in days]

    src = {"orders": [], "pos": [], "inventory": [], "web": []}
    lines_by_cat = [[] for _ in CATS]
    rev_by_cat = [[] for _ in CATS]
    units_by_cat = [[] for _ in CATS]
    for i, d in enumerate(days):
        wd = d.weekday()
        weekend = 1.16 if wd >= 5 else (1.05 if wd == 4 else 1.0)
        sale = 1.27 if dt.date(2026, 9, 25) <= d <= dt.date(2026, 9, 27) else 1.0
        trend = 1 + 0.0016 * i
        orders = 690_000 * weekend * sale * trend * g.jitter(0.035)
        pos = 1_250_000 * (1.2 if wd >= 5 else 1.0) * sale * trend * g.jitter(0.03)
        inv = 420_000 * g.jitter(0.004)
        web = 3_620_000 * (1.12 if wd >= 5 else 1.0) * (1.22 if sale > 1 else 1.0) * trend * g.jitter(0.04)
        src["orders"].append(r0(orders))
        src["pos"].append(r0(pos))
        src["inventory"].append(r0(inv))
        src["web"].append(r0(web))
        lines = orders + pos
        for c in range(len(CATS)):
            share = CAT_SHARE[c] * (1.25 if (c == 2 and sale > 1) else 1.0) * g.jitter(0.03)
            n = lines * share * 0.989
            units = n * (1.6 if c == 0 else 1.2) * g.jitter(0.02)
            lines_by_cat[c].append(r0(n))
            units_by_cat[c].append(r0(units))
            rev_by_cat[c].append(r0(n * CAT_PRICE[c] * g.jitter(0.025)))

    tot = {k: sum(v) for k, v in src.items()}
    bronze_total = sum(tot.values())
    bronze_daily = [sum(src[k][i] for k in src) for i in range(30)]

    # Expectation failures (silver). Rates eased after a store-dimension fix on Sep 23.
    rules = [
        # dataset, rule, constraint, action, source keys, failure rate
        ("silver.order_lines", "known_store_id", "store_id IN silver.stores", "Quarantine", ("orders", "pos"), 0.0068),
        ("silver.order_lines", "positive_quantity", "quantity > 0", "Quarantine", ("orders", "pos"), 0.0028),
        ("silver.order_lines", "valid_order_ts", "order_ts IS NOT NULL", "Quarantine", ("orders", "pos"), 0.0019),
        ("silver.order_lines", "currency_is_usd", "currency = 'USD'", "Warn", ("orders", "pos"), 0.00016),
        ("silver.web_sessions", "has_session_id", "session_id IS NOT NULL", "Quarantine", ("web",), 0.0034),
        ("silver.web_sessions", "valid_event_ts", "event_ts <= current_timestamp()", "Quarantine", ("web",), 0.0018),
        ("silver.inventory", "non_negative_on_hand", "on_hand_qty >= 0", "Quarantine", ("inventory",), 0.0063),
        ("silver.inventory", "sku_present", "sku IS NOT NULL", "Fail update", ("inventory",), 0.0),
    ]
    exp_rows, reasons = [], []
    quarantine_by_src = {k: 0 for k in src}
    for ds, name, cons, action, keys, rate in rules:
        checked = sum(tot[k] for k in keys)
        failed = r0(checked * rate * g.jitter(0.03)) if rate else 0
        if action == "Quarantine":
            for k in keys:
                quarantine_by_src[k] += failed * tot[k] / checked
            reasons.append((name, failed))
        pr = 1 - failed / checked
        if failed == 0:
            st = status("good", "No failures")
        elif action == "Warn":
            st = status("neutral", "Kept, flagged")
        elif rate >= 0.005:
            st = status("warning", "Over 0.5% budget")
        else:
            st = status("good", "Within budget")
        exp_rows.append(dict(ds=ds, rule=name, cons=cons, action=action, checked=checked, failed=failed, pr=r4(pr), st=st))
    quarantined = r0(sum(quarantine_by_src.values()))

    # Sankey: source files -> bronze -> silver / quarantine -> gold
    q = {k: r0(v) for k, v in quarantine_by_src.items()}
    links = [
        ("orders/*.json", "bronze.orders_raw", tot["orders"]),
        ("pos/*.csv", "bronze.pos_raw", tot["pos"]),
        ("inventory/*.parquet", "bronze.inventory_raw", tot["inventory"]),
        ("web_events/*.json", "bronze.web_events_raw", tot["web"]),
        ("bronze.orders_raw", "silver.order_lines", tot["orders"] - q["orders"]),
        ("bronze.pos_raw", "silver.order_lines", tot["pos"] - q["pos"]),
        ("bronze.inventory_raw", "silver.inventory", tot["inventory"] - q["inventory"]),
        ("bronze.web_events_raw", "silver.web_sessions", tot["web"] - q["web"]),
        ("bronze.orders_raw", "quarantine", q["orders"]),
        ("bronze.pos_raw", "quarantine", q["pos"]),
        ("bronze.inventory_raw", "quarantine", q["inventory"]),
        ("bronze.web_events_raw", "quarantine", q["web"]),
        ("silver.order_lines", "gold.daily_sales", tot["orders"] - q["orders"] + tot["pos"] - q["pos"]),
        ("silver.inventory", "gold.inventory_health", tot["inventory"] - q["inventory"]),
        ("silver.web_sessions", "gold.web_funnel", tot["web"] - q["web"]),
    ]
    nodes = []
    for name, stage in [("orders/*.json", 0), ("pos/*.csv", 0), ("inventory/*.parquet", 0), ("web_events/*.json", 0),
                        ("bronze.orders_raw", 1), ("bronze.pos_raw", 1), ("bronze.inventory_raw", 1), ("bronze.web_events_raw", 1),
                        ("silver.order_lines", 2), ("silver.inventory", 2), ("silver.web_sessions", 2), ("quarantine", 2),
                        ("gold.daily_sales", 3), ("gold.inventory_health", 3), ("gold.web_funnel", 3)]:
        n = dict(name=name, stage=stage + 1)
        if name == "quarantine":
            n["status"] = "serious"
        nodes.append(n)

    # Auto Loader: new files per hour, last 7 days
    hours, files = [], []
    for d in days[-7:]:
        for h in range(24):
            open_hours = 8 <= h <= 22
            f = 175 * g.jitter(0.06) + (560 * g.jitter(0.04) if open_hours else 18 * g.jitter(0.3)) + (140 if h == 2 else 0)
            hours.append("%s %02d:00" % (day_label(d), h))
            files.append(r0(f))

    # Pipeline updates: 24 a day; moved to serverless on Sep 26
    p50s, p95s = [], []
    for i, d in enumerate(days):
        faster = 0.86 if d >= dt.date(2026, 9, 26) else 1.0
        load = bronze_daily[i] / 6.0e6
        p50 = (420 + 40 * load) * faster * g.jitter(0.04)
        p95 = p50 * 1.55 * g.jitter(0.05)
        if d == dt.date(2026, 9, 24):
            p50, p95 = 786, 1278
        p50s.append(r0(p50))
        p95s.append(r0(p95))
    med_dur = quantile(p50s, 0.5)

    revenue_total = sum(sum(r) for r in rev_by_cat)
    revenue_daily = [sum(rev_by_cat[c][i] for c in range(len(CATS))) for i in range(30)]
    pass_rate = 1 - quarantined / bronze_total

    kpis = [
        dict(label="Rows into bronze", value=bronze_total, fmt="compact", delta=0.042, deltaLabel="vs prior 30 days", spark=bronze_daily),
        dict(label="Passing expectations", value=r4(pass_rate), fmt="pct2", delta=0.0009, deltaLabel="vs prior 30 days"),
        dict(label="Rows quarantined", value=quarantined, fmt="compact", delta=-0.121, upIsGood=False, deltaLabel="vs prior 30 days"),
        dict(label="Update success", value=r4(718 / 720), fmt="pct", status="good", statusLabel="2 of 720 failed, both retried"),
        dict(label="Median update", value=med_dur, fmt="dur", delta=-0.083, upIsGood=False, deltaLabel="vs prior 30 days", spark=p50s),
        dict(label="Gold revenue", value=revenue_total, fmt="usd", delta=pct_change(revenue_total, revenue_total / 1.031), deltaLabel="vs prior 30 days", spark=revenue_daily),
    ]

    reason_names = {
        "known_store_id": "Unknown store_id", "positive_quantity": "Quantity not positive", "valid_order_ts": "Bad order timestamp",
        "has_session_id": "Missing session_id", "valid_event_ts": "Bad event timestamp", "non_negative_on_hand": "Negative stock on hand",
    }
    reasons.sort(key=lambda x: -x[1])

    charts = dict(
        flow=dict(
            kind="sankey", nodes=nodes, links=[dict(source=a, target=b, value=v) for a, b, v in links], fmt="compact", unitLabel="rows",
            desc="Rows moving from landing files through bronze and silver to gold over 30 days. About 0.7 percent of rows were quarantined.",
        ),
        revenue=dict(
            kind="bar", x=labels, fmt="usd", stack=True, labelEvery=6, xLabel="Day",
            desc="Daily revenue from gold.daily_sales, stacked by category. A fall sale lifted Sep 25 to 27.",
            series=[dict(name=c, data=rev_by_cat[i]) for i, c in enumerate(CATS)],
        ),
        reasons=dict(
            kind="bar", horizontal=True, x=[reason_names[r[0]] for r in reasons], fmt="compact", valueLabels=True, xLabel="Reason",
            desc="Rows quarantined in silver over 30 days, by the expectation they failed.",
            series=[dict(name="Rows quarantined", data=[r[1] for r in reasons])],
        ),
        files=dict(
            kind="line", x=hours, fmt="int", labelEvery=23, tickLabel="day", area=True, xLabel="Hour",
            desc="New files Auto Loader discovered per hour over the last 7 days. Store exports run from 8 am to 10 pm; inventory lands at 2 am.",
            series=[dict(name="Files", data=files)],
        ),
        duration=dict(
            kind="line", x=labels, fmt="dur", labelEvery=6, xLabel="Day", yMin=0,
            desc="Median and 95th percentile pipeline update time per day. Sep 24 was slow during a backfill; the pipeline moved to serverless on Sep 26.",
            series=[dict(name="p50", data=p50s), dict(name="p95", data=p95s)],
            thresholds=[dict(value=1200, label="SLA 20 min", status="critical")],
        ),
    )

    exp_table = dict(
        columns=[col("ds", "Dataset", "code"), col("rule", "Expectation", "code"), col("cons", "Constraint", "code"), col("action", "On failure"),
                 col("checked", "Rows checked", "compact"), col("failed", "Failed", "int"), col("pr", "Pass rate", "pct2"), col("st", "Status", "status")],
        rows=exp_rows,
    )
    catalog = dict(
        columns=[col("t", "Table", "code"), col("layer", "Layer"), col("type", "Type"), col("rows", "Rows, 30 days", "compact"),
                 col("size", "Size", "gb"), col("upd", "Last update"), col("st", "Freshness", "status")],
        rows=[
            dict(t="retail.bronze.orders_raw", layer="Bronze", type="Streaming table", rows=tot["orders"], size=r1(tot["orders"] * 1.9e-6), upd="Oct 6, 23:58", st=status("good", "On time")),
            dict(t="retail.bronze.pos_raw", layer="Bronze", type="Streaming table", rows=tot["pos"], size=r1(tot["pos"] * 0.9e-6), upd="Oct 6, 23:58", st=status("good", "On time")),
            dict(t="retail.bronze.web_events_raw", layer="Bronze", type="Streaming table", rows=tot["web"], size=r1(tot["web"] * 1.1e-6), upd="Oct 6, 23:58", st=status("good", "On time")),
            dict(t="retail.silver.order_lines", layer="Silver", type="Streaming table", rows=tot["orders"] + tot["pos"] - q["orders"] - q["pos"], size=r1((tot["orders"] + tot["pos"]) * 0.55e-6), upd="Oct 6, 23:59", st=status("good", "On time")),
            dict(t="retail.silver.inventory", layer="Silver", type="Streaming table", rows=tot["inventory"] - q["inventory"], size=r1(tot["inventory"] * 0.4e-6), upd="Oct 6, 02:21", st=status("good", "On time")),
            dict(t="retail.gold.daily_sales", layer="Gold", type="Materialized view", rows=150, size=0.1, upd="Oct 7, 00:04", st=status("good", "On time")),
            dict(t="retail.gold.inventory_health", layer="Gold", type="Materialized view", rows=588_000, size=0.2, upd="Oct 6, 02:34", st=status("good", "On time")),
            dict(t="retail.gold.web_funnel", layer="Gold", type="Materialized view", rows=30, size=0.1, upd="Oct 7, 00:16", st=status("warning", "12 min late")),
        ],
    )

    header = ["date", "category", "order_lines", "units_sold", "revenue_usd", "avg_line_value_usd"]
    rows = []
    for i, d in enumerate(days):
        for c, name in enumerate(CATS):
            n = lines_by_cat[c][i]
            rows.append([d.isoformat(), name, n, units_by_cat[c][i], rev_by_cat[c][i], round(rev_by_cat[c][i] / n, 2)])

    return dict(kpis=kpis, charts=charts, tables=dict(expectations=exp_table, catalog=catalog)), header, rows


PANELS = [
    dict(chart="flow", span=12, h=400, title="How rows moved through the layers", sub="Landing files → bronze → silver (or quarantine) → gold, last 30 days"),
    dict(chart="revenue", span=8, h=300, title="Revenue by category", sub="Daily, from `gold.daily_sales`"),
    dict(chart="reasons", span=4, h=300, title="Why rows were quarantined", sub="Silver expectations that rows failed"),
    dict(chart="files", span=6, h=260, title="Auto Loader: new files per hour", sub="Last 7 days, all four landing folders"),
    dict(chart="duration", span=6, h=260, title="Pipeline update time", sub="p50 and p95 per day, 24 updates a day"),
    dict(table="expectations", span=12, title="Data quality expectations", sub="From the pipeline event log, last 30 days"),
    dict(table="catalog", span=12, title="Tables in Unity Catalog", sub="Freshness checked against each table's SLA"),
]

FLOW = [
    ("Land", "Cloud storage", "Web orders as JSON, POS exports as CSV from 140 stores, inventory snapshots as Parquet and web events, all in one landing bucket."),
    ("Ingest", "Auto Loader", "`cloudFiles` streams every new file exactly once, infers and evolves the schema, and keeps surprises in `_rescued_data`."),
    ("Bronze", "Delta Lake", "Rows exactly as they arrived, plus the source file and ingest time. Nothing is dropped here."),
    ("Silver", "Lakeflow pipeline", "Typed, deduplicated and conformed. Expectations quarantine rows that break the rules and log every failure."),
    ("Gold", "Materialized views", "Daily sales, inventory health and the web funnel, refreshed incrementally each update."),
    ("Serve", "Unity Catalog + SQL warehouse", "Lineage and access control on every table; BI tools and this dashboard query gold."),
]

STEPS = [
    ("Bronze keeps everything.", "Auto Loader writes each file as-is with `_source_file` and `_ingested_at`. Columns nobody expected land in `_rescued_data`, so a row can always be traced to its file."),
    ("Expectations decide, not exceptions.", "Unusable rows are dropped to quarantine, odd but valid rows only warn, and a missing SKU fails the update, because that means the upstream contract broke."),
    ("Quarantine, then replay.", "Failed rows are kept with the rule they broke. Once the cause is fixed, for example a new store added to `silver.stores`, they are replayed into silver."),
    ("Customers keep their history.", "Customer changes from the CRM feed are applied as SCD type 2 with `apply_changes`, so reports can use the address that was valid at order time."),
]

MODELS = [
    dict(name="retail.bronze.orders_raw", kind="Streaming table · Auto Loader", columns=[
        ("order_id", "string", "As sent by the web store"),
        ("order_ts", "string", "Raw timestamp text, parsed in silver"),
        ("store_id / customer_id", "string", "Raw ids"),
        ("lines", "array<struct>", "SKU, quantity, unit price"),
        ("_rescued_data", "string", "Fields that did not match the schema"),
        ("_source_file / _ingested_at", "string / timestamp", "Where and when the row arrived"),
    ]),
    dict(name="retail.silver.order_lines", kind="Streaming table · expectations", columns=[
        ("order_id / line_no", "string / int", "Unique per line; duplicates removed"),
        ("order_ts", "timestamp", "Parsed and converted to UTC"),
        ("store_id", "int", "Must exist in `silver.stores`"),
        ("channel", "string", "`web` or `store`"),
        ("sku / category", "string", "Joined from the product dimension"),
        ("quantity / unit_price_usd", "int / decimal(10,2)", "Quantity must be positive"),
    ]),
    dict(name="retail.gold.daily_sales", kind="Materialized view · the CSV download", columns=[
        ("date", "date", "Order date, store local time"),
        ("category", "string", "Product category"),
        ("order_lines / units_sold", "bigint", "Lines and units sold"),
        ("revenue_usd", "decimal(14,2)", "Quantity × unit price"),
        ("avg_line_value_usd", "decimal(10,2)", "Revenue per line"),
    ]),
]

CODE = [
    dict(file="pipeline.py", lang="python",
         caption="The Lakeflow pipeline: Auto Loader into bronze, expectations and quarantine in silver, a materialized view in gold.",
         code='''
import dlt
from pyspark.sql import functions as F

LANDING = "s3://retail-landing"

RULES = {
    "known_store_id": "store_known",
    "positive_quantity": "quantity > 0",
    "valid_order_ts": "order_ts IS NOT NULL",
}
# A row goes to quarantine if any rule is false or null
QUARANTINE_IF = " OR ".join(f"NOT coalesce({rule}, false)" for rule in RULES.values())


# ---------- Bronze: every file, exactly once, as it arrived
@dlt.table(comment="Raw web orders from Auto Loader")
def orders_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{LANDING}/orders/")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# ---------- Silver: typed and checked; bad rows are routed, not lost
# (`stores` and `products` are defined in dimensions.py)
@dlt.view
def order_lines_checked():
    stores = dlt.read("stores").select("store_id", F.lit(True).alias("store_known"))
    lines = (
        dlt.read_stream("orders_raw")
        .withColumn("line", F.explode("lines"))
        .select(
            "order_id",
            F.col("line.line_no").cast("int").alias("line_no"),
            F.to_timestamp("order_ts").alias("order_ts"),
            F.col("store_id").cast("int").alias("store_id"),
            F.col("line.sku").alias("sku"),
            F.col("line.quantity").cast("int").alias("quantity"),
            F.col("line.unit_price").cast("decimal(10,2)").alias("unit_price_usd"),
        )
    )
    return (
        lines.join(F.broadcast(stores), "store_id", "left")
        .withColumn("store_known", F.coalesce("store_known", F.lit(False)))
        .withColumn("is_quarantined", F.expr(QUARANTINE_IF))
    )


@dlt.table(comment="Clean order lines")
@dlt.expect_all_or_drop(RULES)
def order_lines():
    return (
        dlt.read_stream("order_lines_checked")
        .withWatermark("order_ts", "2 days")
        .dropDuplicatesWithinWatermark(["order_id", "line_no"])
        .drop("store_known", "is_quarantined")
    )


@dlt.table(comment="Rows that failed a silver rule, kept for replay")
def order_lines_quarantine():
    return dlt.read_stream("order_lines_checked").where("is_quarantined")


# ---------- Gold: refreshed incrementally on each update
@dlt.table(comment="Daily sales by category")
def daily_sales():
    return (
        dlt.read("order_lines")
        .join(dlt.read("products"), "sku")
        .groupBy(F.to_date("order_ts").alias("date"), "category")
        .agg(
            F.count("*").alias("order_lines"),
            F.sum("quantity").alias("units_sold"),
            F.sum(F.col("quantity") * F.col("unit_price_usd")).alias("revenue_usd"),
        )
        .withColumn("avg_line_value_usd", F.round(F.col("revenue_usd") / F.col("order_lines"), 2))
    )
'''),
    dict(file="databricks.yml", lang="yaml",
         caption="Databricks Asset Bundle: the pipeline and its hourly job, deployed from CI to dev and prod.",
         code='''
bundle:
  name: retail-lakehouse

variables:
  catalog:
    default: retail_dev

resources:
  pipelines:
    retail_medallion:
      name: retail-medallion-${bundle.target}
      catalog: ${var.catalog}
      schema: lakehouse
      serverless: true
      photon: true
      channel: CURRENT
      libraries:
        - file:
            path: ./src/pipeline.py
      notifications:
        - email_recipients: [data-alerts@example.com]
          alerts: [on-update-failure, on-flow-failure]

  jobs:
    retail_medallion_hourly:
      name: retail-medallion-hourly-${bundle.target}
      schedule:
        quartz_cron_expression: "0 0 * * * ?"
        timezone_id: America/Chicago
      tasks:
        - task_key: refresh
          pipeline_task:
            pipeline_id: ${resources.pipelines.retail_medallion.id}
          max_retries: 1

targets:
  dev:
    mode: development
    default: true
  prod:
    mode: production
    variables:
      catalog: retail
'''),
    dict(file="expectations.sql", lang="sql",
         caption="Reads the pipeline event log to produce the expectations table on this page.",
         code='''
-- Pass and fail counts per expectation, from the pipeline's event log
WITH progress AS (
  SELECT
    timestamp,
    explode(
      from_json(
        details:flow_progress:data_quality:expectations,
        'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>'
      )
    ) AS e
  FROM event_log(TABLE(retail.lakehouse.order_lines))
  WHERE event_type = 'flow_progress'
    AND timestamp >= current_timestamp() - INTERVAL 30 DAYS
)
SELECT
  e.dataset,
  e.name                                   AS expectation,
  sum(e.passed_records) + sum(e.failed_records) AS rows_checked,
  sum(e.failed_records)                    AS failed,
  round(sum(e.passed_records) / nullif(sum(e.passed_records) + sum(e.failed_records), 0), 4) AS pass_rate
FROM progress
GROUP BY e.dataset, e.name
ORDER BY failed DESC;
'''),
]

HIGHLIGHTS = [
    ("A schema change that broke nothing",
     "On Sep 22 the web store added `promo_code` to its order JSON. Auto Loader added the column to bronze, the pipeline restarted once on its own, and silver picked the field up after a one-line change. No rows were lost."),
    ("Quarantine is down 12%",
     "Most quarantined rows came from two stores that sent an unknown `store_id` after a POS upgrade. Adding them to `silver.stores` and replaying the quarantine table recovered about 96K rows."),
    ("The slow day",
     "On Sep 24, update p95 reached 21 minutes while a fixed-size cluster scaled for a backfill. Moving the pipeline to serverless on Sep 26 brought p95 back to about 11 minutes."),
    ("Gold stays cheap",
     "Gold tables are materialized views, so most hourly updates only reprocess the current day instead of recomputing 30 days of sales."),
]
