"""Project 06: Unified streaming + batch pipeline for clickstream data."""

import datetime as dt

from common import bump24, col, day_label, day_list, pct_change, r0, r4, status

META = dict(
    num="06",
    slug="unified-streaming-batch",
    seed=606,
    title="Unified Streaming and Batch Pipeline",
    short="Streaming + Batch",
    category="Streaming + batch",
    tagline="Web and mobile clickstream processed in real time by Flink and recomputed nightly by Spark, both writing the same Apache Iceberg tables so the numbers agree.",
    tile_stack="Kafka · Kinesis · Flink · Iceberg · Spark",
    summary=(
        "Web events arrive through Kafka and mobile app events through Amazon Kinesis. A Flink job deduplicates and sessionizes both streams in real time "
        "and writes events and sessions to Apache Iceberg, so product dashboards run a few seconds behind users. Every night a Spark job recomputes the "
        "previous day from the raw events, including the ones that arrived late, and atomically replaces those partitions of the same tables. "
        "One set of tables, one definition of a session, and two engines that check each other."
    ),
    stack=["Apache Kafka", "Amazon Kinesis", "Apache Flink", "Apache Iceberg", "Apache Spark"],
    window="Tuesday, Oct 6, 2026 · reconciliation over the last 14 days",
    csv="events_hourly.csv",
    csv_desc="Web and mobile events, sessions, purchases and late events per hour for 14 days",
    how_note="The stream is fast, the batch is complete, and both write the same tables.",
    model_note="Both engines write these Iceberg tables. The CSV download is the hourly rollup for the last 14 days.",
    code_note="The Flink sessionization job, the nightly Spark recompute, and the query that reconciles them.",
)

END = dt.date(2026, 10, 6)
CHANNELS = [("Organic search", 0.328), ("Direct", 0.246), ("Paid search", 0.152), ("Email", 0.091),
            ("Social", 0.079), ("Referral", 0.056), ("Push notification", 0.048)]
PAGES = [("/", 0.21, 0.31, 38), ("/search", 0.17, 0.12, 52), ("/p/{product}", 0.33, 0.27, 74), ("/c/{category}", 0.14, 0.22, 46),
         ("/cart", 0.07, 0.29, 61), ("/checkout", 0.04, 0.18, 143), ("/account", 0.04, 0.41, 35)]


def build(g):
    days = day_list(END, 14)
    web_h, mob_h, sess_h, purch_h, late_h, csv_rows = [], [], [], [], [], []
    for d in days:
        weekend = d.weekday() >= 5
        for h in range(24):
            web = (520_000 + 1_650_000 * bump24(h, 13.2, 3.0) + 1_500_000 * bump24(h, 20.3, 2.2)) * (0.92 if weekend else 1.0) * g.jitter(0.04)
            mob = (380_000 + 900_000 * bump24(h, 8.2, 1.6) + 1_400_000 * bump24(h, 21.0, 2.4) + 500_000 * bump24(h, 12.5, 2.0)) * (1.15 if weekend else 1.0) * g.jitter(0.04)
            events = web + mob
            sessions = events / 15.5 * g.jitter(0.02)
            purchases = sessions * 0.032 * (1.1 if 19 <= h <= 22 else 1.0) * g.jitter(0.05)
            late = mob * 0.0135 * g.jitter(0.1) + web * 0.0021 * g.jitter(0.1)
            if d == END:
                web_h.append(r0(web)); mob_h.append(r0(mob)); sess_h.append(r0(sessions)); purch_h.append(r0(purchases)); late_h.append(r0(late))
            csv_rows.append(["%sT%02d:00:00-05:00" % (d.isoformat(), h), r0(web), r0(mob), r0(sessions), r0(purchases), r0(late)])

    events = sum(web_h) + sum(mob_h)
    sessions = sum(sess_h)
    purchases = sum(purch_h)
    late = sum(late_h)
    prior = [r for r in csv_rows if r[0].startswith(days[-8].isoformat())]
    prior_events = sum(r[1] + r[2] for r in prior)
    prior_sessions = sum(r[3] for r in prior)
    prior_purch = sum(r[4] for r in prior)

    # Daily stream vs batch difference; Sep 30 had a 9-minute gap during a Flink upgrade
    diffs, diff_status = [], []
    for d in days:
        v = abs(g.gauss(0.00022, 0.0003)) + 0.00008
        if d == dt.date(2026, 9, 30):
            v = 0.0014
        diffs.append(round(v, 5))
        diff_status.append("warning" if v > 0.001 else None)

    funnel = [("Sessions", sessions), ("Viewed a product", sessions * 0.581), ("Added to cart", sessions * 0.121),
              ("Started checkout", sessions * 0.049), ("Purchased", purchases)]

    buckets = ["< 1 s", "1–5 s", "5–30 s", "30 s–1 min", "1–5 min", "5–30 min", "30 min–2 h", "> 2 h"]
    shares = [0.781, 0.168, 0.031, 0.0078, 0.0042, 0.0049, 0.0023, 0.0008]
    tot = sum(shares)
    lat_counts = [r0(events * s / tot * g.jitter(0.02)) for s in shares]
    within = [v if i < 5 else None for i, v in enumerate(lat_counts)]
    after = [v if i >= 5 else None for i, v in enumerate(lat_counts)]
    late_share = sum(lat_counts[5:]) / events

    hours = ["%02d:00" % h for h in range(24)]
    kpis = [
        dict(label="Events", value=events, fmt="compact", delta=pct_change(events, prior_events), deltaLabel="vs Sep 29", spark=[a + b for a, b in zip(web_h, mob_h)]),
        dict(label="Sessions", value=sessions, fmt="compact", delta=pct_change(sessions, prior_sessions), deltaLabel="vs Sep 29", spark=sess_h),
        dict(label="Conversion rate", value=r4(purchases / sessions), fmt="pct2", delta=pct_change(purchases / sessions, prior_purch / prior_sessions), deltaLabel="vs Sep 29"),
        dict(label="Stream vs batch gap", value=round(sum(diffs) / len(diffs), 5), fmt="pct2", status="good", statusLabel="14-day average, limit 0.1%"),
        dict(label="Late events", value=r4(late_share), fmt="pct", note="After the 5-minute watermark; added nightly"),
        dict(label="Freshness p95", value=4.1, fmt="sec", status="good", statusLabel="Event to Iceberg, under 10 s"),
    ]

    charts = dict(
        events=dict(
            kind="bar", x=hours, fmt="compact", stack=True, labelEvery=2, xLabel="Hour",
            desc="Events per hour on Oct 6, stacked by source: web events through Kafka and mobile events through Kinesis.",
            series=[dict(name="Web · Kafka", data=web_h), dict(name="Mobile · Kinesis", data=mob_h)],
        ),
        funnel=dict(
            kind="funnel", steps=[dict(name=n, value=r0(v)) for n, v in funnel], fmt="compact", unitLabel="sessions",
            desc="Sessions on Oct 6 that reached each step of the purchase funnel.",
        ),
        recon=dict(
            kind="bar", x=[day_label(d) for d in days], fmt="pct2", xLabel="Day", labelEvery=1,
            desc="Difference between the streaming and the batch session counts for each day. Sep 30 went over the 0.1 percent limit during a Flink upgrade and was corrected by the batch.",
            series=[dict(name="Difference", data=diffs, itemStatus=diff_status)],
            thresholds=[dict(value=0.001, label="Limit 0.1%", status="warning")],
        ),
        lateness=dict(
            kind="bar", x=buckets, fmt="compact", log=True, yMin=10_000, overlap=True, xLabel="Arrival delay",
            desc="How late events arrived on Oct 6, on a log scale. Events more than 5 minutes late miss the streaming watermark and are added by the nightly batch.",
            series=[dict(name="Within the watermark", data=within), dict(name="Added by the nightly batch", data=after)],
        ),
        channels=dict(
            kind="bar", horizontal=True, x=[c[0] for c in CHANNELS], fmt="compact", valueLabels=True, xLabel="Channel",
            desc="Sessions on Oct 6 by acquisition channel.",
            series=[dict(name="Sessions", data=[r0(sessions * c[1] * g.jitter(0.02)) for c in CHANNELS])],
        ),
    )

    page_rows = []
    views_total = sum(sess_h) * 4.2
    for path, share, exit_rate, avg_s in PAGES:
        views = views_total * share * g.jitter(0.03)
        page_rows.append(dict(p=path, v=r0(views), u=r0(views * 0.61 * g.jitter(0.03)), t=avg_s, x=exit_rate))
    page_rows.sort(key=lambda r: -r["v"])

    tables = dict(
        pages=dict(
            columns=[col("p", "Page", "code"), col("v", "Views", "compact"), col("u", "Unique visitors", "compact"),
                     col("t", "Avg time", "dur"), col("x", "Exit rate", "pct")],
            rows=page_rows,
        ),
        jobs=dict(
            columns=[col("j", "Job", "code"), col("e", "Engine"), col("m", "Mode"), col("last", "Last run"),
                     col("d", "Detail"), col("st", "Status", "status")],
            rows=[
                dict(j="sessionize-clickstream", e="Flink 1.20", m="Streaming", last="Running since Sep 30, 02:14", d="Checkpoint every 30 s, 1.4 GB state", st=status("good", "Running")),
                dict(j="nightly_recompute", e="Spark 3.5", m="Batch", last="Oct 7, 01:40 · 38 min", d="Replaced 2 partitions of sessions", st=status("good", "Succeeded")),
                dict(j="reconcile_stream_batch", e="Spark SQL", m="Batch", last="Oct 7, 02:21 · 3 min", d="Largest difference 0.02%", st=status("good", "Succeeded")),
                dict(j="iceberg_maintenance", e="Spark 3.5", m="Hourly", last="Oct 7, 02:15 · 6 min", d="Compacted 1,204 files", st=status("good", "Succeeded")),
                dict(j="late_events_report", e="Spark SQL", m="Daily", last="Oct 7, 02:30 · 1 min", d="Mobile app 8.3 sends 61% of late events", st=status("warning", "Needs follow-up")),
            ],
        ),
    )

    header = ["hour_start", "web_events", "mobile_events", "sessions", "purchases", "late_events"]
    return dict(kpis=kpis, charts=charts, tables=tables), header, csv_rows


PANELS = [
    dict(chart="events", span=8, h=290, title="Events per hour", sub="Oct 6, by source"),
    dict(chart="funnel", span=4, h=290, title="Purchase funnel", sub="Sessions reaching each step, Oct 6"),
    dict(chart="recon", span=6, h=260, title="Stream vs batch difference", sub="Session counts per day; the nightly batch is the reference"),
    dict(chart="lateness", span=6, h=260, title="How late events arrive", sub="Delay from event time to arrival, log scale"),
    dict(chart="channels", span=5, h=300, title="Sessions by channel", sub="Oct 6"),
    dict(table="pages", span=7, title="Top pages", sub="Oct 6, web and app combined"),
    dict(table="jobs", span=12, title="Jobs", sub="State at 02:30 on Oct 7"),
]

FLOW = [
    ("Collect", "Web and mobile SDKs", "Page views, searches, cart changes and purchases, each with a client event id and timestamp."),
    ("Ingest", "Kafka · Kinesis", "Web events land in Kafka. The mobile backend already runs on AWS, so app events arrive through Kinesis."),
    ("Stream", "Apache Flink", "Reads both streams, drops duplicates by event id, groups events into sessions with a 30-minute gap and commits every 30 s."),
    ("Store", "Apache Iceberg", "One set of tables for both engines: events, sessions and funnel steps, partitioned by day."),
    ("Recompute", "Apache Spark, nightly", "Rebuilds yesterday from raw events, late arrivals included, and replaces just those partitions."),
    ("Reconcile", "Spark SQL", "Compares the streaming and batch results per day and alerts above 0.1%."),
]

STEPS = [
    ("One definition of a session.", "Both engines use the same rules: dedupe on `event_id`, a 30-minute inactivity gap and the same session id, so on-time data gives the same answer in Flink and Spark."),
    ("Late data is expected.", "Flink's watermark waits 5 minutes. Anything later is still stored in the raw events table within seconds, and the nightly recompute adds it to sessions."),
    ("Replace, don't append.", "The batch job uses Iceberg's dynamic partition overwrite, so a recomputed day replaces the old one in a single commit. Readers see either the old day or the new one, never a mix."),
    ("The engines check each other.", "A daily job compares counts by day and platform, using Iceberg time travel to read the streaming result from before the overwrite."),
]

MODELS = [
    dict(name="clickstream.events", kind="Iceberg · appended by Flink · days(event_ts)", columns=[
        ("event_id", "string", "Client-generated id; duplicates are dropped"),
        ("anonymous_id / user_id", "string", "Device id, and the account id once known"),
        ("platform / source", "string", "`web` or `ios`/`android`; `kafka` or `kinesis`"),
        ("event_type", "string", "`page_view`, `search`, `add_to_cart`, `purchase` and others"),
        ("page", "string", "Route template, for example `/p/{product}`"),
        ("event_ts / ingest_ts", "timestamp", "When it happened and when it arrived"),
    ]),
    dict(name="clickstream.sessions", kind="Iceberg · Flink writes, Spark replaces nightly", columns=[
        ("session_id", "string", "md5 of `anonymous_id` and the session start"),
        ("anonymous_id", "string", "Device"),
        ("platform / channel", "string", "Where the session happened and how it started"),
        ("started_at / ended_at", "timestamp", "First event, and last event plus the 30-minute gap"),
        ("events / page_views", "int", "Activity in the session"),
        ("converted", "boolean", "True when the session has a purchase"),
    ]),
    dict(name="events_hourly", kind="Rollup · the CSV download", columns=[
        ("hour_start", "timestamp", "Hour, US Central"),
        ("web_events / mobile_events", "bigint", "Events by source"),
        ("sessions / purchases", "bigint", "Sessions started and purchases made"),
        ("late_events", "bigint", "Events that arrived after the watermark"),
    ]),
]

CODE = [
    dict(file="sessionize.sql", lang="sql",
         caption="Flink SQL: both sources unioned, deduplicated by `event_id`, and grouped into sessions with a 30-minute gap.",
         code='''
CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  'warehouse' = 's3://clickstream-lake/warehouse'
);

CREATE TEMPORARY TABLE web_events (
  event_id STRING, anonymous_id STRING, user_id STRING, event_type STRING, page STRING,
  event_ts TIMESTAMP_LTZ(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'web.events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sessionize',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'latest',
  'format' = 'json'
);

CREATE TEMPORARY TABLE mobile_events (
  event_id STRING, anonymous_id STRING, user_id STRING, event_type STRING, page STRING, os STRING,
  event_ts TIMESTAMP_LTZ(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' MINUTE
) WITH (
  'connector' = 'kinesis',
  'stream.arn' = 'arn:aws:kinesis:us-east-1:111122223333:stream/mobile-events',
  'aws.region' = 'us-east-1',
  'source.init.position' = 'LATEST',
  'format' = 'json'
);

-- keep the first copy of each event (state expires after an hour)
SET 'table.exec.state.ttl' = '1 h';

CREATE TEMPORARY VIEW all_events AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time) AS rn
  FROM (
    SELECT event_id, anonymous_id, user_id, event_type, page, 'web' AS platform, event_ts, PROCTIME() AS proc_time FROM web_events
    UNION ALL
    SELECT event_id, anonymous_id, user_id, event_type, page, os AS platform, event_ts, PROCTIME() AS proc_time FROM mobile_events
  )
) WHERE rn = 1;

INSERT INTO lake.clickstream.sessions
SELECT
  MD5(CONCAT(anonymous_id, '|', DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss.SSS'))) AS session_id,
  anonymous_id,
  FIRST_VALUE(platform)                                AS platform,
  window_start                                         AS started_at,
  window_end                                           AS ended_at,
  COUNT(*)                                             AS events,
  COUNT(*) FILTER (WHERE event_type = 'page_view')     AS page_views,
  MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) = 1 AS converted
FROM TABLE(
  SESSION(TABLE all_events PARTITION BY anonymous_id, DESCRIPTOR(event_ts), INTERVAL '30' MINUTES)
)
GROUP BY window_start, window_end, anonymous_id;
'''),
    dict(file="recompute_day.py", lang="python",
         caption="Spark, nightly: rebuild one day of sessions from raw events, late arrivals included, and replace that day in one commit.",
         code='''
"""Recompute sessions for one day from raw events. Usage: recompute_day.py 2026-10-06"""
import sys

from pyspark.sql import SparkSession, Window, functions as F

day = sys.argv[1]
spark = SparkSession.builder.appName(f"recompute-sessions-{day}").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

events = (
    spark.table("lake.clickstream.events")
    .where(F.to_date("event_ts") == F.lit(day))
    .dropDuplicates(["event_id"])
)

by_device = Window.partitionBy("anonymous_id").orderBy("event_ts")
gap_s = F.col("event_ts").cast("long") - F.lag(F.col("event_ts").cast("long")).over(by_device)

sessions = (
    events
    .withColumn("is_new", F.when(gap_s.isNull() | (gap_s >= 30 * 60), 1).otherwise(0))
    .withColumn("session_n", F.sum("is_new").over(by_device))
    .groupBy("anonymous_id", "session_n")
    .agg(
        F.min("event_ts").alias("started_at"),
        F.max("event_ts").alias("last_event_at"),
        F.first("platform", ignorenulls=True).alias("platform"),
        F.count("*").alias("events"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_views"),
        F.max(F.col("event_type") == "purchase").alias("converted"),
    )
    # same id and end time as the streaming job
    .withColumn("session_id", F.md5(F.concat_ws("|", "anonymous_id", F.date_format("started_at", "yyyy-MM-dd HH:mm:ss.SSS"))))
    .withColumn("ended_at", F.col("last_event_at") + F.expr("INTERVAL 30 MINUTES"))
    .select("session_id", "anonymous_id", "platform", "started_at", "ended_at", "events", "page_views", "converted")
)

# Replaces only the partitions present in `sessions` (this day), atomically.
sessions.writeTo("lake.clickstream.sessions").overwritePartitions()
'''),
    dict(file="reconcile.sql", lang="sql",
         caption="Spark SQL with Iceberg time travel: the streaming result (before the overwrite) against the batch result (now).",
         code='''
WITH stream AS (
  SELECT platform, count(*) AS sessions
  FROM lake.clickstream.sessions TIMESTAMP AS OF '2026-10-07 01:39:00'
  WHERE started_at >= '2026-10-06' AND started_at < '2026-10-07'
  GROUP BY platform
),
batch AS (
  SELECT platform, count(*) AS sessions
  FROM lake.clickstream.sessions
  WHERE started_at >= '2026-10-06' AND started_at < '2026-10-07'
  GROUP BY platform
)
SELECT
  b.platform,
  s.sessions                                               AS stream_sessions,
  b.sessions                                               AS batch_sessions,
  round(abs(b.sessions - s.sessions) / b.sessions, 5)      AS difference,
  abs(b.sessions - s.sessions) / b.sessions > 0.001        AS over_limit
FROM batch AS b
JOIN stream AS s USING (platform)
ORDER BY difference DESC;
'''),
]

HIGHLIGHTS = [
    ("The Sep 30 upgrade",
     "Flink was upgraded and restarted from a savepoint. Nine minutes of mobile events were processed after their sessions had closed, so the streaming count for that day was 0.14% low until the nightly recompute corrected it."),
    ("Late, not lost",
     "About 0.8% of events arrive after the 5-minute watermark, mostly from mobile apps that were offline. They are in the raw events table within seconds and in sessions after the nightly run."),
    ("Seconds behind users",
     "Product dashboards read sessions about 4 seconds behind real time at p95, instead of waiting for a nightly job."),
    ("Cheap to reprocess",
     "Recomputing one day takes about 38 minutes. A month-long backfill runs as one job per day, in parallel, and each day lands as its own commit."),
]
