"""Project 03: CDC pipeline, PostgreSQL to Snowflake."""

from common import bump24, col, five_min_labels, pct_change, quantile, r0, r1, status

META = dict(
    num="03",
    slug="cdc-pipeline",
    seed=303,
    title="CDC Pipeline: Postgres to Snowflake",
    short="CDC Pipeline",
    category="Change data capture",
    tagline="Every insert, update and delete in an orders database captured by Debezium, streamed through Kafka and Flink, and queryable in Snowflake within seconds.",
    tile_stack="Debezium · Kafka · Flink · Snowflake",
    summary=(
        "The order service writes to PostgreSQL. Debezium reads the database's write-ahead log through logical replication, so every insert, update and delete "
        "becomes a Kafka event without touching the application or querying its tables. A Flink job flattens and cleans the changes while keeping them in order per row, "
        "Snowpipe Streaming lands them in Snowflake within seconds, and Dynamic Tables merge them into copies of the source tables that stay in sync row for row."
    ),
    stack=["PostgreSQL", "Debezium", "Apache Kafka", "Apache Flink", "Snowflake"],
    window="Tuesday, Oct 6, 2026 · 24 hours, US Central",
    csv="change_events_5min.csv",
    csv_desc="Inserts, updates, deletes and replication lag in 5-minute intervals",
    how_note="Log-based capture: the source database never sees an extra query.",
    model_note="The same order row at three points: the Debezium event in Kafka, the change row in Snowflake, and the merged table.",
    code_note="The Debezium connector, the Flink SQL that normalizes changes, and the Snowflake merge.",
)

TABLES = [
    # name, share of events, insert/update/delete mix, rows in source
    ("inventory", 0.215, (0.05, 0.94, 0.01), 2_418_337),
    ("order_items", 0.172, (0.71, 0.24, 0.05), 96_204_118),
    ("orders", 0.121, (0.42, 0.57, 0.01), 31_877_450),
    ("payments", 0.098, (0.55, 0.45, 0.0), 33_102_966),
    ("shipments", 0.091, (0.36, 0.64, 0.0), 29_460_212),
    ("prices", 0.088, (0.02, 0.97, 0.01), 1_284_005),
    ("customers", 0.064, (0.21, 0.78, 0.01), 7_912_640),
    ("addresses", 0.047, (0.48, 0.47, 0.05), 11_046_381),
    ("returns", 0.038, (0.62, 0.38, 0.0), 2_211_904),
    ("promotions", 0.028, (0.18, 0.62, 0.2), 48_119),
    ("products", 0.022, (0.08, 0.9, 0.02), 186_402),
    ("carriers", 0.016, (0.01, 0.99, 0.0), 214),
]


def build(g):
    labels = five_min_labels()
    ins, upd, dele, lag50, lag95 = [], [], [], [], []
    for i in range(288):
        h = i / 12.0
        base = 5200 + 21000 * bump24(h, 12.6, 2.8) + 26000 * bump24(h, 20.1, 2.1) + 9000 * bump24(h, 9.0, 1.4)
        n = base * g.jitter(0.05)
        bulk = 0
        if 25 <= i <= 28:  # 02:05-02:20, nightly price rewrite: 1.2M updates in one transaction
            bulk = [180_000, 420_000, 390_000, 210_000][i - 25]
        load = (n + bulk) / 60000.0
        ins.append(r0(n * 0.37))
        upd.append(r0(n * 0.59 + bulk))
        dele.append(r0(n * 0.04))
        p50 = 1.2 + 0.9 * min(load, 1.2) + g.gauss(0.12)
        p95 = 2.9 + 2.1 * min(load, 1.2) + abs(g.gauss(0.3))
        if bulk:
            p50, p95 = [(2.4, 5.1), (3.2, 7.9), (3.0, 7.4), (2.1, 4.8)][i - 25]
        lag50.append(r1(p50))
        lag95.append(r1(p95))

    total = [a + b + c for a, b, c in zip(ins, upd, dele)]
    events = sum(total)
    hourly_total = [sum(total[h * 12:(h + 1) * 12]) for h in range(24)]
    peak_rate = max(total) / 300.0
    hours = ["%02d:00" % h for h in range(24)]

    per_table = []
    for name, share, mix, rows_src in TABLES:
        n = events * share * g.jitter(0.02)
        if name == "prices":
            n += 1_200_000 * 0.93
        per_table.append((name, r0(n), mix, rows_src))
    per_table.sort(key=lambda t: -t[1])

    kpis = [
        dict(label="Change events", value=events, fmt="compact", delta=0.034, deltaLabel="vs Sep 29", spark=hourly_total),
        dict(label="Median lag", value=r1(quantile(lag50, 0.5)), fmt="sec", status="good", statusLabel="p95 under the 10 s SLA all day"),
        dict(label="Peak throughput", value=r0(peak_rate), fmt="persec", note="02:10, nightly price update"),
        dict(label="Tables replicated", value=len(TABLES), fmt="int", note="3 schema changes applied today"),
        dict(label="Connector uptime", value=0.9998, fmt="pct2", status="good", statusLabel="1 task restart, recovered"),
        dict(label="Snowflake credits", value=14.6, fmt="dec1", delta=-0.062, upIsGood=False, deltaLabel="vs Sep 29"),
    ]

    charts = dict(
        lag=dict(
            kind="line", x=labels, fmt="sec", labelEvery=35, xLabel="Time", yMin=0, yMax=12,
            desc="Seconds from a commit in PostgreSQL to the change row being queryable in Snowflake, p50 and p95 per 5 minutes, with the 10 second SLA.",
            series=[dict(name="p50", data=lag50), dict(name="p95", data=lag95)],
            thresholds=[dict(value=10, label="SLA 10 s", status="critical")],
        ),
        tables=dict(
            kind="bar", horizontal=True, x=[t[0] for t in per_table], fmt="compact", valueLabels=True, xLabel="Table",
            desc="Change events per source table on Oct 6. Prices include the nightly bulk update.",
            series=[dict(name="Change events", data=[t[1] for t in per_table])],
        ),
        ops=dict(
            kind="bar", x=hours, fmt="compact", stack=True, labelEvery=2, xLabel="Hour",
            desc="Inserts, updates and deletes per hour. The 02:00 hour is the nightly price update.",
            series=[dict(name="Inserts", data=[sum(ins[h * 12:(h + 1) * 12]) for h in range(24)]),
                    dict(name="Updates", data=[sum(upd[h * 12:(h + 1) * 12]) for h in range(24)]),
                    dict(name="Deletes", data=[sum(dele[h * 12:(h + 1) * 12]) for h in range(24)])],
        ),
    )

    recon_rows = []
    for name, _, _, rows_src in sorted(TABLES, key=lambda t: -t[3]):
        diff = 3 if name == "inventory" else 0
        st = status("neutral", "In flight") if diff else status("good", "Match")
        recon_rows.append(dict(t=name, pg=rows_src, sf=rows_src - diff, diff=diff, st=st))

    events_rows = []
    lsn = 0x3A7F2C10
    sample = [
        ("orders", "u", "order_id=88213407", "23:59:58.412", 1.21), ("order_items", "c", "item_id=301877122", "23:59:58.512", 1.34),
        ("payments", "c", "payment_id=55102344", "23:59:58.940", 1.18), ("inventory", "u", "sku=SKU-204417, store=118", "23:59:59.003", 1.62),
        ("shipments", "u", "shipment_id=7731209", "23:59:59.240", 1.07), ("addresses", "d", "address_id=2209817", "23:59:59.388", 1.45),
        ("customers", "u", "customer_id=4410982", "23:59:59.561", 1.29), ("orders", "c", "order_id=88213411", "23:59:59.702", 1.11),
        ("order_items", "c", "item_id=301877123", "23:59:59.702", 1.15), ("inventory", "u", "sku=SKU-118820, store=42", "23:59:59.950", 1.73),
    ]
    op_name = {"c": "insert", "u": "update", "d": "delete"}
    for t, op, key, ts, lag in reversed(sample):
        lsn += g.ri(400, 9000)
        events_rows.append(dict(lsn="%X/%08X" % (0x2E, lsn), t=t, op=op_name[op], key=key, ts=ts, lag=lag))
    events_rows.reverse()

    tables = dict(
        components=dict(
            columns=[col("c", "Component"), col("st", "Status", "status"), col("d", "Detail")],
            rows=[
                dict(c="Postgres replication slot", st=status("good", "Healthy"), d="212 MB WAL retained"),
                dict(c="Debezium source connector", st=status("good", "Running"), d="Restarted once, 14:32"),
                dict(c="Kafka cluster", st=status("good", "Healthy"), d="3 brokers, in sync"),
                dict(c="Kafka Connect cluster", st=status("good", "Healthy"), d="2 workers"),
                dict(c="Flink job cdc-normalize", st=status("good", "Running"), d="Parallelism 6"),
                dict(c="Snowflake Kafka connector", st=status("good", "Running"), d="Snowpipe Streaming"),
                dict(c="Dynamic tables (12)", st=status("good", "On target"), d="Target lag 1 min"),
            ],
        ),
        recon=dict(
            columns=[col("t", "Table", "code"), col("pg", "PostgreSQL rows", "int"), col("sf", "Snowflake rows", "int"),
                     col("diff", "Difference", "int"), col("st", "Status", "status")],
            rows=recon_rows,
        ),
        schema=dict(
            columns=[col("time", "Time"), col("t", "Table", "code"), col("chg", "Change", "code"), col("st", "Result", "status")],
            rows=[
                dict(time="09:12", t="orders", chg="ADD COLUMN promo_code", st=status("good", "In RAW, added to CORE")),
                dict(time="11:40", t="shipments", chg="carrier_code varchar(8→16)", st=status("good", "In RAW, nothing to do")),
                dict(time="16:05", t="customers", chg="ADD COLUMN marketing_opt_in", st=status("warning", "In RAW, CORE pending")),
            ],
        ),
        events=dict(
            columns=[col("lsn", "LSN", "code"), col("t", "Table", "code"), col("op", "Operation"), col("key", "Primary key", "code"),
                     col("ts", "Committed at"), col("lag", "Lag", "sec")],
            rows=events_rows,
        ),
    )

    header = ["interval_start", "inserts", "updates", "deletes", "events_total", "lag_p50_s", "lag_p95_s"]
    rows = [["2026-10-06T%s:00-05:00" % labels[i], ins[i], upd[i], dele[i], total[i], lag50[i], lag95[i]] for i in range(288)]
    return dict(kpis=kpis, charts=charts, tables=tables), header, rows


PANELS = [
    dict(chart="lag", span=8, h=290, title="Replication lag", sub="Commit in PostgreSQL to change row in Snowflake, p50 and p95"),
    dict(chart="tables", span=4, h=290, title="Changes by table", sub="Change events per source table today"),
    dict(chart="ops", span=7, h=280, title="Change operations per hour", sub="Inserts, updates and deletes"),
    dict(table="components", span=5, title="Pipeline components", sub="State at 23:59"),
    dict(table="recon", span=6, title="Source vs target row counts", sub="Checked every 15 minutes. The 3 inventory rows in flight matched at 00:01."),
    dict(table="schema", span=6, title="Schema changes today", sub="New columns land in RAW on their own"),
    dict(table="events", span=12, title="Latest change events", sub="The last ten changes committed on Oct 6"),
]

FLOW = [
    ("Source", "PostgreSQL 16", "The order service's database. Logical replication with the `pgoutput` plugin; one publication covers the 12 tables."),
    ("Capture", "Debezium on Kafka Connect", "Takes an initial snapshot, then reads the write-ahead log and emits every insert, update and delete with its LSN."),
    ("Transport", "Apache Kafka", "One topic per table, keyed by primary key so changes to a row stay in order. Each event carries the row before and after."),
    ("Normalize", "Apache Flink", "Reads all twelve topics and keeps what the merge needs: key, operation, LSN, commit time and the row, written exactly once."),
    ("Land", "Snowpipe Streaming", "The Snowflake Kafka connector appends every change to `RAW.CHANGES` within seconds."),
    ("Merge", "Snowflake Dynamic Tables", "One per source table: the newest version of each row, deleted rows removed, refreshed every minute."),
]

STEPS = [
    ("No load on the source.", "Debezium reads the WAL instead of polling tables, so the order service sees no extra queries. A heartbeat keeps the replication slot moving when tables are quiet."),
    ("Order per key.", "Topics are partitioned by primary key and every change carries its LSN, so the merge always keeps the newest version, even if a batch arrives late."),
    ("Deletes are real.", "A delete arrives with `op = 'd'` and the merge removes the row, so Snowflake never keeps rows the source has deleted."),
    ("Trust, then verify.", "Every 15 minutes a job compares row counts and a hash of key columns between PostgreSQL and Snowflake. That feeds the reconciliation table."),
]

MODELS = [
    dict(name="pg.public.orders", kind="Kafka topic · Debezium event (JSON)", columns=[
        ("before / after", "struct", "Row before and after the change; `before` is null for inserts"),
        ("op", "string", "`c` insert, `u` update, `d` delete, `r` snapshot read"),
        ("source.lsn", "bigint", "Position in the write-ahead log"),
        ("source.ts_ms", "bigint", "Commit time in PostgreSQL"),
        ("ts_ms", "bigint", "When Debezium processed the change"),
    ]),
    dict(name="RAW.CHANGES", kind="Snowflake table · append-only, all 12 tables", columns=[
        ("record_content:table_name / pk", "variant", "Source table and primary key"),
        ("record_content:op / is_deleted", "variant", "What happened to the row"),
        ("record_content:lsn", "variant", "Orders the changes to one row"),
        ("record_content:committed_ms", "variant", "Commit time in PostgreSQL, epoch ms"),
        ("record_content:row_json", "variant", "The whole row as JSON, so new columns arrive automatically"),
        ("record_metadata", "variant", "Kafka topic, partition, offset and connector push time"),
    ]),
    dict(name="CORE.ORDERS", kind="Dynamic table · target lag 1 minute", columns=[
        ("order_id", "number", "One row per live order"),
        ("customer_id", "number", "Customer"),
        ("status", "varchar", "Latest status"),
        ("total_usd", "number(12,2)", "Order total"),
        ("updated_at / lsn", "timestamp_ltz / number", "Version of the row that is current"),
    ]),
]

CODE = [
    dict(file="debezium-orders-pg.json", lang="json",
         caption="Kafka Connect configuration for the Debezium PostgreSQL source connector. A heartbeat keeps the replication slot advancing on quiet tables.",
         code='''
{
  "name": "debezium-orders-pg",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "orders-db.internal",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "${secrets:orders-db/debezium-password}",
    "database.dbname": "orders",
    "topic.prefix": "pg",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_orders",
    "publication.name": "dbz_orders",
    "publication.autocreate.mode": "disabled",
    "table.include.list": "public.orders,public.order_items,public.payments,public.shipments,public.inventory,public.prices,public.customers,public.addresses,public.returns,public.promotions,public.products,public.carriers",
    "snapshot.mode": "initial",
    "heartbeat.interval.ms": "10000",
    "decimal.handling.mode": "string",
    "time.precision.mode": "connect",
    "tombstones.on.delete": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
'''),
    dict(file="normalize_changes.sql", lang="sql",
         caption="Flink SQL: one job reads all twelve Debezium topics and writes a single, flat change stream, exactly once.",
         code='''
-- Debezium events from every table; the row images stay as JSON text
CREATE TABLE changes_in (
  key_json  STRING,
  `before`  STRING,
  `after`   STRING,
  op        STRING,
  source    ROW<`table` STRING, lsn BIGINT, ts_ms BIGINT>
) WITH (
  'connector' = 'kafka',
  'topic-pattern' = 'pg\\.public\\..*',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-cdc-normalize',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'key.format' = 'raw',
  'key.fields' = 'key_json',
  'value.format' = 'json',
  'value.fields-include' = 'EXCEPT_KEY'
);

-- One flat record per change. Keyed by table + primary key, so a row's changes stay in order.
CREATE TABLE changes_out (
  pk            STRING,
  table_name    STRING,
  op            STRING,
  is_deleted    BOOLEAN,
  lsn           BIGINT,
  committed_ms  BIGINT,
  row_json      STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.changes',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'key.fields' = 'pk',
  'value.format' = 'json',
  'value.fields-include' = 'EXCEPT_KEY',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'cdc-changes'
);

INSERT INTO changes_out
SELECT
  CONCAT(source.`table`, ':', key_json),
  source.`table`,
  op,
  op = 'd',
  source.lsn,
  source.ts_ms,
  COALESCE(`after`, `before`)   -- deletes carry the key in the before image
FROM changes_in
WHERE op IN ('c', 'u', 'd', 'r');
'''),
    dict(file="core_orders.sql", lang="sql",
         caption="Snowflake: a dynamic table keeps the newest version of each order and removes deleted ones. The view feeds the lag chart.",
         code='''
CREATE OR REPLACE DYNAMIC TABLE core.orders
  TARGET_LAG = '1 minute'
  WAREHOUSE = cdc_wh
AS
WITH parsed AS (
  SELECT
    record_content:lsn::NUMBER                  AS lsn,
    record_content:is_deleted::BOOLEAN          AS is_deleted,
    PARSE_JSON(record_content:row_json::STRING) AS r
  FROM raw.changes
  WHERE record_content:table_name::STRING = 'orders'
),
latest AS (
  SELECT *
  FROM parsed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY r:order_id::NUMBER ORDER BY lsn DESC) = 1
)
SELECT
  r:order_id::NUMBER                         AS order_id,
  r:customer_id::NUMBER                      AS customer_id,
  r:status::VARCHAR                          AS status,
  r:total_usd::NUMBER(12, 2)                 AS total_usd,
  r:promo_code::VARCHAR                      AS promo_code,   -- new on Oct 6, already in RAW
  TO_TIMESTAMP_LTZ(r:updated_at::NUMBER, 3)  AS updated_at,
  lsn
FROM latest
WHERE NOT is_deleted;

-- Commit in PostgreSQL to arrival in Snowflake, per 5 minutes
CREATE OR REPLACE VIEW monitoring.cdc_lag_5min AS
SELECT
  TIME_SLICE(TO_TIMESTAMP_LTZ(record_content:committed_ms::NUMBER, 3), 5, 'MINUTE') AS interval_start,
  COUNT(*) AS changes,
  APPROX_PERCENTILE(record_metadata:SnowflakeConnectorPushTime::NUMBER
                    - record_content:committed_ms::NUMBER, 0.50) / 1000 AS lag_p50_s,
  APPROX_PERCENTILE(record_metadata:SnowflakeConnectorPushTime::NUMBER
                    - record_content:committed_ms::NUMBER, 0.95) / 1000 AS lag_p95_s
FROM raw.changes
GROUP BY 1;
'''),
]

HIGHLIGHTS = [
    ("The 2 a.m. bulk update",
     "A nightly job rewrote 1.2 million prices in one transaction. Throughput peaked near 1,400 events a second, p95 lag reached 7.9 s and drained within 15 minutes. The SLA held."),
    ("A restart nobody noticed",
     "At 14:32 the Debezium task restarted after a network blip. It resumed from the last LSN it had confirmed to PostgreSQL, so nothing was lost and the merge ignored the few replayed events."),
    ("Schema changes without a deploy",
     "Each change carries the whole row as JSON, so `promo_code`, a wider `carrier_code` and `marketing_opt_in` reached `RAW.CHANGES` the moment they existed in PostgreSQL. Exposing `promo_code` in `CORE.ORDERS` took one line."),
    ("The WAL stays small",
     "The replication slot holds about 200 MB of write-ahead log. An alert fires at 5 GB, long before a stuck consumer could fill the source database's disk."),
]
