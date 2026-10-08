"""Project 10: Real-time fraud detection pipeline."""

import datetime as dt
import math

from common import bump24, col, day_label, day_list, five_min_labels, pct_change, r0, r2, r4, status

META = dict(
    num="10",
    slug="fraud-detection-streaming",
    seed=1010,
    title="Real-Time Fraud Detection Pipeline",
    short="Fraud Detection",
    category="Real-time ML",
    tagline="Card transactions scored in under 50 ms using streaming features from Spark and a Feast feature store, with alerts, analyst feedback and model quality in one view.",
    tile_stack="Kafka · Spark Streaming · Feast · Redis · Grafana",
    summary=(
        "Every card authorization is published to Kafka. A Spark Structured Streaming job keeps rolling features for each card, merchant and device, such as "
        "spend in the last ten minutes and how many merchants a card has used, and pushes them to a Feast feature store backed by Redis. The scoring service "
        "reads those features in a few milliseconds, scores the transaction and answers before the payment is approved. High scores become alerts for analysts, "
        "and their decisions flow back as labels to measure precision and recall every day."
    ),
    stack=["Apache Kafka", "Spark Structured Streaming", "Feast", "Redis", "Grafana"],
    window="Tuesday, Oct 6, 2026 · model quality over the last 30 days",
    csv="transactions_5min.csv",
    csv_desc="Transactions, flags, declines, amounts and scoring latency in 5-minute intervals",
    how_note="From authorization to decision in about 46 ms at p95, with the same features online and in training.",
    model_note="One event, one feature view and one score record: the whole life of a decision.",
    code_note="The streaming features, their Feast definitions, and the scoring service.",
)

END = dt.date(2026, 10, 6)
MCC = [("Gift cards", 0.0261), ("Digital goods", 0.0192), ("Electronics", 0.0108), ("Travel", 0.0081),
       ("Jewelry", 0.0074), ("Fuel", 0.0031), ("Restaurants", 0.0012), ("Grocery", 0.0005)]


def build(g):
    labels = five_min_labels()
    tx, flagged, declined, rate, amount, lat95 = [], [], [], [], [], []
    for i in range(288):
        h = i / 12.0
        n = (2_600 + 8_200 * bump24(h, 12.6, 2.6) + 9_600 * bump24(h, 18.6, 2.4) + 3_000 * bump24(h, 9.0, 1.5)) * g.jitter(0.035)
        r = 0.0035 * g.jitter(0.12)
        if 37 <= i <= 44:  # 03:05-03:40, card testing at one online merchant
            r = [0.0091, 0.0157, 0.0189, 0.0176, 0.0162, 0.0128, 0.0094, 0.0061][i - 37]
            n += 480
        f = n * r
        tx.append(r0(n))
        flagged.append(r0(f))
        declined.append(r0(f * 0.41 * g.jitter(0.1)))
        rate.append(round(f / n, 5))
        amount.append(r2(61.4 * g.jitter(0.04) * (0.7 if 37 <= i <= 44 else 1.0)))
        lat95.append(r0(41 + 6 * (n / 13000) + abs(g.gauss(2.5))))

    total = sum(tx)
    total_flagged = sum(flagged)
    hourly = [sum(tx[h * 12:(h + 1) * 12]) for h in range(24)]
    tpm = [r0(v / 5) for v in tx]

    # Score histogram (log-normal-ish, with a fraud tail)
    bins = ["%.2f" % (b / 20) for b in range(20)]
    weights = [math.exp(-0.62 * b) for b in range(20)]
    wsum = sum(weights[:16])
    approved_total = total - total_flagged
    approved = [r0(approved_total * w / wsum) if b < 16 else None for b, w in enumerate(weights)]
    tail = [0.34, 0.27, 0.22, 0.17]
    flagged_bins = [None] * 16 + [r0(total_flagged * t) for t in tail]

    # Model quality, 30 days; v14 shipped on Sep 24
    days = day_list(END, 30)
    precision, recall = [], []
    for d in days:
        v14 = d >= dt.date(2026, 9, 24)
        precision.append(round((0.722 if v14 else 0.664) + g.gauss(0.012), 3))
        recall.append(round((0.861 if v14 else 0.853) + g.gauss(0.01), 3))

    tp, fn, fp = 8_412, 1_369, 3_436
    tn = 85_206_000 - tp - fn - fp

    kpis = [
        dict(label="Transactions scored", value=total, fmt="compact", delta=0.027, deltaLabel="vs Sep 29", spark=hourly),
        dict(label="Flag rate", value=r4(total_flagged / total), fmt="pct2", delta=0.062, upIsGood=False, deltaLabel="vs Sep 29"),
        dict(label="Scoring p95", value=46, fmt="ms", status="good", statusLabel="Budget 100 ms"),
        dict(label="Precision, 30 days", value=round(tp / (tp + fp), 3), fmt="ratio", delta=0.087, deltaLabel="since model v14", spark=precision),
        dict(label="Recall, 30 days", value=round(tp / (tp + fn), 3), fmt="ratio", delta=0.009, deltaLabel="since model v14", spark=recall),
        dict(label="Confirmed fraud stopped", value=1_918_400, fmt="usd", note="30 days, confirmed by analysts"),
    ]

    charts = dict(
        tpm=dict(
            kind="line", x=labels, fmt="int", labelEvery=35, area=True, xLabel="Time",
            desc="Card transactions per minute on Oct 6, averaged over 5-minute intervals.",
            series=[dict(name="Transactions per minute", data=tpm)],
        ),
        flagrate=dict(
            kind="line", x=labels, fmt="pct2", labelEvery=35, xLabel="Time", yMin=0, yMax=0.02,
            desc="Share of transactions flagged for review or declined, per 5 minutes. A card-testing attack pushed it to 1.9 percent around 03:15.",
            series=[dict(name="Flag rate", data=rate)],
            thresholds=[dict(value=0.01, label="Alert 1%", status="warning")],
        ),
        scores=dict(
            kind="bar", x=bins, fmt="compact", log=True, yMin=100, overlap=True, xLabel="Risk score (bin start)",
            desc="Transactions on Oct 6 by risk score, on a log scale. Scores of 0.80 and above go to review or are declined.",
            series=[dict(name="Approved, below 0.80", data=approved), dict(name="Review or decline, 0.80 and up", data=flagged_bins)],
        ),
        merchants=dict(
            kind="bar", horizontal=True, x=[m[0] for m in MCC], fmt="pct2", valueLabels=True, xLabel="Merchant category",
            desc="Flag rate by merchant category on Oct 6. Gift cards and digital goods are the riskiest.",
            series=[dict(name="Flag rate", data=[round(m[1] * g.jitter(0.03), 5) for m in MCC])],
        ),
        quality=dict(
            kind="line", x=[day_label(d) for d in days], fmt="ratio", labelEvery=6, xLabel="Day", yMin=0.5, yMax=1.0,
            desc="Daily precision and recall from analyst decisions and chargebacks. Model v14 shipped on Sep 24 and raised precision.",
            series=[dict(name="Precision", data=precision), dict(name="Recall", data=recall)],
        ),
        confusion=dict(
            kind="matrix", rows=["Fraud", "Legitimate"], cols=["Flagged", "Not flagged"], values=[[tp, fn], [fp, tn]], fmt="compact",
            labels=[["Caught", "Missed"], ["False alarm", "Correctly passed"]], title="Confusion matrix, 30 days",
        ),
    )

    alerts = [
        ("AL-90412", "23:58", "4821", "Maple Gift Co.", "Gift cards", 500.00, 0.97, "8 cards used this device in 24 h", status("critical", "Confirmed fraud")),
        ("AL-90408", "23:41", "1177", "Volt Electronics", "Electronics", 1_849.99, 0.91, "Spend 11× the card's 30-day average", status("neutral", "In review")),
        ("AL-90397", "23:12", "6630", "Skyway Air", "Travel", 742.30, 0.86, "2,300 km from the previous transaction", status("good", "Cleared")),
        ("AL-90384", "22:47", "3092", "PixelPlay Store", "Digital goods", 99.99, 0.88, "First digital purchase, new device", status("good", "Cleared")),
        ("AL-90371", "22:05", "5518", "Lumen Jewelers", "Jewelry", 2_310.00, 0.93, "3 merchants in 6 minutes", status("critical", "Confirmed fraud")),
        ("AL-90102", "03:22", "7744", "QuickTop Online", "Digital goods", 1.00, 0.99, "41 cards tested at merchant in 5 min", status("critical", "Confirmed fraud")),
        ("AL-90098", "03:19", "2281", "QuickTop Online", "Digital goods", 1.00, 0.99, "41 cards tested at merchant in 5 min", status("critical", "Confirmed fraud")),
        ("AL-89951", "01:14", "9035", "Harbor Fuel", "Fuel", 85.40, 0.82, "Card used in two states within an hour", status("good", "Cleared")),
    ]

    tables = dict(
        alerts=dict(
            columns=[col("id", "Alert", "code"), col("t", "Time"), col("card", "Card", "code"), col("m", "Merchant"), col("c", "Category"),
                     col("a", "Amount", "usd2"), col("s", "Score", "ratio"), col("why", "Top reason"), col("st", "Outcome", "status")],
            rows=[dict(id=a, t=b, card="•••• " + c, m=d, c=e, a=f, s=s_, why=w, st=st) for a, b, c, d, e, f, s_, w, st in alerts],
        ),
        features=dict(
            columns=[col("fv", "Feature view", "code"), col("ent", "Entity"), col("n", "Features", "int"), col("src", "Source"),
                     col("ttl", "TTL"), col("fresh", "Online age"), col("st", "Status", "status")],
            rows=[
                dict(fv="card_txn_rolling", ent="card", n=9, src="Spark stream (push)", ttl="2 h", fresh="1.2 s", st=status("good", "Fresh")),
                dict(fv="merchant_risk", ent="merchant", n=7, src="Spark stream (push)", ttl="2 h", fresh="1.4 s", st=status("good", "Fresh")),
                dict(fv="device_history", ent="device", n=5, src="Spark stream (push)", ttl="24 h", fresh="1.1 s", st=status("good", "Fresh")),
                dict(fv="card_profile", ent="card", n=14, src="Daily batch", ttl="48 h", fresh="6 h", st=status("good", "Fresh")),
                dict(fv="customer_profile", ent="customer", n=11, src="Daily batch", ttl="48 h", fresh="26 h", st=status("warning", "Batch 2 h late")),
            ],
        ),
    )

    header = ["interval_start", "transactions", "flagged", "flag_rate", "declined", "avg_amount_usd", "scoring_p95_ms"]
    rows = [["2026-10-06T%s:00-05:00" % labels[i], tx[i], flagged[i], rate[i], declined[i], amount[i], lat95[i]] for i in range(288)]
    return dict(kpis=kpis, charts=charts, tables=tables), header, rows


PANELS = [
    dict(chart="tpm", span=6, h=260, title="Transactions per minute", sub="Oct 6, 5-minute average"),
    dict(chart="flagrate", span=6, h=260, title="Flag rate", sub="Sent to review or declined"),
    dict(chart="scores", span=7, h=290, title="Risk score distribution", sub="Transactions by score, log scale"),
    dict(chart="merchants", span=5, h=290, title="Flag rate by merchant category", sub="Oct 6"),
    dict(chart="quality", span=8, h=280, title="Precision and recall", sub="Daily, from analyst decisions and chargebacks"),
    dict(chart="confusion", span=4, title="Confusion matrix", sub="Last 30 days, threshold 0.80"),
    dict(table="alerts", span=12, title="Recent alerts", sub="Score, the feature that contributed most, and the analyst's decision"),
    dict(table="features", span=12, title="Feature store", sub="Feast feature views and how old their online values are"),
]

FLOW = [
    ("Authorize", "Payment gateway", "Each card authorization goes to Kafka with amount, merchant, device and location, keyed by card."),
    ("Compute features", "Spark Structured Streaming", "Sliding windows per card, merchant and device: counts, sums, distinct merchants and velocity, updated every 5 seconds."),
    ("Serve features", "Feast + Redis", "Streaming and daily batch features in one registry; online lookups take about 3 ms."),
    ("Score", "Model service", "A gradient-boosted model scores each transaction with its features before the payment is approved."),
    ("Act", "Rules + review queue", "0.90 and above declines, 0.80 to 0.90 goes to an analyst, below 0.80 approves."),
    ("Learn", "Labels + Grafana", "Analyst decisions and chargebacks become labels; precision and recall are tracked daily and drive retraining."),
]

STEPS = [
    ("Same features online and offline.", "Feast serves the feature definitions used in training, with point-in-time joins offline and Redis online, so the model sees the same numbers in production."),
    ("Fresh features, bounded latency.", "Streaming features reach Redis about a second after a transaction. If the store is slow, the service scores with batch features rather than delay the payment."),
    ("Thresholds are business decisions.", "The 0.80 and 0.90 cut-offs trade analyst workload against missed fraud. The confusion matrix shows that trade-off in numbers."),
    ("Feedback closes the loop.", "Every alert outcome and chargeback is joined back to its score. That join produces the daily precision and recall, and the training labels."),
]

MODELS = [
    dict(name="payments.authorizations", kind="Kafka topic · Avro · keyed by card", columns=[
        ("txn_id", "string", "Unique per authorization"),
        ("card_id", "string", "Tokenised card; never the card number"),
        ("merchant_id / mcc", "string / int", "Merchant and its category code"),
        ("amount_usd", "decimal(12,2)", "Converted at authorization time"),
        ("device_id / lat / lon", "string / double", "Where the request came from"),
        ("event_ts", "timestamp", "Authorization time"),
    ]),
    dict(name="card_txn_rolling", kind="Feast feature view · Redis online · TTL 2 h", columns=[
        ("card_id", "entity", "Join key"),
        ("txn_count_10m / txn_count_1h", "int64", "Transactions in the window"),
        ("amount_sum_10m", "float", "Spend in the last ten minutes"),
        ("distinct_merchants_10m", "int64", "A burst of merchants is a classic fraud signal"),
        ("km_from_last_txn", "float", "Distance from the previous transaction"),
        ("event_timestamp", "timestamp", "End of the window the values describe"),
    ]),
    dict(name="fraud.scores", kind="Iceberg table · one row per decision", columns=[
        ("txn_id", "string", "The authorization"),
        ("score / decision", "double / string", "Risk score and `approve`, `review` or `decline`"),
        ("model_version", "string", "For example `fraud-v14`"),
        ("features", "map<string,double>", "The values used, for audits and debugging"),
        ("label / labeled_at", "boolean / timestamp", "Filled in later from analysts and chargebacks"),
    ]),
]

CODE = [
    dict(file="card_features.py", lang="python",
         caption="Spark Structured Streaming: ten-minute sliding windows per card, pushed to Feast every five seconds.",
         code='''
from feast import FeatureStore
from feast.data_source import PushMode
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

store = FeatureStore(repo_path="feature_repo")
SCHEMA = open("schemas/authorization.avsc").read()

txns = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "payments.authorizations")
    .load()
    .select(from_avro(F.expr("substring(value, 6, length(value) - 5)"), SCHEMA).alias("t"))
    .select("t.*")
    .withColumn("event_ts", F.expr("timestamp_millis(event_time_ms)"))
)

rolling = (
    txns.withWatermark("event_ts", "2 minutes")
    .groupBy(F.window("event_ts", "10 minutes", "1 minute"), "card_id")
    .agg(
        F.count("*").alias("txn_count_10m"),
        F.sum("amount_usd").cast("float").alias("amount_sum_10m"),
        F.approx_count_distinct("merchant_id").alias("distinct_merchants_10m"),
    )
    .select("card_id", F.col("window.end").alias("event_timestamp"),
            "txn_count_10m", "amount_sum_10m", "distinct_merchants_10m")
)


def push_to_feast(batch, batch_id):
    rows = batch.toPandas()
    if rows.empty:
        return
    # Several windows per card can update in one batch; the newest one is what scoring needs.
    latest = rows.sort_values("event_timestamp").drop_duplicates("card_id", keep="last")
    store.push("card_txn_rolling_push", latest, to=PushMode.ONLINE_AND_OFFLINE)


(rolling.writeStream
    .outputMode("update")
    .foreachBatch(push_to_feast)
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", "s3://fraud-features/checkpoints/card_txn_rolling")
    .start())
'''),
    dict(file="feature_repo/cards.py", lang="python",
         caption="Feast definitions: the card entity, a push source fed by the stream, and the feature view the model reads.",
         code='''
from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, PushSource
from feast.types import Float32, Int64

card = Entity(name="card", join_keys=["card_id"])

# Offline copy of the pushed features, for point-in-time correct training sets
card_rolling_offline = FileSource(
    name="card_txn_rolling_offline",
    path="s3://fraud-features/card_txn_rolling/",
    timestamp_field="event_timestamp",
)

card_rolling_push = PushSource(name="card_txn_rolling_push", batch_source=card_rolling_offline)

card_txn_rolling = FeatureView(
    name="card_txn_rolling",
    entities=[card],
    ttl=timedelta(hours=2),
    schema=[
        Field(name="txn_count_10m", dtype=Int64),
        Field(name="amount_sum_10m", dtype=Float32),
        Field(name="distinct_merchants_10m", dtype=Int64),
    ],
    source=card_rolling_push,
    online=True,
    tags={"owner": "fraud-data", "freshness_sla": "5s"},
)
'''),
    dict(file="score.py", lang="python",
         caption="The scoring service: online features from Feast, a gradient-boosted model, and the decision thresholds.",
         code='''
import time

import xgboost as xgb
from fastapi import FastAPI
from feast import FeatureStore
from pydantic import BaseModel

store = FeatureStore(repo_path="feature_repo")
model = xgb.Booster(model_file="models/fraud-v14.json")

FEATURES = [
    "card_txn_rolling:txn_count_10m",
    "card_txn_rolling:amount_sum_10m",
    "card_txn_rolling:distinct_merchants_10m",
    "card_profile:amount_avg_30d",
    "merchant_risk:chargeback_rate_90d",
    "device_history:cards_seen_24h",
]
NAMES = [f.split(":")[1] for f in FEATURES]


class Authorization(BaseModel):
    txn_id: str
    card_id: str
    merchant_id: str
    device_id: str
    amount_usd: float
    km_from_last_txn: float


app = FastAPI()


@app.post("/score")
def score(txn: Authorization) -> dict:
    started = time.perf_counter()
    values = store.get_online_features(
        features=FEATURES,
        entity_rows=[{"card_id": txn.card_id, "merchant_id": txn.merchant_id, "device_id": txn.device_id}],
    ).to_dict()

    row = [txn.amount_usd, txn.km_from_last_txn] + [values[name][0] or 0.0 for name in NAMES]
    risk = float(model.predict(xgb.DMatrix([row], feature_names=["amount_usd", "km_from_last_txn", *NAMES]))[0])
    decision = "decline" if risk >= 0.90 else "review" if risk >= 0.80 else "approve"

    return {
        "txn_id": txn.txn_id,
        "score": round(risk, 4),
        "decision": decision,
        "model_version": "fraud-v14",
        "latency_ms": round((time.perf_counter() - started) * 1000, 1),
    }
'''),
]

HIGHLIGHTS = [
    ("The 3 a.m. card-testing attack",
     "At 03:05 a bot started testing stolen card numbers with $1 purchases at one online store. Velocity features flagged the pattern within minutes, the flag rate peaked at 1.9%, and the merchant was rate-limited at 03:41."),
    ("Model v14 cut false alarms",
     "Retrained on Sep 24 with device features, the model's precision rose from about 0.66 to 0.72 at the same recall, which means hundreds fewer false alarms a week for analysts."),
    ("46 ms at p95",
     "The feature lookup takes about 3 ms and the model about 4 ms; the rest is network and serialisation. The budget for a decision is 100 ms."),
    ("One late batch, no drama",
     "`customer_profile` features were 26 hours old after an upstream delay. Streaming features carry most of the signal, so scores stayed stable while the batch caught up."),
]
