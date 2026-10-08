"""Project 07: Serverless data pipeline on AWS."""

import datetime as dt

from common import col, day_label, day_list, pct_change, quantile, r0, r2, r4, status

META = dict(
    num="07",
    slug="serverless-aws-pipeline",
    seed=707,
    title="Serverless Data Pipeline on AWS",
    short="Serverless on AWS",
    category="Serverless on AWS",
    tagline="Store sales files processed the moment they land in S3, by EventBridge, Step Functions, Lambda and Glue, with nothing running between files.",
    tile_stack="S3 · Lambda · Step Functions · Glue · Athena",
    summary=(
        "Point-of-sale systems in 140 stores drop sales files into Amazon S3 throughout the day. Each new file triggers an EventBridge rule that starts "
        "a Step Functions workflow: a Lambda function validates the file, and an AWS Glue job merges its rows into an Apache Iceberg table in the Glue Data Catalog. "
        "Athena queries the table directly and Redshift Serverless refreshes the reporting views every 15 minutes. Nothing runs between files, so the whole "
        "pipeline costs about $14 a day."
    ),
    stack=["Amazon S3", "Amazon EventBridge", "AWS Step Functions", "AWS Lambda", "AWS Glue", "Amazon Athena", "Amazon Redshift Serverless"],
    window="Last 30 days · Sep 7 – Oct 6, 2026",
    csv="pipeline_daily.csv",
    csv_desc="Files, workflow outcomes, Lambda and Glue activity, cost and sales per day",
    how_note="Event-driven from end to end: a file arrives, a workflow runs, and then everything scales back to zero.",
    model_note="Raw files keep their CSV layout; the Iceberg table is what Athena and Redshift query.",
    code_note="The Step Functions workflow, the validation Lambda and the Glue job.",
)

END = dt.date(2026, 10, 6)
REGIONS = [("Midwest", 0.27), ("Southeast", 0.21), ("Southwest", 0.17), ("Northeast", 0.15), ("West", 0.12), ("Pacific Northwest", 0.08)]
SERVICES = ["AWS Glue", "Redshift Serverless", "Amazon S3", "Amazon Athena", "Step Functions", "AWS Lambda"]
SERVICE_DAY = [9.62, 2.41, 0.86, 0.58, 0.11, 0.07]


def build(g):
    days = day_list(END, 30)
    labels = [day_label(d) for d in days]
    files, ok, retried, failed, inv, errs, err_rate, glue_p50, glue_p95, sales = ([] for _ in range(10))
    cost = [[] for _ in SERVICES]
    for i, d in enumerate(days):
        weekend = d.weekday() >= 5
        f = r0(630 * (1.06 if weekend else 1.0) * g.jitter(0.025))
        fail = max(0, r0(g.gauss(2.4, 1.6)))
        retry = max(0, r0(g.gauss(5.0, 2.2)))
        if d == dt.date(2026, 9, 18):
            fail, retry = 11, 9  # a store's POS upgrade sent files with a new header
        if d == dt.date(2026, 10, 1):
            retry = 31  # Glue concurrency limit during a backfill
        files.append(f)
        failed.append(fail)
        retried.append(retry)
        ok.append(f - fail - retry)
        invocations = f + fail + r0(f * 0.02)
        e = max(0, r0(g.gauss(0.45, 0.6)))
        if d == dt.date(2026, 9, 18):
            e = 4
        inv.append(invocations)
        errs.append(e)
        err_rate.append(r4(e / invocations))
        p50 = 92 * g.jitter(0.05)
        p95 = p50 * 1.8 * g.jitter(0.06)
        if d == dt.date(2026, 10, 1):
            p95 = 412
        glue_p50.append(r0(p50))
        glue_p95.append(r0(p95))
        trend = 0.94 if i >= 20 else 1.0  # Glue Flex for backfills from Sep 27
        for k, base in enumerate(SERVICE_DAY):
            cost[k].append(r2(base * (trend if k == 0 else 1.0) * (f / 630) * g.jitter(0.04)))
        sales.append(r0(1_610_000 * (1.18 if weekend else 1.0) * g.jitter(0.035)))

    total_files = sum(files)
    total_cost = sum(sum(c) for c in cost)
    total_sales = sum(sales)
    success = (sum(ok) + sum(retried)) / total_files

    kpis = [
        dict(label="Files processed", value=total_files, fmt="int", delta=0.031, deltaLabel="vs prior 30 days", spark=files),
        dict(label="Workflow success", value=r4(success), fmt="pct", status="good", statusLabel="%d failed, all bad files" % sum(failed)),
        dict(label="File to query, p50", value=372, fmt="dur", delta=-0.071, upIsGood=False, deltaLabel="vs prior 30 days"),
        dict(label="Lambda error rate", value=r4(sum(errs) / sum(inv)), fmt="pct2", status="good", statusLabel="Alert at 0.5%"),
        dict(label="Cost, 30 days", value=round(total_cost, 2), fmt="usd2", delta=-0.096, upIsGood=False, deltaLabel="vs prior 30 days",
             spark=[sum(cost[k][i] for k in range(len(SERVICES))) for i in range(30)]),
        dict(label="Sales processed", value=total_sales, fmt="usd", delta=pct_change(total_sales, total_sales / 1.024), deltaLabel="vs prior 30 days", spark=sales),
    ]

    charts = dict(
        invocations=dict(
            kind="bar", x=labels, fmt="int", labelEvery=6, xLabel="Day",
            desc="Lambda invocations per day across the validation and quarantine functions.",
            series=[dict(name="Invocations", data=inv)],
        ),
        errors=dict(
            kind="line", x=labels, fmt="pct2", labelEvery=6, xLabel="Day", yMin=0, yMax=0.006,
            desc="Share of Lambda invocations that ended in an error, per day, with the 0.5 percent alert threshold. Sep 18 shows the bad POS files.",
            series=[dict(name="Error rate", data=err_rate)],
            thresholds=[dict(value=0.005, label="Alert 0.5%", status="warning")],
        ),
        executions=dict(
            kind="bar", x=labels, fmt="int", stack=True, labelEvery=6, xLabel="Day",
            desc="Step Functions executions per day by outcome. Failures are files that failed validation and were quarantined.",
            series=[dict(name="Succeeded", data=ok), dict(name="Succeeded after retry", data=retried, status="warning"),
                    dict(name="Failed, quarantined", data=failed, status="critical")],
        ),
        regions=dict(
            kind="bar", horizontal=True, x=[r[0] for r in REGIONS], fmt="usd", valueLabels=True, xLabel="Region",
            desc="Sales in the processed files over 30 days, by store region.",
            series=[dict(name="Sales", data=[r0(total_sales * r[1] * g.jitter(0.01)) for r in REGIONS])],
        ),
        cost=dict(
            kind="bar", x=labels, fmt="usd2", stack=True, labelEvery=6, xLabel="Day",
            desc="Daily AWS cost of the pipeline by service. Glue is most of it; using Glue Flex for backfills from Sep 27 lowered it.",
            series=[dict(name=n, data=cost[k]) for k, n in enumerate(SERVICES)],
        ),
        glue=dict(
            kind="line", x=labels, fmt="dur", labelEvery=6, xLabel="Day", yMin=0,
            desc="Glue job run time per day, median and 95th percentile. Oct 1 hit the concurrency limit during a backfill.",
            series=[dict(name="p50", data=glue_p50), dict(name="p95", data=glue_p95)],
        ),
    )

    runs = [
        ("exec-7f3a91", "store-118/2026-10-06/pos_2345.csv", "23:47", 341, 4_812, status("good", "Succeeded")),
        ("exec-7f3a8c", "store-042/2026-10-06/pos_2330.csv", "23:32", 318, 3_960, status("good", "Succeeded")),
        ("exec-7f3a77", "store-007/2026-10-06/pos_2330.csv", "23:31", 702, 5_221, status("warning", "Succeeded after retry")),
        ("exec-7f3a6e", "store-131/2026-10-06/pos_2315.csv", "23:17", 309, 2_874, status("good", "Succeeded")),
        ("exec-7f3a52", "store-093/2026-10-06/pos_2300.csv", "23:02", 11, 0, status("critical", "Quarantined: bad header")),
        ("exec-7f3a49", "store-118/2026-10-06/pos_2300.csv", "23:01", 336, 4_406, status("good", "Succeeded")),
        ("exec-7f3a3d", "store-064/2026-10-06/pos_2245.csv", "22:47", 297, 2_519, status("good", "Succeeded")),
        ("exec-7f3a30", "store-021/2026-10-06/pos_2245.csv", "22:46", 325, 3_377, status("good", "Succeeded")),
    ]

    tables = dict(
        runs=dict(
            columns=[col("e", "Execution", "code"), col("f", "File", "code"), col("s", "Started"), col("d", "Duration", "dur"),
                     col("r", "Rows", "int"), col("st", "Outcome", "status")],
            rows=[dict(e=a, f=b, s=c, d=d_, r=e_, st=f_) for a, b, c, d_, e_, f_ in runs],
        ),
    )

    header = ["date", "files", "executions_succeeded", "executions_retried", "executions_failed", "lambda_invocations", "lambda_errors",
              "glue_p50_s", "glue_p95_s"] + ["cost_%s_usd" % n.lower().replace("aws ", "").replace("amazon ", "").replace(" ", "_") for n in SERVICES] + ["sales_usd"]
    rows = []
    for i, d in enumerate(days):
        rows.append([d.isoformat(), files[i], ok[i], retried[i], failed[i], inv[i], errs[i], glue_p50[i], glue_p95[i]]
                    + [cost[k][i] for k in range(len(SERVICES))] + [sales[i]])
    return dict(kpis=kpis, charts=charts, tables=tables), header, rows


PANELS = [
    dict(chart="executions", span=8, h=290, title="Workflow executions by outcome", sub="Step Functions, one execution per file"),
    dict(chart="regions", span=4, h=290, title="Sales by region", sub="From the processed files, 30 days"),
    dict(chart="invocations", span=6, h=250, title="Lambda invocations", sub="Per day"),
    dict(chart="errors", span=6, h=250, title="Lambda error rate", sub="Per day, with the alert threshold"),
    dict(chart="cost", span=7, h=290, title="Daily cost by service", sub="Everything the pipeline uses, in USD"),
    dict(chart="glue", span=5, h=290, title="Glue job run time", sub="p50 and p95 per day"),
    dict(table="runs", span=12, title="Latest executions", sub="Oct 6, most recent first"),
]

FLOW = [
    ("Land", "Amazon S3", "POS systems upload about 630 sales files a day to `s3://sales-landing/<store>/<date>/`."),
    ("Trigger", "Amazon EventBridge", "An Object Created rule starts one Step Functions execution per file."),
    ("Validate", "AWS Lambda", "Checks the header, row count and encoding. Bad files move to a quarantine prefix with the reason."),
    ("Transform", "AWS Glue", "A Spark job types the rows and merges them into an Iceberg table on transaction id."),
    ("Catalog", "Glue Data Catalog", "Every Iceberg commit updates the catalog, so new rows are queryable immediately."),
    ("Serve", "Athena · Redshift Serverless", "Athena for ad hoc questions; Redshift refreshes reporting views every 15 minutes."),
]

STEPS = [
    ("Pay per file.", "No cluster waits for work. Lambda and Glue start when a file lands and stop when it is done; backfills run on Glue Flex at a lower price."),
    ("Retries live in the workflow.", "Step Functions retries throttled Lambda calls and Glue errors with exponential backoff before an execution is marked failed and an alert goes out."),
    ("Safe to run twice.", "The Glue job merges on `transaction_id` and `sku`, so a file processed twice, by a retry or a manual rerun, does not double the sales."),
    ("Bad files don't block good ones.", "A file that fails validation goes to quarantine with its reason and the next file runs normally. Store support gets a daily list."),
]

MODELS = [
    dict(name="s3://sales-landing", kind="CSV files from POS systems", columns=[
        ("store_id / register_id", "string", "Where the sale happened"),
        ("transaction_id", "string", "Unique per sale within a store"),
        ("sold_at", "string", "Local timestamp, parsed by Glue"),
        ("sku / quantity / unit_price", "string", "One row per item"),
        ("payment_type", "string", "`card`, `cash`, `gift_card`, `mobile`"),
    ]),
    dict(name="sales.transactions", kind="Iceberg table · Glue Data Catalog · days(sold_at), store_id", columns=[
        ("transaction_id / sku", "string", "Merge key"),
        ("store_id", "int", "Store"),
        ("sold_at", "timestamp", "Converted to UTC"),
        ("quantity / unit_price", "int / decimal(10,2)", "Typed values"),
        ("source_file", "string", "The S3 key it came from"),
    ]),
    dict(name="pipeline_daily", kind="Rollup · the CSV download", columns=[
        ("date", "date", "Day"),
        ("files / executions_*", "int", "Files received and how their workflows ended"),
        ("lambda_invocations / lambda_errors", "int", "Lambda activity"),
        ("glue_p50_s / glue_p95_s", "int", "Glue run time"),
        ("cost_*_usd / sales_usd", "decimal", "Cost by service, and sales in the files"),
    ]),
]

CODE = [
    dict(file="process_sales_file.asl.json", lang="json",
         caption="Step Functions workflow (Amazon States Language): validate, transform, and quarantine on failure.",
         code='''
{
  "Comment": "Process one POS sales file from S3",
  "StartAt": "ValidateFile",
  "States": {
    "ValidateFile": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "validate-sales-file",
        "Payload": {
          "bucket.$": "$.detail.bucket.name",
          "key.$": "$.detail.object.key"
        }
      },
      "ResultSelector": { "file.$": "$.Payload" },
      "Retry": [
        {
          "ErrorEquals": ["Lambda.TooManyRequestsException", "Lambda.ServiceException"],
          "IntervalSeconds": 2,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        { "ErrorEquals": ["InvalidFile"], "ResultPath": "$.error", "Next": "Quarantine" }
      ],
      "Next": "MergeIntoIceberg"
    },
    "MergeIntoIceberg": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "merge-sales-file",
        "Arguments": {
          "--source_key.$": "$.file.key",
          "--store_id.$": "$.file.store_id"
        }
      },
      "Retry": [
        {
          "ErrorEquals": ["Glue.ConcurrentRunsExceededException", "Glue.AWSGlueException"],
          "IntervalSeconds": 30,
          "MaxAttempts": 4,
          "BackoffRate": 2
        }
      ],
      "End": true
    },
    "Quarantine": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": { "FunctionName": "quarantine-sales-file", "Payload.$": "$" },
      "Next": "Rejected"
    },
    "Rejected": {
      "Type": "Fail",
      "Error": "InvalidFile",
      "Cause": "The file failed validation and was moved to quarantine"
    }
  }
}
'''),
    dict(file="validate_file.py", lang="python",
         caption="Lambda: cheap checks before any Spark runs. Raising `InvalidFile` sends the execution to the quarantine branch.",
         code='''
import csv
import io

import boto3

s3 = boto3.client("s3")
EXPECTED = ["store_id", "register_id", "transaction_id", "sold_at", "sku", "quantity", "unit_price", "payment_type"]
MAX_BYTES = 50 * 1024 * 1024


class InvalidFile(Exception):
    """A file that must not be processed. Step Functions catches this by name."""


def handler(event, context):
    bucket, key = event["bucket"], event["key"]   # <store>/<yyyy-mm-dd>/<file>.csv
    head = s3.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] == 0:
        raise InvalidFile(f"{key} is empty")
    if head["ContentLength"] > MAX_BYTES:
        raise InvalidFile(f"{key} is larger than 50 MB")

    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as err:
        raise InvalidFile(f"{key} is not UTF-8: {err}") from err

    reader = csv.reader(io.StringIO(text))
    header = next(reader, [])
    if [h.strip().lower() for h in header] != EXPECTED:
        raise InvalidFile(f"{key} has an unexpected header: {header}")

    rows = sum(1 for _ in reader)
    if rows == 0:
        raise InvalidFile(f"{key} has no rows")

    return {
        "bucket": bucket,
        "key": key,
        "store_id": key.split("/")[0].removeprefix("store-"),
        "rows": rows,
        "etag": head["ETag"].strip('"'),
    }
'''),
    dict(file="merge_sales_file.py", lang="python",
         caption="Glue job (Spark + Iceberg): type the rows and merge them on `transaction_id` and `sku`, so reruns never duplicate sales.",
         code='''
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Job parameters include --datalake-formats iceberg and the glue_catalog Spark settings.
args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_key", "store_id"])
glue = GlueContext(SparkContext.getOrCreate())
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

rows = (
    spark.read.option("header", True).csv(f"s3://sales-landing/{args['source_key']}")
    .select(
        F.col("store_id").cast("int").alias("store_id"),
        "register_id",
        "transaction_id",
        F.to_utc_timestamp(F.to_timestamp("sold_at"), "America/Chicago").alias("sold_at"),
        "sku",
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        "payment_type",
    )
    .where(F.col("quantity") != 0)
    .dropDuplicates(["transaction_id", "sku"])
    .withColumn("source_file", F.lit(args["source_key"]))
)
rows.createOrReplaceTempView("incoming")

spark.sql("""
    MERGE INTO glue_catalog.sales.transactions AS t
    USING incoming AS s
    ON t.transaction_id = s.transaction_id AND t.sku = s.sku
    WHEN NOT MATCHED THEN INSERT *
""")

job.commit()
'''),
]

HIGHLIGHTS = [
    ("A POS upgrade, contained",
     "On Sep 18 a software update at one store changed the CSV header. Eleven files failed validation and went to quarantine with the reason; every other store's files kept flowing. After the store's export was fixed, the quarantined files were replayed."),
    ("The Oct 1 backfill",
     "Reprocessing a month for an audit hit Glue's concurrent-run limit. Step Functions retried with backoff, so 31 executions finished late instead of failing; p95 Glue time that day was about 7 minutes."),
    ("About $14 a day",
     "Glue is two-thirds of the cost. Moving backfills to Glue Flex on Sep 27 cut Glue spend by about 6% without slowing the daily files."),
    ("Minutes, not a nightly batch",
     "Sales are queryable in Athena about six minutes after a store uploads a file, and in the Redshift reporting views within the next 15-minute refresh."),
]
