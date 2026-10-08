"""Project 04: End-to-end ETL with orchestration and CI/CD."""

import datetime as dt

from common import col, day_label, day_list, quantile, r0, status

META = dict(
    num="04",
    slug="etl-orchestration-cicd",
    seed=404,
    title="ETL with Orchestration and CI/CD",
    short="ETL + CI/CD",
    category="Orchestration & CI/CD",
    tagline="A subscription-revenue warehouse built with Airflow and dbt on Snowflake, tested and deployed through GitHub Actions, with Terraform managing every Snowflake object.",
    tile_stack="Airflow · dbt · Snowflake · GitHub Actions · Terraform",
    summary=(
        "Billing, product usage and CRM data are extracted every night and loaded into Snowflake by an Airflow DAG. dbt turns them into tested models, "
        "from staging views to a finance mart with monthly recurring revenue and churn. Every change goes through a pull request: CI lints the SQL, builds only "
        "the changed models and their children in a throwaway schema and runs their tests, and merging deploys to production. Terraform manages the warehouses, "
        "databases, roles and grants, so the platform is reviewed like the code that runs on it."
    ),
    stack=["Apache Airflow", "dbt", "Snowflake", "GitHub Actions", "Terraform"],
    window="Last 14 nightly runs · Sep 23 – Oct 6, 2026",
    csv="dag_task_runs.csv",
    csv_desc="Every task of the last 14 nightly runs, with state, tries and duration",
    how_note="Code review covers everything: the SQL, the DAG and the infrastructure.",
    model_note="A raw table as loaded, the MRR mart dbt builds from it, and the Airflow task history behind the status grid (the CSV download).",
    code_note="The nightly DAG, the MRR model, the CI workflow and the Terraform for Snowflake.",
)

END = dt.date(2026, 10, 6)
TASKS = ["extract [billing]", "extract [product_events]", "extract [crm]", "load_raw", "dbt_source_freshness",
         "dbt_build_staging", "dbt_build_marts", "publish_exposures", "notify_finance"]
TASK_SEC = [212, 486, 164, 318, 41, 402, 1310, 95, 4]
STATES = [dict(label="Success", status="good"), dict(label="Succeeded on retry", status="warning"),
          dict(label="Failed", status="critical"), dict(label="Upstream failed", status="neutral")]


def build(g):
    days = day_list(END, 14)
    labels = [day_label(d) for d in days]
    cells, csv_rows, run_minutes = [], [], []
    for xi, d in enumerate(days):
        faster = 0.86 if d >= dt.date(2026, 9, 27) else 1.0
        t = dt.datetime(d.year, d.month, d.day, 2, 0, 4)
        total = 0
        failed_at = None
        for yi, task in enumerate(TASKS):
            state, tries = 0, 1
            dur = TASK_SEC[yi] * g.jitter(0.08) * (faster if yi >= 5 else 1.0)
            note = ""
            if d == dt.date(2026, 9, 29) and task == "dbt_build_marts":
                state, dur, note = 2, 724, "unique test failed on fct_invoices"
                failed_at = yi
            elif failed_at is not None and yi > failed_at:
                state, dur, note = 3, 0, "skipped after failure"
            elif d == dt.date(2026, 10, 2) and task == "extract [product_events]":
                state, tries, dur, note = 1, 2, dur + 300, "429 from the events API, retried after 5 min"
            elif d == dt.date(2026, 9, 25) and task == "extract [crm]":
                state, tries, dur, note = 1, 2, dur + 300, "CRM export not ready, retried"
            cells.append([xi, yi, state, note])
            if yi <= 2:
                start = t  # the three extracts run in parallel
            else:
                start = t + dt.timedelta(seconds=total)
            csv_rows.append([d.isoformat(), task, ["success", "success", "failed", "upstream_failed"][state], tries,
                             start.strftime("%Y-%m-%dT%H:%M:%S-05:00") if state != 3 else "", r0(dur)])
            if yi == 2:
                total += max(TASK_SEC[0], TASK_SEC[1] * (1.6 if d == dt.date(2026, 10, 2) else 1.0), TASK_SEC[2]) * g.jitter(0.05)
            elif yi > 2:
                total += dur
        run_minutes.append(round(total / 60.0, 1))

    succeeded = sum(1 for d in days if d != dt.date(2026, 9, 29))
    ok_minutes = [m for d, m in zip(days, run_minutes) if d != dt.date(2026, 9, 29)]

    months = ["Oct 2025", "Nov 2025", "Dec 2025", "Jan 2026", "Feb 2026", "Mar 2026", "Apr 2026", "May 2026",
              "Jun 2026", "Jul 2026", "Aug 2026", "Sep 2026"]
    mrr, churn = [], []
    v = 1_518_000
    for i in range(12):
        growth = 0.0175 + g.gauss(0.004)
        v *= 1 + growth
        mrr.append(r0(v))
        churn.append(round(0.0182 + g.gauss(0.0016) - 0.0002 * i, 4))

    kpis = [
        dict(label="Runs succeeded", value=succeeded, fmt="int", status="warning", statusLabel="13 of 14; Sep 29 stopped at the marts"),
        dict(label="Median run time", value=r0(quantile(ok_minutes, 0.5) * 60), fmt="dur", delta=-0.121, upIsGood=False, deltaLabel="vs prior 14 runs", spark=run_minutes),
        dict(label="dbt models", value=64, fmt="int", note="38 views, 21 tables, 5 incremental"),
        dict(label="Tests passing", value=round(413 / 415, 4), fmt="pct", status="warning", statusLabel="2 warnings, no failures (Oct 6)"),
        dict(label="Production deploys", value=23, fmt="int", note="0 rollbacks in 14 days"),
        dict(label="MRR, September", value=mrr[-1], fmt="usd", delta=round(mrr[-1] / mrr[-2] - 1, 4), deltaLabel="vs August", spark=mrr),
    ]

    nodes = [
        dict(name="billing.invoices", col=0, row=0), dict(name="billing.subscriptions", col=0, row=1),
        dict(name="crm.accounts", col=0, row=2), dict(name="product.events", col=0, row=3),
        dict(name="stg_invoices", col=1, row=0), dict(name="stg_subscriptions", col=1, row=1),
        dict(name="stg_accounts", col=1, row=2), dict(name="stg_events", col=1, row=3),
        dict(name="int_subscription_periods", col=2, row=1), dict(name="int_account_usage", col=2, row=3),
        dict(name="fct_invoices", col=3, row=0, status="warning", statusLabel="1 warning", note="not_null warn on discount_code"),
        dict(name="fct_mrr", col=3, row=1), dict(name="dim_accounts", col=3, row=2, status="warning", statusLabel="1 warning", note="new plan_tier value enterprise_plus"),
        dict(name="fct_usage_daily", col=3, row=3),
        dict(name="Finance dashboard", col=4, row=0.5), dict(name="Board report", col=4, row=1.6), dict(name="Churn model", col=4, row=2.7),
    ]
    edges = [
        ("billing.invoices", "stg_invoices"), ("billing.subscriptions", "stg_subscriptions"), ("crm.accounts", "stg_accounts"),
        ("product.events", "stg_events"), ("stg_invoices", "fct_invoices"), ("stg_subscriptions", "int_subscription_periods"),
        ("int_subscription_periods", "fct_mrr"), ("stg_accounts", "dim_accounts"), ("stg_events", "int_account_usage"),
        ("int_account_usage", "fct_usage_daily"), ("int_account_usage", "dim_accounts"), ("fct_invoices", "Finance dashboard"),
        ("fct_mrr", "Finance dashboard"), ("fct_mrr", "Board report"), ("dim_accounts", "Board report"),
        ("dim_accounts", "Churn model"), ("fct_usage_daily", "Churn model"),
    ]

    charts = dict(
        grid=dict(
            kind="statusgrid", x=labels, y=TASKS, cells=cells, states=STATES, yLabel="Task",
            desc="Final state of every task in the last 14 nightly runs. Sep 29 failed in dbt_build_marts; two extracts succeeded on retry.",
        ),
        lineage=dict(
            kind="graph", nodes=nodes, edges=edges, columns=["Sources", "Staging", "Intermediate", "Marts", "Exposures"],
            desc="dbt lineage from raw sources through staging and intermediate models to marts and the reports that use them. Two marts carry test warnings.",
        ),
        runtime=dict(
            kind="bar", x=labels, fmt="dur", avgLine=True, xLabel="Run", yStep=600,
            desc="Total duration of each nightly run. Sep 29 is short because it stopped at the failed marts build.",
            series=[dict(name="Run time", data=[r0(m * 60) for m in run_minutes],
                         itemStatus=["critical" if d == dt.date(2026, 9, 29) else None for d in days])],
        ),
        mrr=dict(
            kind="line", x=months, fmt="usd", area=True, xLabel="Month", endLabels=True, scale=True,
            desc="Monthly recurring revenue at month end from fct_mrr, October 2025 to September 2026.",
            series=[dict(name="MRR", data=mrr)],
        ),
    )

    tables = dict(
        tests=dict(
            columns=[col("type", "Test type", "code"), col("n", "Tests", "int"), col("w", "Warned", "int"),
                     col("f", "Failed", "int"), col("st", "Status", "status")],
            rows=[
                dict(type="not_null", n=168, p=167, w=1, f=0, st=status("warning", "1 warning")),
                dict(type="unique", n=71, p=71, w=0, f=0, st=status("good", "Passing")),
                dict(type="relationships", n=58, p=58, w=0, f=0, st=status("good", "Passing")),
                dict(type="accepted_values", n=44, p=43, w=1, f=0, st=status("warning", "1 warning")),
                dict(type="expression_is_true", n=37, p=37, w=0, f=0, st=status("good", "Passing")),
                dict(type="unit tests", n=25, p=25, w=0, f=0, st=status("good", "Passing")),
                dict(type="source freshness", n=12, p=12, w=0, f=0, st=status("good", "Passing")),
            ],
        ),
        terraform=dict(
            columns=[col("r", "Resource", "code"), col("n", "Count", "int"), col("m", "Module"), col("st", "Drift", "status")],
            rows=[
                dict(r="snowflake_warehouse", n=4, m="compute", st=status("good", "None")),
                dict(r="snowflake_database", n=3, m="storage", st=status("good", "None")),
                dict(r="snowflake_schema", n=14, m="storage", st=status("good", "None")),
                dict(r="snowflake_account_role", n=11, m="access", st=status("good", "None")),
                dict(r="snowflake_grant_privileges_to_account_role", n=86, m="access", st=status("warning", "Edited by hand")),
                dict(r="snowflake_service_user", n=5, m="access", st=status("good", "None")),
                dict(r="snowflake_resource_monitor", n=2, m="cost", st=status("good", "None")),
            ],
        ),
        ci=dict(
            columns=[col("pr", "PR", "code"), col("t", "Change", wrap=True), col("c", "Checks"), col("m", "Models built", "int"),
                     col("d", "Duration", "dur"), col("st", "Result", "status"), col("dep", "Deployed")],
            rows=[
                dict(pr="#482", t="Add net revenue retention to fct_mrr", c="lint · build · test", m=6, d=461, st=status("good", "Passed"), dep="Oct 6"),
                dict(pr="#481", t="Upgrade dbt to 1.10", c="lint · build · test", m=64, d=1263, st=status("good", "Passed"), dep="Oct 5"),
                dict(pr="#480", t="Terraform: reporting warehouse auto-suspend 300 s", c="plan · apply", m=0, d=118, st=status("good", "Passed"), dep="Oct 3"),
                dict(pr="#479", t="Incremental fct_usage_daily", c="lint · build · test", m=3, d=402, st=status("good", "Passed"), dep="Oct 1"),
                dict(pr="#478", t="Rename stg_crm_accounts to stg_accounts", c="lint · build · test", m=9, d=544, st=status("good", "Passed"), dep="Sep 30"),
                dict(pr="#477", t="Deduplicate invoices from the billing API", c="lint · build · test", m=4, d=370, st=status("good", "Passed"), dep="Sep 30"),
                dict(pr="#475", t="Allow plan tier enterprise_plus", c="lint · build · test", m=5, d=388, st=status("critical", "Test failed"), dep="Not deployed"),
                dict(pr="#474", t="Incremental fct_mrr and fct_invoices", c="lint · build · test", m=7, d=512, st=status("good", "Passed"), dep="Sep 26"),
            ],
        ),
    )

    header = ["run_date", "task_id", "state", "try_number", "started_at", "duration_s"]
    return dict(kpis=kpis, charts=charts, tables=tables), header, csv_rows


PANELS = [
    dict(chart="grid", span=12, h=340, title="Nightly DAG: task status", sub="`elt_nightly`, one column per run, final state of each task"),
    dict(chart="lineage", span=12, h=360, title="dbt lineage", sub="Sources to the reports that depend on them; amber outlines carry test warnings"),
    dict(chart="runtime", span=6, h=260, title="Run time per night", sub="Whole DAG, with the 14-run average. Sep 29 (red) stopped early."),
    dict(chart="mrr", span=6, h=260, title="Monthly recurring revenue", sub="Month-end MRR from `fct_mrr`"),
    dict(table="tests", span=5, title="dbt tests, latest run", sub="Oct 6: 413 of 415 passed, 2 warned"),
    dict(table="terraform", span=7, title="Snowflake objects in Terraform", sub="Nightly `terraform plan` checks for drift"),
    dict(table="ci", span=12, title="Recent pull requests", sub="CI checks and production deploys"),
]

FLOW = [
    ("Extract", "Python tasks on Airflow", "Billing API, product events and CRM exports pulled nightly to S3 as JSON, in parallel, with retries and backoff."),
    ("Load", "Snowflake COPY INTO", "Raw tables keep each file's rows plus load metadata. Loads are idempotent by file name."),
    ("Transform", "dbt", "64 models, from staging views to incremental marts, with 415 tests and freshness checks on every source."),
    ("Orchestrate", "Apache Airflow", "One nightly DAG with retries for flaky APIs, timeouts on long steps and alerts on failure."),
    ("Test & deploy", "GitHub Actions", "Every pull request lints the SQL and builds only changed models and their children in a throwaway schema."),
    ("Provision", "Terraform", "Warehouses, databases, roles and grants are code, reviewed and applied from the same pipeline."),
]

STEPS = [
    ("Slim CI.", "Pull requests build `state:modified+` against production's manifest and defer everything else to prod, so a typical check takes 6 to 8 minutes instead of a full rebuild."),
    ("Idempotent loads.", "COPY INTO skips files it has already loaded and every run is keyed by its logical date, so reruns and backfills never duplicate rows."),
    ("Tests that block.", "`unique` and `relationships` tests fail the build; softer checks only warn. The Sep 29 failure stopped bad numbers before the finance dashboard refreshed."),
    ("Everything is code.", "Terraform owns warehouses, roles and grants. A nightly `terraform plan` reports drift, like the grant someone changed by hand."),
]

MODELS = [
    dict(name="raw.billing_invoices", kind="Snowflake table · loaded by COPY INTO", columns=[
        ("payload", "variant", "One invoice as returned by the billing API"),
        ("_file_name", "varchar", "Source file in the S3 stage"),
        ("_file_row", "number", "Row number within the file"),
        ("_loaded_at", "timestamp_ltz", "Load time"),
    ]),
    dict(name="marts.fct_mrr", kind="dbt incremental · merge on month + account", columns=[
        ("month", "date", "First day of the month"),
        ("account_id", "varchar", "Customer account"),
        ("mrr_usd / prev_mrr_usd", "number(12,2)", "MRR this month and last month"),
        ("mrr_change_usd", "number(12,2)", "Difference between the two"),
        ("movement", "varchar", "`new`, `expansion`, `contraction`, `churn` or `flat`"),
    ]),
    dict(name="dag_task_runs", kind="Airflow task history · the CSV download", columns=[
        ("run_date", "date", "Logical date of the nightly run"),
        ("task_id", "string", "Task, with the mapped source for extracts"),
        ("state", "string", "`success`, `failed` or `upstream_failed`"),
        ("try_number", "int", "2 means the task succeeded on retry"),
        ("started_at / duration_s", "timestamp / int", "When it started and how long it ran"),
    ]),
]

CODE = [
    dict(file="elt_nightly.py", lang="python",
         caption="The nightly Airflow DAG. Extracts fan out per source, then load, freshness checks and dbt build run in order.",
         code='''
"""Nightly ELT: extract to S3, COPY INTO Snowflake, dbt build, publish."""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from include.extract import extract_to_s3  # writes JSON files to s3://raw-landing/<source>/<ds>/

DBT = {"cwd": "/opt/dbt", "env": {"DBT_TARGET": "prod"}, "append_env": True}


@dag(
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "retry_exponential_backoff": True},
    tags=["elt", "finance"],
)
def elt_nightly():
    @task(pool="api_extracts")
    def extract(source: str, ds=None) -> str:
        return extract_to_s3(source, ds)

    files = extract.expand(source=["billing", "product_events", "crm"])

    load_raw = SQLExecuteQueryOperator(
        task_id="load_raw",
        conn_id="snowflake_loader",
        sql="sql/copy_into_raw.sql",  # COPY INTO raw.<table> FROM @landing/<source>/{{ ds }}/
    )
    freshness = BashOperator(task_id="dbt_source_freshness", bash_command="dbt source freshness", **DBT)
    staging = BashOperator(task_id="dbt_build_staging", bash_command="dbt build --select staging", **DBT)
    marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command="dbt build --select intermediate+ --exclude staging",
        execution_timeout=timedelta(hours=1),
        **DBT,
    )
    publish = BashOperator(task_id="publish_exposures", bash_command="python publish_exposures.py", **DBT)

    @task
    def notify_finance(ds=None):
        from include.alerts import post_message
        post_message("#finance-data", f"Finance mart refreshed for {ds}.")

    files >> load_raw >> freshness >> staging >> marts >> publish >> notify_finance()


elt_nightly()
'''),
    dict(file="fct_mrr.sql", lang="sql",
         caption="dbt model: MRR per account and month, with every month filled in so churn shows up as a drop to zero.",
         code='''
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['month', 'account_id'],
    on_schema_change='append_new_columns'
) }}

with monthly as (
    select account_id, month, sum(mrr_usd) as mrr_usd
    from {{ ref('int_subscription_periods') }}
    group by 1, 2
),

months as (
    select distinct month from {{ ref('int_subscription_periods') }}
),

-- every account in every month from its first month on, so churn appears as zero
spine as (
    select a.account_id, m.month
    from (select account_id, min(month) as first_month from monthly group by 1) as a
    join months as m on m.month >= a.first_month
),

filled as (
    select s.account_id, s.month, coalesce(m.mrr_usd, 0) as mrr_usd
    from spine as s
    left join monthly as m using (account_id, month)
),

movements as (
    select
        month,
        account_id,
        mrr_usd,
        coalesce(lag(mrr_usd) over (partition by account_id order by month), 0) as prev_mrr_usd
    from filled
)

select
    month,
    account_id,
    mrr_usd,
    prev_mrr_usd,
    mrr_usd - prev_mrr_usd as mrr_change_usd,
    case
        when prev_mrr_usd = 0 and mrr_usd > 0 then 'new'
        when prev_mrr_usd > 0 and mrr_usd = 0 then 'churn'
        when mrr_usd > prev_mrr_usd then 'expansion'
        when mrr_usd < prev_mrr_usd then 'contraction'
        else 'flat'
    end as movement
from movements
where not (mrr_usd = 0 and prev_mrr_usd = 0)  -- nothing to report after an account churns
{% if is_incremental() %}
  and month >= (select dateadd(month, -1, max(month)) from {{ this }})
{% endif %}
'''),
    dict(file="ci.yml", lang="yaml",
         caption="GitHub Actions: pull requests build only what changed in a schema of their own; merges to main deploy dbt and apply Terraform.",
         code='''
name: analytics-ci

on:
  pull_request:
    paths: ["dbt/**", "infra/**"]
  push:
    branches: [main]

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

permissions:
  id-token: write   # OIDC to AWS for the dbt artifacts bucket
  contents: read

jobs:
  dbt:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: dbt
    env:
      SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
      SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_CI_USER }}
      SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_CI_KEY }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ARTIFACTS_ROLE }}
          aws-region: us-east-1
      - run: pip install -r requirements.txt
      - run: sqlfluff lint models --dialect snowflake
      - run: dbt deps
      - run: aws s3 cp s3://analytics-dbt-artifacts/prod/manifest.json prod-state/manifest.json

      - name: Build changed models and their children
        if: github.event_name == 'pull_request'
        env:
          DBT_SCHEMA: pr_${{ github.event.number }}
        run: dbt build --select state:modified+ --defer --state prod-state --target ci

      - name: Deploy to production
        if: github.ref == 'refs/heads/main'
        run: |
          dbt build --target prod
          aws s3 cp target/manifest.json s3://analytics-dbt-artifacts/prod/manifest.json

  terraform:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: infra
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
      - run: terraform init -input=false
      - run: terraform plan -input=false -out=tfplan
      - if: github.ref == 'refs/heads/main'
        run: terraform apply -input=false tfplan
'''),
    dict(file="snowflake.tf", lang="hcl",
         caption="Terraform for Snowflake: warehouses from one map, a database, a role for dbt and a monthly credit limit.",
         code='''
terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0"
    }
  }
  backend "s3" {
    bucket = "analytics-terraform-state"
    key    = "snowflake/prod.tfstate"
    region = "us-east-1"
  }
}

locals {
  warehouses = {
    LOADING   = { size = "XSMALL", auto_suspend = 60 }
    TRANSFORM = { size = "SMALL", auto_suspend = 60 }
    REPORTING = { size = "SMALL", auto_suspend = 300 }
    CI        = { size = "XSMALL", auto_suspend = 60 }
  }
}

resource "snowflake_resource_monitor" "monthly" {
  name            = "ANALYTICS_MONTHLY"
  credit_quota    = 400
  frequency       = "MONTHLY"
  start_timestamp = "2026-01-01 00:00"
  notify_triggers = [75, 90]
  suspend_trigger = 100
}

resource "snowflake_warehouse" "this" {
  for_each            = local.warehouses
  name                = "${each.key}_WH"
  warehouse_size      = each.value.size
  auto_suspend        = each.value.auto_suspend
  auto_resume         = true
  initially_suspended = true
  resource_monitor    = snowflake_resource_monitor.monthly.name
}

resource "snowflake_database" "analytics" {
  name                        = "ANALYTICS"
  data_retention_time_in_days = 7
}

resource "snowflake_account_role" "transformer" {
  name    = "TRANSFORMER"
  comment = "dbt runs as this role"
}

resource "snowflake_grant_privileges_to_account_role" "transformer_warehouse" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "OPERATE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.this["TRANSFORM"].name
  }
}
'''),
]

HIGHLIGHTS = [
    ("The Sep 29 failure did its job",
     "A billing API retry sent one invoice twice. The `unique` test on `fct_invoices` failed, so the finance dashboard kept the previous day's numbers instead of showing double revenue. PR #477 deduplicated invoices and the next run was clean."),
    ("CI that catches things",
     "Slim CI built 6 models per pull request on average instead of 64, and stopped a broken `accepted_values` test (PR #475) before it reached production."),
    ("Runs got 12% faster",
     "Making the largest marts incremental in PR #474 cut the median nightly run by about six minutes."),
    ("Drift is visible",
     "A grant changed by hand in the Snowflake UI showed up in the nightly `terraform plan`. It was added to the code instead of being silently reverted on the next apply."),
]
