"""Project 09: Infrastructure-as-code data platform."""

import datetime as dt

from common import bump24, col, day_label, day_list, r0, r1, status

META = dict(
    num="09",
    slug="iac-data-platform",
    seed=909,
    title="Infrastructure-as-Code Data Platform",
    short="IaC Data Platform",
    category="Infrastructure as code",
    tagline="A complete lakehouse, from the network to the dashboards, defined in Terraform and Helm on Kubernetes and rebuilt from nothing in under 20 minutes.",
    tile_stack="Terraform · Docker · Kubernetes · Iceberg · Grafana",
    summary=(
        "Every piece of this platform is code. Terraform creates the network, a Kubernetes cluster, object storage and a PostgreSQL database. "
        "Helm releases, also managed by Terraform, install an Apache Iceberg REST catalog backed by that database, Trino for SQL, the Spark operator for jobs, "
        "Airflow for scheduling, and Prometheus and Grafana for monitoring. The same modules build dev, staging and prod at different sizes, so a new environment "
        "is a pull request and about 18 minutes of `terraform apply`. Spark and Airflow run from Docker images pinned by digest."
    ),
    stack=["Terraform", "Docker", "Kubernetes", "Helm", "Apache Iceberg", "PostgreSQL", "Grafana"],
    window="Last 30 days · Sep 7 – Oct 6, 2026",
    csv="terraform_applies.csv",
    csv_desc="Every Terraform apply in the last 30 days, with its changes, duration and result",
    how_note="Three layers, all in one repository: infrastructure, platform services and the images they run.",
    model_note="The catalog's own state lives in PostgreSQL; environments differ only in their settings.",
    code_note="The root Terraform module, the Helm releases for the lakehouse, and the Spark image.",
)

END = dt.date(2026, 10, 6)
MODULES = ["kubernetes", "network", "monitoring", "storage", "airflow", "trino", "postgres", "iceberg_catalog", "spark"]
RES = {"dev": [21, 14, 6, 5, 5, 4, 4, 4, 3], "staging": [21, 14, 6, 5, 5, 4, 4, 4, 3], "prod": [25, 16, 8, 7, 6, 6, 6, 5, 4]}
BUDGET = [
    ("Trino", 0.9998, "good"), ("Iceberg REST catalog", 0.99992, "good"), ("PostgreSQL", 1.0, "good"),
    ("Spark operator", 0.99954, "good"), ("Airflow", 0.99922, "warning"), ("Grafana", 0.99996, "good"),
]


def build(g):
    total_resources = sum(sum(v) for v in RES.values())

    # Cluster utilisation, hourly over 7 days; nightly Spark jobs at 02:00
    days = day_list(END, 7)
    hours, cpu, mem = [], [], []
    for d in days:
        for h in range(24):
            c = 0.31 + 0.12 * bump24(h, 13.5, 3.5) + 0.38 * bump24(h, 2.8, 0.9) + g.gauss(0.015)
            m = 0.56 + 0.05 * bump24(h, 13.5, 4) + 0.09 * bump24(h, 2.8, 1.0) + g.gauss(0.008)
            hours.append("%s %02d:00" % (day_label(d), h))
            cpu.append(round(min(0.95, c), 3))
            mem.append(round(min(0.95, m), 3))

    # Terraform applies
    month = day_list(END, 30)
    applies, csv_rows = [], []
    pr = 274
    for d in month:
        n = 1 if d.weekday() < 5 else 0
        if g.chance(0.55) and d.weekday() < 5:
            n += 1
        for _ in range(n):
            pr += 1
            env = g.weighted(["dev", "staging", "prod"], [0.42, 0.33, 0.25])
            added, changed, destroyed = g.ri(0, 4), g.ri(0, 5), (1 if g.chance(0.15) else 0)
            if added + changed + destroyed == 0:
                changed = 1
            dur = r0((60 + 22 * (added + changed) + g.uni(0, 50)) * (1.3 if env == "prod" else 1.0))
            result = "applied"
            if pr == 296:
                env, added, changed, destroyed, dur, result = "staging", 6, 0, 0, 412, "failed"
            if pr == 284:
                env, added, changed, destroyed, dur = "dev", 18, 2, 0, 640  # Airflow module added
            hh, mm = g.ri(9, 17), g.ri(0, 59)
            applies.append(dict(pr=pr, env=env, a=added, c=changed, x=destroyed, dur=dur, result=result,
                                at="%s %02d:%02d" % (day_label(d), hh, mm)))
            csv_rows.append(["%sT%02d:%02d:00-05:00" % (d.isoformat(), hh, mm), env, "#%d" % pr, added, changed, destroyed, dur, result])
    ok = sum(1 for a in applies if a["result"] == "applied")
    last20 = applies[-20:]

    kpis = [
        dict(label="Managed resources", value=total_resources, fmt="int", note="dev %d · staging %d · prod %d" % tuple(sum(RES[e]) for e in ("dev", "staging", "prod"))),
        dict(label="Rebuild from nothing", value=18, fmt="min", status="good", statusLabel="Tested monthly, last on Sep 28"),
        dict(label="Terraform applies", value=len(applies), fmt="int", delta=0.12, deltaLabel="vs prior 30 days"),
        dict(label="Applies succeeded", value=ok, fmt="int", status="warning", statusLabel="1 failed on a quota, re-run"),
        dict(label="Platform uptime", value=0.99952, fmt="pct2", status="good", statusLabel="SLO 99.9%"),
        dict(label="Open drift", value=0, fmt="int", status="good", statusLabel="2 hand edits codified this month"),
    ]

    charts = dict(
        resources=dict(
            kind="bar", horizontal=True, stack=True, x=MODULES, fmt="int", xLabel="Module",
            desc="Terraform-managed resources by module, stacked by environment. Prod has more nodes, replicas and alarms.",
            series=[dict(name=e.capitalize() if e != "dev" else "Dev", data=RES[e]) for e in ("dev", "staging", "prod")],
        ),
        budget=dict(
            kind="meters", fmt="pct", statusChips=True,
            items=[dict(label=name, value=(1 - up) / 0.001, fill=min(1.0, (1 - up) / 0.001),
                        text="%s used" % ("{:.0%}".format(min(1.0, (1 - up) / 0.001))),
                        note="Uptime " + ("%.3f" % (up * 100)).rstrip("0").rstrip(".") + "%",
                        status=st, statusLabel="Over 75% of budget" if st == "warning" else "Within budget")
                   for name, up, st in BUDGET],
        ),
        utilization=dict(
            kind="line", x=hours, fmt="pct", labelEvery=23, tickLabel="day", xLabel="Hour", yMin=0, yMax=1, yStep=0.2,
            desc="Cluster CPU and memory requested as a share of capacity, hourly over 7 days. Nightly Spark jobs peak around 03:00.",
            series=[dict(name="CPU", data=cpu), dict(name="Memory", data=mem)],
            thresholds=[dict(value=0.8, label="Scale-out 80%", status="warning")],
        ),
        applies=dict(
            kind="bar", x=["#%d" % a["pr"] for a in last20], fmt="int", xLabel="Pull request",
            desc="Resources added, changed or destroyed by each of the last 20 applies.",
            series=[dict(name="Resources changed", data=[a["a"] + a["c"] + a["x"] for a in last20],
                         itemStatus=["critical" if a["result"] == "failed" else None for a in last20])],
        ),
    )

    tables = dict(
        pods=dict(
            columns=[col("ns", "Namespace", "code"), col("w", "Workload", "code"), col("r", "Ready"), col("rs", "Restarts, 24 h", "int"),
                     col("cpu", "CPU, cores", "dec1"), col("mem", "Memory", "gb"), col("st", "Status", "status")],
            rows=[
                dict(ns="lakehouse", w="iceberg-rest", r="2/2", rs=0, cpu=0.2, mem=0.6, st=status("good", "Healthy")),
                dict(ns="lakehouse", w="trino-coordinator", r="1/1", rs=0, cpu=0.9, mem=6.2, st=status("good", "Healthy")),
                dict(ns="lakehouse", w="trino-worker", r="4/4", rs=0, cpu=6.8, mem=41.5, st=status("good", "Healthy")),
                dict(ns="spark", w="spark-operator", r="1/1", rs=0, cpu=0.1, mem=0.3, st=status("good", "Healthy")),
                dict(ns="airflow", w="airflow-scheduler", r="2/2", rs=4, cpu=0.8, mem=3.6, st=status("warning", "Restarting")),
                dict(ns="airflow", w="airflow-webserver", r="1/1", rs=0, cpu=0.2, mem=1.1, st=status("good", "Healthy")),
                dict(ns="monitoring", w="prometheus", r="1/1", rs=0, cpu=0.6, mem=4.8, st=status("good", "Healthy")),
                dict(ns="monitoring", w="grafana", r="1/1", rs=0, cpu=0.1, mem=0.4, st=status("good", "Healthy")),
            ],
        ),
        recent=dict(
            columns=[col("pr", "PR", "code"), col("env", "Env"), col("chg", "Changes", "code"), col("dur", "Duration", "dur"),
                     col("st", "Result", "status")],
            rows=[dict(pr="#%d" % a["pr"], env=a["env"], chg="+%d ~%d −%d" % (a["a"], a["c"], a["x"]), dur=a["dur"],
                       st=status("critical", "Failed, re-run") if a["result"] == "failed" else status("good", "Applied"))
                  for a in reversed(applies[-8:])],
        ),
        catalog=dict(
            columns=[col("t", "Table", "code"), col("v", "Format"), col("snap", "Snapshots", "int"), col("files", "Data files", "int"),
                     col("size", "Size", "gb"), col("last", "Last commit")],
            rows=[
                dict(t="analytics.orders", v="v2", snap=412, files=1284, size=186.4, last="Oct 6, 23:41"),
                dict(t="analytics.order_items", v="v2", snap=409, files=2210, size=341.9, last="Oct 6, 23:41"),
                dict(t="analytics.customers", v="v2", snap=96, files=148, size=12.7, last="Oct 6, 22:10"),
                dict(t="events.page_views", v="v2", snap=1688, files=8803, size=1204.6, last="Oct 6, 23:59"),
                dict(t="finance.invoices", v="v2", snap=61, files=203, size=24.1, last="Oct 6, 05:12"),
                dict(t="ops.platform_metrics", v="v2", snap=2016, files=744, size=8.3, last="Oct 6, 23:55"),
            ],
        ),
    )

    header = ["applied_at", "environment", "pull_request", "added", "changed", "destroyed", "duration_s", "result"]
    return dict(kpis=kpis, charts=charts, tables=tables), header, csv_rows


PANELS = [
    dict(chart="resources", span=7, h=320, title="Resources by module and environment", sub="Everything Terraform manages"),
    dict(chart="budget", span=5, title="Error budget used, 30 days", sub="SLO 99.9% for each service"),
    dict(chart="utilization", span=8, h=270, title="Cluster utilisation, prod", sub="CPU and memory requested, hourly, last 7 days"),
    dict(chart="applies", span=4, h=270, title="Size of recent applies", sub="Resources changed by the last 20 applies; red failed"),
    dict(table="pods", span=7, title="Workloads, prod", sub="State at 23:59 on Oct 6"),
    dict(table="recent", span=5, title="Latest applies", sub="From CI, newest first"),
    dict(table="catalog", span=12, title="Tables in the Iceberg REST catalog", sub="Prod warehouse"),
]

FLOW = [
    ("Network & cluster", "Terraform: VPC + EKS", "Private subnets in three zones and an EKS cluster with on-demand nodes for services and spot nodes for Spark."),
    ("Storage & state", "Terraform: S3 + RDS", "A versioned S3 warehouse bucket and PostgreSQL for the Iceberg catalog and Airflow's metadata."),
    ("Platform services", "Helm, via Terraform", "Iceberg REST catalog, Trino, the Spark operator and Airflow, each a Helm release with values per environment."),
    ("Images", "Docker", "Spark and Airflow images built in CI, scanned, and pinned by digest in the Helm values."),
    ("Observe", "Prometheus + Grafana", "Cluster, service and SLO dashboards provisioned as code, with alerts on error-budget burn."),
    ("Promote", "Pull requests", "Plans are reviewed on every PR; applies run from CI with remote state and locking."),
]

STEPS = [
    ("One set of modules, three sizes.", "Environments differ only in a tfvars file: node counts, instance sizes, replicas and retention. Staging really is a smaller prod."),
    ("State is shared and locked.", "Remote state lives in S3 with a DynamoDB lock, one workspace per environment. Nobody applies from a laptop."),
    ("Rebuilding is rehearsed.", "Once a month CI destroys dev and recreates it from nothing, then runs smoke tests that query Iceberg through Trino. The last run took 18 minutes."),
    ("Drift is a bug.", "A nightly plan runs against every environment. Any difference opens an issue; this month two hand edits were found and moved into code."),
]

MODELS = [
    dict(name="iceberg_tables", kind="PostgreSQL · behind the Iceberg REST catalog", columns=[
        ("catalog_name", "varchar", "Catalog the table belongs to"),
        ("table_namespace", "varchar", "For example `analytics`"),
        ("table_name", "varchar", "For example `orders`"),
        ("metadata_location", "varchar", "Current metadata file in S3; a commit swaps it atomically"),
        ("previous_metadata_location", "varchar", "The one before, for safe concurrent commits"),
    ]),
    dict(name="environments/*.tfvars", kind="Settings that differ per environment", columns=[
        ("service_nodes", "number", "dev 2 · staging 2 · prod 4"),
        ("spark_max_nodes", "number", "Spot node limit: dev 4 · staging 6 · prod 20"),
        ("rds_instance_class", "string", "`db.t4g.medium` in dev and staging, `db.r6g.large` in prod"),
        ("trino_workers", "number", "dev 1 · staging 2 · prod 4"),
        ("backup_retention_days", "number", "dev 1 · staging 7 · prod 30"),
    ]),
    dict(name="terraform_applies", kind="From CI logs · the CSV download", columns=[
        ("applied_at", "timestamp", "When the apply finished"),
        ("environment", "string", "`dev`, `staging` or `prod`"),
        ("pull_request", "string", "PR that triggered it"),
        ("added / changed / destroyed", "int", "Resources in the plan"),
        ("duration_s / result", "int / string", "How long it took and how it ended"),
    ]),
]

CODE = [
    dict(file="main.tf", lang="hcl",
         caption="Root module: network, cluster, storage, database and the platform services, all parameterised by environment.",
         code='''
terraform {
  required_version = ">= 1.8"
  required_providers {
    aws  = { source = "hashicorp/aws", version = "~> 5.60" }
    helm = { source = "hashicorp/helm", version = "~> 2.15" }
  }
  backend "s3" {
    bucket         = "dataplatform-tf-state"
    key            = "platform/terraform.tfstate"   # one workspace per environment
    region         = "us-east-1"
    dynamodb_table = "dataplatform-tf-locks"
    encrypt        = true
  }
}

locals {
  name = "dp-${terraform.workspace}"
}

module "network" {
  source     = "./modules/network"
  name       = local.name
  cidr_block = var.vpc_cidr
  azs        = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

module "eks" {
  source          = "terraform-aws-modules/eks/aws"
  version         = "~> 20.24"
  cluster_name    = local.name
  cluster_version = "1.31"
  vpc_id          = module.network.vpc_id
  subnet_ids      = module.network.private_subnet_ids

  eks_managed_node_groups = {
    services = {
      instance_types = [var.service_instance_type]
      min_size       = var.service_nodes
      max_size       = var.service_nodes + 2
      desired_size   = var.service_nodes
    }
    spark = {
      capacity_type  = "SPOT"
      instance_types = ["r6i.2xlarge", "r6a.2xlarge", "r5.2xlarge"]
      min_size       = 0
      max_size       = var.spark_max_nodes
      desired_size   = 0
      labels         = { workload = "spark" }
      taints = {
        spark = { key = "workload", value = "spark", effect = "NO_SCHEDULE" }
      }
    }
  }
}

module "warehouse" {
  source      = "./modules/storage"
  bucket_name = "${local.name}-warehouse"
  versioning  = true
}

module "catalog_db" {
  source                = "./modules/postgres"
  identifier            = "${local.name}-catalog"
  instance_class        = var.rds_instance_class
  subnet_ids            = module.network.private_subnet_ids
  allowed_sg_ids        = [module.eks.node_security_group_id]
  backup_retention_days = var.backup_retention_days
}

module "platform" {
  source        = "./modules/platform"   # Helm releases, see helm.tf
  environment   = terraform.workspace
  warehouse_uri = "s3://${module.warehouse.bucket_name}/warehouse"
  catalog_db    = module.catalog_db.connection
  trino_workers = var.trino_workers
  image_digests = var.image_digests
}
'''),
    dict(file="modules/platform/helm.tf", lang="hcl",
         caption="The lakehouse services as Helm releases. Trino depends on the catalog, so it is installed after it.",
         code='''
resource "helm_release" "iceberg_rest" {
  name             = "iceberg-rest"
  namespace        = "lakehouse"
  create_namespace = true
  repository       = "oci://registry.example.com/charts"
  chart            = "iceberg-rest-catalog"
  version          = "0.4.2"

  values = [templatefile("${path.module}/values/iceberg-rest.yaml", {
    warehouse = var.warehouse_uri
    jdbc_url  = "jdbc:postgresql://${var.catalog_db.host}:5432/${var.catalog_db.database}"
    image     = "registry.example.com/iceberg-rest@${var.image_digests.iceberg_rest}"
  })]

  set_sensitive {
    name  = "catalog.jdbc.password"
    value = var.catalog_db.password
  }
}

resource "helm_release" "trino" {
  name       = "trino"
  namespace  = "lakehouse"
  repository = "https://trinodb.github.io/charts"
  chart      = "trino"
  version    = "0.30.0"

  values = [templatefile("${path.module}/values/trino.yaml", {
    workers          = var.trino_workers
    iceberg_rest_uri = "http://iceberg-rest.lakehouse.svc:8181"
    warehouse        = var.warehouse_uri
  })]

  depends_on = [helm_release.iceberg_rest]
}

resource "helm_release" "spark_operator" {
  name             = "spark-operator"
  namespace        = "spark"
  create_namespace = true
  repository       = "https://kubeflow.github.io/spark-operator"
  chart            = "spark-operator"
  version          = "2.0.2"

  set {
    name  = "spark.jobNamespaces[0]"
    value = "spark"
  }
}
'''),
    dict(file="spark/Dockerfile", lang="docker",
         caption="The Spark image used by every job: Spark 3.5 plus the Iceberg runtime and AWS bundle, pinned versions only.",
         code='''
# Spark 3.5 with Iceberg and S3 support, for jobs run by the Spark operator
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

ARG ICEBERG_VERSION=1.6.1
ARG MAVEN=https://repo1.maven.org/maven2/org/apache/iceberg

USER root
ADD --chmod=644 ${MAVEN}/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

COPY jobs/ /opt/spark/work-dir/jobs/
USER spark
'''),
]

HIGHLIGHTS = [
    ("A rebuild drill every month",
     "On Sep 28 CI destroyed dev and recreated it from nothing in 18 minutes. Smoke tests then created an Iceberg table, wrote to it with Spark and read it back through Trino."),
    ("The one failed apply",
     "PR #296 failed in staging when AWS refused more spot vCPUs. Terraform had recorded what it already created, so after the quota was raised the re-run only added the rest."),
    ("Airflow is spending its budget",
     "The Airflow scheduler restarted four times after hitting its memory limit, using about 78% of its monthly error budget. The limit is raised in the next PR."),
    ("Spot for Spark, on-demand for services",
     "Spark runs on spot nodes behind a taint, so catalog and query services never land there. Nightly jobs cost about 65% less than on on-demand nodes."),
]
