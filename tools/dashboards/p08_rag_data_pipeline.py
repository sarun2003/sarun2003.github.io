"""Project 08: RAG data pipeline (ingest, chunk, embed, index, evaluate)."""

import datetime as dt

from common import col, day_label, day_list, quantile, r0, r2, r4, status

META = dict(
    num="08",
    slug="rag-data-pipeline",
    seed=808,
    title="RAG Data Pipeline",
    short="RAG Pipeline",
    category="AI data pipeline",
    tagline="Company documents ingested, chunked, embedded and indexed in Pinecone every hour, with retrieval quality measured nightly against labelled questions.",
    tile_stack="Python · Airflow · LangChain · Pinecone",
    summary=(
        "An assistant can only answer from what it can find. This pipeline keeps a vector index in step with six document sources: product docs, support tickets, "
        "policies, the engineering wiki, release notes and FAQs. Every hour Airflow finds new, changed and deleted documents, LangChain loaders and splitters turn "
        "them into overlapping chunks with metadata, an embedding model turns the chunks into vectors, and Pinecone stores them in one namespace per source. "
        "Each night 500 labelled questions check that the right passage still comes back."
    ),
    stack=["Python", "Apache Airflow", "LangChain", "Pinecone", "PostgreSQL"],
    window="Last 30 days · Sep 7 – Oct 6, 2026",
    csv="ingestion_daily.csv",
    csv_desc="Documents added, updated and deleted, chunks, embedding tokens and cost, latency and recall per day",
    how_note="Ingest → chunk → embed → index → evaluate, incrementally, every hour.",
    model_note="A manifest in PostgreSQL tracks every document so only changed ones are re-embedded; Pinecone holds the vectors.",
    code_note="Incremental indexing, the hourly DAG, and the nightly retrieval evaluation.",
)

END = dt.date(2026, 10, 6)
SOURCES = [("support_tickets", 21_400, 214_000), ("product_docs", 9_800, 382_000), ("eng_wiki", 7_600, 296_000),
           ("faqs", 4_100, 33_000), ("release_notes", 3_200, 89_000), ("policies", 2_100, 106_000)]
QUALITY = [("product_docs", 0.94, 0.81), ("support_tickets", 0.84, 0.66), ("policies", 0.95, 0.86),
           ("eng_wiki", 0.90, 0.74), ("release_notes", 0.93, 0.79), ("faqs", 0.96, 0.88)]
PRICE_PER_M_TOKENS = 0.13


def build(g):
    days = day_list(END, 30)
    labels = [day_label(d) for d in days]
    added, updated, deleted, chunks, tokens, cost, p50, p95, recall = ([] for _ in range(9))
    for i, d in enumerate(days):
        weekend = d.weekday() >= 5
        a = r0((95 if weekend else 190) * g.jitter(0.2))
        u = r0((150 if weekend else 430) * g.jitter(0.18))
        x = r0((12 if weekend else 38) * g.jitter(0.3))
        if d == dt.date(2026, 9, 15):
            u = 2_140  # the policy handbook was reorganised
        if d == dt.date(2026, 10, 1):
            a = 640  # v9 release notes and docs
        c = r0((a + u) * 23.2 * g.jitter(0.05))
        t = r0(c * 431 * g.jitter(0.03))
        added.append(a); updated.append(u); deleted.append(x); chunks.append(c); tokens.append(t)
        cost.append(r2(t / 1e6 * PRICE_PER_M_TOKENS))
        lat50 = 94 * g.jitter(0.05)
        lat95 = 171 * g.jitter(0.06)
        if d == dt.date(2026, 9, 22):
            lat50, lat95 = 121, 238
        p50.append(r0(lat50)); p95.append(r0(lat95))
        recall.append(round(min(0.93, 0.885 + 0.0011 * i + g.gauss(0.004)), 3))

    docs_total = sum(s[1] for s in SOURCES)
    vectors_total = sum(s[2] for s in SOURCES)
    kpis = [
        dict(label="Documents indexed", value=docs_total, fmt="int", delta=0.048, deltaLabel="vs Sep 6"),
        dict(label="Vectors", value=vectors_total, fmt="compact", note="1,536 dimensions, cosine"),
        dict(label="Recall@5", value=recall[-1], fmt="ratio", delta=round(recall[-1] / recall[0] - 1, 4), deltaLabel="vs Sep 7", spark=recall),
        dict(label="Retrieval p95", value=r0(quantile(p95, 0.5)), fmt="ms", status="good", statusLabel="Budget 250 ms"),
        dict(label="Change to searchable", value=14, fmt="min", status="good", statusLabel="Median, hourly sync"),
        dict(label="Embedding cost", value=round(sum(cost), 2), fmt="usd2", note="30 days, only changed documents"),
    ]

    bins = ["0–64", "64–128", "128–192", "192–256", "256–320", "320–384", "384–448", "448–512"]
    shares = [0.041, 0.079, 0.108, 0.087, 0.075, 0.105, 0.211, 0.294]
    chunk_counts = [r0(vectors_total * s) for s in shares]

    charts = dict(
        ingestion=dict(
            kind="bar", x=labels, fmt="int", stack=True, labelEvery=6, xLabel="Day",
            desc="Documents added, updated and deleted at the sources each day. Sep 15 is the policy handbook reorganisation; Oct 1 is the v9 release.",
            series=[dict(name="Updated", data=updated), dict(name="Added", data=added), dict(name="Deleted", data=deleted)],
        ),
        sources=dict(
            kind="bar", horizontal=True, x=[s[0] for s in SOURCES], fmt="compact", valueLabels=True, xLabel="Source",
            desc="Documents in the index by source.",
            series=[dict(name="Documents", data=[s[1] for s in SOURCES])],
        ),
        latency=dict(
            kind="line", x=labels, fmt="ms", labelEvery=6, xLabel="Day", yMin=0, yMax=300,
            desc="Retrieval latency per day, query embedding plus vector search, p50 and p95, with the 250 millisecond budget.",
            series=[dict(name="p50", data=p50), dict(name="p95", data=p95)],
            thresholds=[dict(value=250, label="Budget 250 ms", status="critical")],
        ),
        quality=dict(
            kind="bar", horizontal=True, x=[q[0] for q in QUALITY], fmt="ratio", yMin=0, yMax=1, valueLabels=True, xLabel="Namespace", barCategoryGap="28%",
            desc="Recall at 5 and mean reciprocal rank on the labelled question set, by namespace. Support tickets score lowest.",
            series=[dict(name="Recall@5", data=[q[1] for q in QUALITY]), dict(name="MRR", data=[q[2] for q in QUALITY])],
        ),
        chunks=dict(
            kind="bar", x=bins, fmt="compact", xLabel="Tokens per chunk", labelEvery=0,
            desc="Chunks in the index by size in tokens. The splitter targets 512 tokens; short chunks are the ends of documents and short support tickets.",
            series=[dict(name="Chunks", data=chunk_counts)],
        ),
    )

    def rank_status(rank):
        if rank is None:
            return status("critical", "Miss")
        return status("good", "Hit at 1") if rank == 1 else status("neutral", "Hit at %d" % rank)

    samples = [
        ("How many PTO days carry over into next year?", "policies", "Time-off policy, section 4.2", 0.83, 1, 92),
        ("Can I export dashboards to PDF on the Starter plan?", "product_docs", "Exporting dashboards", 0.79, 1, 88),
        ("Why does SSO fail with error AADSTS50011?", "support_tickets", "Ticket 48211", 0.74, 2, 117),
        ("What changed in the v9.2 API rate limits?", "release_notes", "Release 9.2", 0.81, 1, 95),
        ("How do I rotate the warehouse service key?", "eng_wiki", "Key rotation runbook", 0.77, 1, 104),
        ("Is there a discount for nonprofits?", "faqs", "Pricing FAQ", 0.85, 1, 79),
        ("Invoices show the wrong currency for one customer", "support_tickets", "Ticket 47702", 0.62, None, 131),
        ("Which regions support data residency?", "product_docs", "Data residency", 0.76, 3, 101),
    ]

    tables = dict(
        index=dict(
            columns=[col("ns", "Namespace", "code"), col("docs", "Documents", "int"), col("vec", "Vectors", "compact"),
                     col("per", "Chunks per doc", "dec1"), col("last", "Last upsert"), col("st", "Status", "status")],
            rows=[dict(ns=n, docs=dc, vec=v, per=round(v / dc, 1), last=["23:58", "23:41", "23:12", "22:05", "21:30", "18:44"][i],
                       st=status("good", "In sync") if n != "support_tickets" else status("warning", "Low recall"))
                  for i, (n, dc, v) in enumerate(SOURCES)],
        ),
        samples=dict(
            columns=[col("q", "Question", wrap=True), col("ns", "Namespace", "code"), col("top", "Top result"), col("score", "Score", "ratio"),
                     col("lat", "Latency", "ms"), col("st", "Result", "status")],
            rows=[dict(q=q, ns=ns, top=t, score=s, lat=l, st=rank_status(r)) for q, ns, t, s, r, l in samples],
        ),
    )

    header = ["date", "docs_added", "docs_updated", "docs_deleted", "chunks_upserted", "embedding_tokens", "embedding_cost_usd",
              "retrieval_p50_ms", "retrieval_p95_ms", "recall_at_5"]
    rows = [[d.isoformat(), added[i], updated[i], deleted[i], chunks[i], tokens[i], cost[i], p50[i], p95[i], recall[i]] for i, d in enumerate(days)]
    return dict(kpis=kpis, charts=charts, tables=tables), header, rows


PANELS = [
    dict(chart="ingestion", span=8, h=290, title="Document changes per day", sub="Detected at the sources and applied to the index"),
    dict(chart="sources", span=4, h=290, title="Documents by source", sub="One Pinecone namespace each"),
    dict(chart="latency", span=6, h=300, title="Retrieval latency", sub="Query embedding plus vector search"),
    dict(chart="quality", span=6, h=300, title="Retrieval quality by namespace", sub="500 labelled questions, Oct 6"),
    dict(chart="chunks", span=5, h=270, title="Chunk sizes", sub="Tokens per chunk; the target is 512"),
    dict(table="index", span=7, title="Index namespaces", sub="Pinecone index `kb-prod`"),
    dict(table="samples", span=12, title="Sample retrievals from the eval set", sub="Top result for each question and where the right passage ranked"),
]

FLOW = [
    ("Detect", "Airflow + source APIs", "Every hour, list documents changed since the last run in each source, by modified time and content hash."),
    ("Load", "LangChain loaders", "Confluence, Zendesk, Markdown docs and PDFs become text plus metadata: source, URL, title, access group, updated date."),
    ("Chunk", "Recursive splitter", "About 512-token chunks with a 64-token overlap, split on headings and paragraphs first so chunks stay coherent."),
    ("Embed", "Embedding model", "1,536-dimension vectors, 256 chunks per request, with retries and a token budget per run."),
    ("Index", "Pinecone", "One namespace per source. Vector ids are `doc_id#chunk`, so updates overwrite and deletes are exact."),
    ("Evaluate", "Nightly eval", "500 labelled questions measure recall@5 and MRR per namespace; a drop alerts the team."),
]

STEPS = [
    ("Only changed documents are re-embedded.", "A content hash per document skips unchanged ones, which keeps embedding cost under a dollar a day."),
    ("Deletes are first-class.", "When a document is deleted or archived at the source, its chunk ids are removed from Pinecone in the same run, so the assistant never cites a retired policy."),
    ("Metadata does the filtering.", "Every vector carries its source, access group and updated date, so search can be limited to what a user is allowed to see before ranking."),
    ("Quality is measured, not assumed.", "Retrieval is scored every night against labelled questions. A chunking or model change ships only if recall@5 does not drop."),
]

MODELS = [
    dict(name="kb.documents", kind="PostgreSQL · the manifest", columns=[
        ("doc_id", "text", "Stable id from the source system"),
        ("source / url / title", "text", "Where it lives"),
        ("content_hash", "char(64)", "SHA-256 of the text; unchanged means skip"),
        ("chunk_count", "int", "How many vectors the document has"),
        ("updated_at / deleted_at", "timestamptz", "From the source"),
        ("last_indexed_at", "timestamptz", "When the index last matched this version"),
    ]),
    dict(name="kb-prod", kind="Pinecone index · 1,536 dims · cosine", columns=[
        ("id", "string", "`doc_id#chunk_no`"),
        ("values", "float[1536]", "The embedding"),
        ("metadata.doc_id / title / url", "string", "Used for citations"),
        ("metadata.access_group", "string", "Filter so users only see what they may read"),
        ("metadata.updated_at", "string", "Lets the assistant prefer recent content"),
        ("metadata.text", "string", "The chunk itself, returned with matches"),
    ]),
    dict(name="ingestion_daily", kind="Rollup · the CSV download", columns=[
        ("date", "date", "Day"),
        ("docs_added / docs_updated / docs_deleted", "int", "Changes found at the sources"),
        ("chunks_upserted / embedding_tokens", "int", "Work done"),
        ("embedding_cost_usd", "decimal", "Tokens × price"),
        ("retrieval_p50_ms / p95_ms / recall_at_5", "int / double", "Search speed and quality"),
    ]),
]

CODE = [
    dict(file="index_documents.py", lang="python",
         caption="Incremental indexing: skip unchanged documents, upsert new chunks, and delete chunks a shorter version no longer has.",
         code='''
import hashlib

from langchain_openai import OpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pinecone import Pinecone

splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
    encoding_name="cl100k_base",
    chunk_size=512,
    chunk_overlap=64,
    separators=["\\n## ", "\\n### ", "\\n\\n", "\\n", ". ", " "],
)
embeddings = OpenAIEmbeddings(model="text-embedding-3-large", dimensions=1536, chunk_size=256)
index = Pinecone().Index("kb-prod")  # reads PINECONE_API_KEY


def index_document(doc, manifest) -> int:
    """Index one document if it changed. Returns the number of chunks written."""
    meta = doc.metadata
    digest = hashlib.sha256(doc.page_content.encode("utf-8")).hexdigest()
    previous = manifest.get(meta["doc_id"])
    if previous and previous.content_hash == digest:
        return 0  # unchanged: no embedding cost

    chunks = splitter.split_documents([doc])
    vectors = embeddings.embed_documents([c.page_content for c in chunks])
    records = [
        {
            "id": f"{meta['doc_id']}#{n}",
            "values": vector,
            "metadata": {
                "doc_id": meta["doc_id"],
                "title": meta["title"],
                "url": meta["url"],
                "access_group": meta["access_group"],
                "updated_at": meta["updated_at"],
                "chunk_no": n,
                "text": chunk.page_content,
            },
        }
        for n, (chunk, vector) in enumerate(zip(chunks, vectors))
    ]
    for start in range(0, len(records), 100):
        index.upsert(vectors=records[start:start + 100], namespace=meta["source"])

    # A shorter new version leaves old chunks behind; remove them.
    if previous and previous.chunk_count > len(chunks):
        stale = [f"{meta['doc_id']}#{n}" for n in range(len(chunks), previous.chunk_count)]
        index.delete(ids=stale, namespace=meta["source"])

    manifest.save(meta["doc_id"], content_hash=digest, chunk_count=len(chunks))
    return len(chunks)


def remove_document(doc_id: str, source: str, chunk_count: int) -> None:
    index.delete(ids=[f"{doc_id}#{n}" for n in range(chunk_count)], namespace=source)
'''),
    dict(file="kb_sync.py", lang="python",
         caption="Hourly Airflow DAG: one mapped task per source finds changes, indexes them and removes deleted documents.",
         code='''
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from kb.index_documents import index_document, remove_document
from kb.manifest import Manifest
from kb.sources import changed_since, deleted_since, load

SOURCES = ["product_docs", "support_tickets", "policies", "eng_wiki", "release_notes", "faqs"]


@dag(
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["rag", "knowledge-base"],
)
def kb_sync():
    @task(max_active_tis_per_dag=3)
    def sync_source(source: str, data_interval_start=None) -> dict:
        manifest = Manifest(source)
        written = 0
        for ref in changed_since(source, data_interval_start):
            written += index_document(load(ref), manifest)
        removed = 0
        for doc in deleted_since(source, data_interval_start):
            remove_document(doc.doc_id, source, doc.chunk_count)
            manifest.mark_deleted(doc.doc_id)
            removed += 1
        return {"source": source, "chunks_written": written, "documents_removed": removed}

    @task
    def record_metrics(results: list[dict]) -> None:
        from kb.metrics import write_run_metrics
        write_run_metrics(results)

    record_metrics(sync_source.expand(source=SOURCES))


kb_sync()
'''),
    dict(file="evaluate_retrieval.py", lang="python",
         caption="Nightly evaluation: recall@5 and mean reciprocal rank on 500 labelled questions, per namespace.",
         code='''
from collections import defaultdict

from kb.index_documents import embeddings, index


def evaluate(questions: list[dict], k: int = 5) -> dict:
    """Each question has text, namespace and the doc_ids that contain the answer."""
    hits, reciprocal_ranks, counts = defaultdict(int), defaultdict(float), defaultdict(int)
    for q in questions:
        result = index.query(
            vector=embeddings.embed_query(q["text"]),
            top_k=k,
            namespace=q["namespace"],
            include_metadata=True,
        )
        ranked = [match.metadata["doc_id"] for match in result.matches]
        rank = next((i + 1 for i, doc_id in enumerate(ranked) if doc_id in q["relevant"]), None)
        ns = q["namespace"]
        counts[ns] += 1
        if rank is not None:
            hits[ns] += 1
            reciprocal_ranks[ns] += 1 / rank
    return {
        ns: {"recall_at_5": hits[ns] / n, "mrr": reciprocal_ranks[ns] / n, "questions": n}
        for ns, n in counts.items()
    }
'''),
]

HIGHLIGHTS = [
    ("The policy reorganisation",
     "On Sep 15 HR restructured the policy handbook and about 2,100 documents changed in one afternoon. Only those were re-embedded, about 54K chunks for roughly three dollars, and the old chunks were deleted in the same run."),
    ("Smaller chunks, better answers",
     "Moving from 1,000-token chunks to 512 tokens with heading-aware splitting raised recall@5 on the eval set from 0.86 to 0.91."),
    ("Fast enough to feel instant",
     "Retrieval takes about 170 ms at p95, including embedding the question, inside the assistant's 250 ms budget. Sep 22 came close during an index scale-up."),
    ("Support tickets need more",
     "The support namespace has the lowest recall, 0.84, because tickets are short and repetitive. The next step is hybrid search that also matches product names and error codes as keywords."),
]
