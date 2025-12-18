# Flywire Data Visualization
This codebase allows for visualization of the proofread flywire dataset (over 16 million rows).  
It is designed to serve and visualize flywire data by user determined inputs like neurotransmitter probabilities, synapse count between neurons, and neuropils.

---

## ⚙️ Setup

### 1. Install Dependencies

Make sure you have Python 3.13+ and install the required packages:

```bash
uv sync
```

By default, the neo4j database and aggregations run on (bolt port):
```bash
http://127.0.0.1:7687
```
### 2. Project Layout

This project uses a `src/`-based layout.  
The Python package lives in `src/app`, which helps prevent accidental
imports from the project root and improves packaging correctness.

🔹 Step 1: Download the datafile from the flywire site at and look for the proofread feather dataset (proofread_connections_783.feather):
```bash
    https://zenodo.org/records/10676866
```

Once downloaded, set up a directory called data\raw relative to this README file. Once completed, the directory structure should look like:

```bash
project-root/
├── src/                    # Source-root layout (Python package lives here)
│   └── app/
│       ├── __init__.py     # Marks app as a Python package
│       ├── db.py           # Neo4j connection and query utilities
│       ├── etl.py          # Feather → Neo4j ETL pipeline
│       ├── models.py       # Pydantic data models
│       └── main.py         # Application entry point
│
├── data/                   # Local datasets
│   └── raw/
│       └── proofread_connections_783.feather
├── docker-compose.yaml     # Multi-service setup (app, Neo4j, tests, mypy)
├── pyproject.toml          # Project metadata and dependencies
├── uv.lock                 # Locked dependency graph (uv)
├── test.py                 # Unit and integration tests (pytest)
└── README.md               # Project documentation
```
🔹 Step 2: Using docker-compose.yaml

Install Docker Desktop support from the VS Code Extensions Marketplace.

Open Docker Desktop.

Run the unit testing (pytest) and linting (mypy) container services automatically, or via the following docker-compose commands.

```docker-compose
    'C:\Program Files\Docker\Docker\resources\bin\docker.EXE' compose -f 'docker-compose.yaml' up -d --build 'mypy'
```
and
```docker-compose
    'C:\Program Files\Docker\Docker\resources\bin\docker.EXE' compose -f 'docker-compose.yaml' up -d --build 'pytest'
```

🔹 Step 3: Running the app
Note that the docker-compose.yaml file also automatically installs uv for dependency management for ease of use. To then run the app and database:
```docker-compose
    'C:\Program Files\Docker\Docker\resources\bin\docker.EXE' compose -f 'docker-compose.yaml' up -d --build 'python_app'
```

## Architecture
The architecture of this app is as follows:
<img width="756" height="329" alt="image" src="https://github.com/user-attachments/assets/48c4cdd2-83f9-4de9-8e09-1154cf26456f" />

## Step-by-step (Docker)

1) Start Neo4j + API containers
```bash
docker compose up -d --build neo4j python_app
```

2) Load data into Neo4j (choose ONE)

2a) Load a small subset (example: first 100k rows)
```bash
docker compose exec -T python_app python -c "from app.db import get_driver, close_driver; from app.etl import load_connections_arrow; d=get_driver(); load_connections_arrow(d, feather_path='data/raw/proofread_connections_783.feather', max_rows=100000, batch_size=10000, clear_graph=True); close_driver(d)"
```

2b) Load the entire dataset (no row limit)

Note: this can take a long time and requires substantial CPU/RAM/disk. If it fails due to memory/time, reduce scope using `max_rows=...`.

```bash
docker compose exec -T python_app python -c "from app.db import get_driver, close_driver; from app.etl import load_connections_arrow; d=get_driver(); load_connections_arrow(d, feather_path='data/raw/proofread_connections_783.feather', max_rows=None, batch_size=10000, clear_graph=True); close_driver(d)"
```

3) Open Swagger UI OR test via terminal (two ways)

Option A (browser):

http://127.0.0.1:8000/docs

Option B (terminal):
```bash
curl -s http://127.0.0.1:8000/health
```

4) Try an example query (demo root_id=720575940624547622)

You can retrieve the connectome information for the specified neuron, identified by root_id (e.g., root_id=720575940624547622), filtered by synapse count threshold, neurotransmitter types, and neuropil regions.

```bash
curl "http://127.0.0.1:8000/neuron/720575940624547622/circuit?k=2&threshold=10"
```

5) Stop containers
```bash
docker compose down
```

## aggregate.py (what it does + how to run)

### What it does

Reads the raw Feather file in chunks and writes a Parquet *dataset* partitioned by `neuropil`.
It also adds:

- `dominant_score`: the max neurotransmitter probability per row
- `dominant_nt`: the neurotransmitter name(s) at that max (ties become comma-separated)

### How to run

Option A: run inside Docker (recommended if your local Python env is not set up)

```bash
docker compose up -d --build python_app
docker compose exec -T python_app python aggregate.py \
    --input data/raw/proofread_connections_783.feather \
    --output-dir data/aggregates \
    --dataset-dirname connections_with_dominant_nt_by_neuropil
```

Option B: run locally (if you have a working local environment)

```bash
uv sync
python aggregate.py --input data/raw/proofread_connections_783.feather
```

Common useful flags:

- Limit rows for a quick test: `--max-rows 100000`
- Drop the 6 probability columns in output (keeps `dominant_nt`/`dominant_score`): `--drop-prob-cols`

### Output

By default it writes under `data/aggregates/` with a Hive-style layout like:

`data/aggregates/connections_with_dominant_nt_by_neuropil/neuropil=<value>/part-*.parquet`

## Step-by-step: Open the Dashboard

Goal: get to the interactive dashboard at:

http://127.0.0.1:8000/dashboard/syn_count

This dashboard depends on a **precomputed summary JSON** served by the API at `/dataset/summary`.

### (Requirement) Push aggregated data back into Neo4j

If your rubric requires that the *aggregated/gold* dataset is pushed back into the Big Data system (Neo4j),
you can store the dashboard summary JSON inside Neo4j and serve `/dataset/summary` from Neo4j.

1) Generate the summary JSON (still required once)

```bash
python summarize_data.py --input data/raw/proofread_connections_783.feather --out-json data/summary/flywire_summary.json
```

Purpose: produces the aggregated/gold summary artifact.

Alternative (Gold from Parquet): build the dashboard summary *from the Parquet dataset*

If you already ran `aggregate.py` (Parquet partitioned by `neuropil`), you can generate the dashboard summary
by grouping the Parquet data by `neuropil` + `dominant_nt` and computing synapse-count histograms.
These histograms support fast counts at different thresholds via suffix sums.

```bash
python summarize_parquet.py \
    --dataset-root data/aggregates/connections_with_dominant_nt_by_neuropil \
    --out-json data/summary/flywire_summary_from_parquet.json
```

Purpose: creates aggregated counts from Parquet (Gold) without re-scanning the raw Feather.

2) Push that summary into Neo4j

Docker:

```bash
docker compose exec -T python_app python -m app.aggregates_to_neo4j \
    --summary-json data/summary/flywire_summary.json \
    --dataset-id flywire
```

If you used Parquet-based summary:

```bash
docker compose exec -T python_app python -m app.aggregates_to_neo4j \
    --summary-json data/summary/flywire_summary_from_parquet.json \
    --dataset-id flywire
```

Local:

```bash
PYTHONPATH=src python -m app.aggregates_to_neo4j \
    --summary-json data/summary/flywire_summary.json \
    --dataset-id flywire
```

Purpose: writes a `(:DatasetSummary {id: 'flywire'})` node into Neo4j containing the aggregated summary JSON.

3) Tell the API to read `/dataset/summary` from Neo4j

- Set env var `FLYWIRE_SUMMARY_SOURCE=neo4j`
- Optional: set `FLYWIRE_SUMMARY_DATASET_ID=flywire` (default)

To ensure *dashboard endpoints* read from the Big Data system, you can also force the pairs endpoint to Neo4j:

- Set env var `FLYWIRE_PAIRS_SOURCE=neo4j`

Docker (example):

```bash
docker compose exec -T python_app sh -lc 'export FLYWIRE_SUMMARY_SOURCE=neo4j; curl -s http://127.0.0.1:8000/dataset/summary | head'
```

Purpose: demonstrates the dashboard summary is now served directly from the Big Data system.

### Prerequisites (one-time)

1) Put the raw dataset in the expected location

- File: `data/raw/proofread_connections_783.feather`
- Download from: https://zenodo.org/records/10676866

2) Make sure required ports are available

- API: `8000`
- Neo4j (Bolt): `7687`

---

## Option A (recommended): Everything in Docker

### A1) Start Neo4j + API

```bash
docker compose up -d --build neo4j python_app
```

Purpose: starts the FastAPI server (port 8000) and Neo4j (port 7687).

Optional checks:

```bash
curl -s http://127.0.0.1:8000/health
```

Purpose: confirms the API is reachable.

```bash
docker compose logs -f python_app
```

Purpose: tails API logs if something looks stuck.

### A2) Generate the required summary JSON (required for the dashboard)

```bash
docker compose exec -T python_app python summarize_data.py \
    --input data/raw/proofread_connections_783.feather \
    --out-json data/summary/flywire_summary.json
```

Purpose: scans the Feather file in streaming mode and writes the summary JSON that the dashboard reads via `/dataset/summary`.

Useful options:

- Quick test (much faster, less accurate):

```bash
docker compose exec -T python_app python summarize_data.py \
    --input data/raw/proofread_connections_783.feather \
    --max-rows 100000 \
    --out-json data/summary/flywire_summary.json
```

- Custom output path (and tell the API where to find it):

```bash
docker compose exec -T python_app sh -lc \
    'export FLYWIRE_SUMMARY_JSON=/app/data/summary/my_summary.json && \
     python summarize_data.py --input data/raw/proofread_connections_783.feather --out-json "$FLYWIRE_SUMMARY_JSON"'
```

Purpose: sets `FLYWIRE_SUMMARY_JSON` so `/dataset/summary` reads your custom file.

### A3) (Optional) Build the Parquet dataset for a faster/more complete pairs list

```bash
docker compose exec -T python_app python aggregate.py \
    --input data/raw/proofread_connections_783.feather \
    --output-dir data/aggregates \
    --dataset-dirname connections_with_dominant_nt_by_neuropil
```

Purpose: creates a Parquet dataset partitioned by `neuropil` that the dashboard uses for `/dataset/pairs` when filtering by neuropil.

Options:

- Smaller run:

```bash
docker compose exec -T python_app python aggregate.py \
    --input data/raw/proofread_connections_783.feather \
    --max-rows 100000
```

- If you wrote the dataset elsewhere, point the API at it:

Set env var `FLYWIRE_PAIRS_PARQUET` to the dataset root (default is `data/aggregates/connections_with_dominant_nt_by_neuropil`).

### A4) (Optional but recommended) Load edges into Neo4j for full graph queries

This powers the “Search by neuron” panel and all graph endpoints.

- Load a small subset:

```bash
docker compose exec -T python_app python -c "from app.db import get_driver, close_driver; from app.etl import load_connections_arrow; d=get_driver(); load_connections_arrow(d, feather_path='data/raw/proofread_connections_783.feather', max_rows=100000, batch_size=10000, clear_graph=True); close_driver(d)"
```

- Load the full dataset (slow / heavy):

```bash
docker compose exec -T python_app python -c "from app.db import get_driver, close_driver; from app.etl import load_connections_arrow; d=get_driver(); load_connections_arrow(d, feather_path='data/raw/proofread_connections_783.feather', max_rows=None, batch_size=10000, clear_graph=True); close_driver(d)"
```

Purpose: creates `Neuron` nodes and `CONNECTS_TO` relationships with synapse/neurotransmitter properties.

### A5) Open the dashboard

Open in your browser:

http://127.0.0.1:8000/dashboard/syn_count

If you get a 404 for `/dataset/summary`, re-run step A2.

### A6) Stop everything

```bash
docker compose down
```

Purpose: stops containers (and keeps Neo4j data in Docker volumes).

---

## Option B: Run the API locally (Neo4j still in Docker)

Use this if you prefer local Python dev (e.g., `--reload`) but still want an easy Neo4j.

### B1) Start Neo4j

```bash
docker compose up -d neo4j
```

Purpose: runs Neo4j on `bolt://127.0.0.1:7687`.

### B2) Install dependencies

```bash
uv sync
```

Purpose: installs the Python dependencies into your environment.

### B3) Generate the summary JSON (required)

```bash
python summarize_data.py \
    --input data/raw/proofread_connections_783.feather \
    --out-json data/summary/flywire_summary.json
```

Purpose: generates the file that `/dataset/summary` serves.

### B4) Run the API server

```bash
PYTHONPATH=src \
NEO4J_URI=bolt://127.0.0.1:7687 \
NEO4J_USER=neo4j \
NEO4J_PASSWORD=example_password \
uvicorn app.api:app --app-dir src --host 127.0.0.1 --port 8000 --reload
```

Purpose: starts the FastAPI server locally with hot reload.

### B5) Open the dashboard

http://127.0.0.1:8000/dashboard/syn_count


---

## Deliverable Statement + Verification Steps (Rubric)

### Deliverable statement (what we built)

This project produces an aggregated “gold” dashboard summary from the raw FlyWire dataset and **pushes that aggregated artifact back into the Big Data system (Neo4j)**. The FastAPI service can be configured to serve the dashboard summary directly **from Neo4j**, and the dashboard reads its aggregated charts/totals from the `/dataset/summary` API endpoint.

Concretely:

- Aggregation artifact: a JSON summary (histograms + counts) generated from the dataset.
- Big Data write-back: the full JSON is stored in Neo4j as `(:DatasetSummary {id: <dataset_id>})` with a `summary_json` property.
- Big Data read path: the API serves `/dataset/summary` from Neo4j when `FLYWIRE_SUMMARY_SOURCE=neo4j`.
- Dashboard provenance: the dashboard UI consumes `/dataset/summary`, so it is driven by aggregated data served from Neo4j.

### Verification steps (copy/paste; screenshot-friendly)

The commands below demonstrate (1) the aggregate exists in Neo4j and (2) the dashboard summary endpoint is served from Neo4j.

#### 1) Confirm the aggregated summary exists in Neo4j

Using Docker (recommended):

```bash
docker compose exec -T neo4j cypher-shell -u neo4j -p example_password \
    "MATCH (s:DatasetSummary {id: 'flywire'}) RETURN s.id AS id, size(s.summary_json) AS summaryBytes"
```

Expected: one row returned with a non-zero `summaryBytes`.

#### 2) Confirm the API is configured to read the summary from Neo4j

If running via `docker compose up ...`, check the container env:

```bash
docker compose exec -T python_app sh -lc 'echo FLYWIRE_SUMMARY_SOURCE=$FLYWIRE_SUMMARY_SOURCE; echo FLYWIRE_SUMMARY_DATASET_ID=$FLYWIRE_SUMMARY_DATASET_ID'
```

Expected: `FLYWIRE_SUMMARY_SOURCE=neo4j` and `FLYWIRE_SUMMARY_DATASET_ID=flywire`.

#### 3) Fetch the dashboard summary through the API

```bash
curl -s http://127.0.0.1:8000/dataset/summary | head
```

Expected: JSON output (the aggregated summary). This is the same artifact that is stored in Neo4j and served to the dashboard.

#### 4) Open the dashboard (uses the aggregated API summary)

Open:

http://127.0.0.1:8000/dashboard/syn_count

Expected: charts render and filters work; charts/totals are populated from `/dataset/summary`.

#### (Optional) Strict “all dashboard data from Neo4j” mode

If your rubric requires that even the dashboard *pairs list* comes from the Big Data system, set:

- `FLYWIRE_PAIRS_SOURCE=neo4j`

Note: this requires the relationships to be loaded into Neo4j via the ETL step; otherwise `/dataset/pairs` will return fewer/empty results.





