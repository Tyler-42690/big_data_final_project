# Flywire Data Visualization
This codebase allows for visualization of the proofread flywire dataset (over 16 million rows). using **Flask** and **Polars**.  
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

