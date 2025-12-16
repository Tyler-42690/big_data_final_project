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

project-root/
├── src/
│   └── app/
│       ├── __init__.py
│       ├── db.py
│       ├── etl.py
│       ├── models.py
│       └── main.py
├── tests/
│   ├── test_db.py
│   └── test_etl.py
├── data/
│   └── raw/
│       └── proofread_connections_783.feather
├── docker-compose.yaml
├── Dockerfile
├── pyproject.toml
├── uv.lock
├── test.py
└── README.md

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

