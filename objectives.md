Choose one Distributed Big Data Platform.
⦁	MongoDB (Atlas or Docker)
⦁	Cassandra
⦁	Neo4j (USED)
⦁	Redis Stack
⦁	Elasticsearch/OpenSearch

These must be real distributed data systems, not RDBMS (MySQL, SQL Server, PostgreSQL, Oracle, Teradata).
Tools like VSCode, Anaconda, Jupyter are IDEs, not Big Data platforms.

 

Dataset Requirements

At least 750,000 rows. More than Million will be great. (REQUIREMENT MET, 16M rows)

At least 8 meaningful columns (REQUIREMENT MET).

Can be from REST API, Kafka stream, or a public dataset (PUBLIC DATASET CHOSEN).

Format can be JSON, CSV, Parquet, Avro or anything supported by the NOSQL databases. (FEATHER)

 

Architecture and Setup

Use Docker Compose or Free-tier Cloud (Docker Compose DONE)

Show how your cluster is structured (replica set, sharding, single-node, multi-node, etc.) (TO BE MADE)

Include a simple architecture diagram in the repo using draw.io or mermaid or anything of your choice. (TO BE MADE)

Processing Layer
Use Python with:

⦁	Pandas / Polars / Dask / PySpark (any one is fine) (POLARS CHOOSEN)

⦁	Proper project structure using UV (DONE)

⦁	Pydantic models for schema validation (DONE)

⦁	Mypy for type-checking (DONE)

⦁	Logging (DONE)

⦁	PyTest (at least 3 tests) (NEEDS ONE MORE)

 

Pipeline Requirements
Raw Layer

⦁	Ingest the dataset into your Big Data platform. (DONE)

⦁	Show row count and schema directly from the DB. (DONE)

Clean Layer

⦁	Handle missing values (NOT APPLICABLE)

⦁	Normalize text (NOT APPLICABLE)

⦁	Standardize dates (NOT APPLICABLE)

⦁	Remove duplicates (DONE AUTOMATICALLY VIA MERGE)

⦁	Validate schema using Pydantic (DONE)

⦁	Show this transformed data back in the DB (NEEDS IMPLEMENTATION, MINOR)

⦁	Aggregated Layer (DONE)

⦁	Build summary/aggregated datasets (Gold) (DONE, can be accessed quickly via Parquet).

⦁	Push them back into the Big Data system (NEEDS IMPLEMENTATION).

 

Visualizations (3 Total)
Use Streamlit or Matplotlib or Tableau or Power BI.

⦁	If using Tableau/Power BI, the data must come directly from your Big Data system (not flat files) (Matplotlib, DONE).

⦁	Your dashboard should use the aggregated data (DONE).

 

Video Presentation (Max 6 minutes)
Every group member must talk.

Show:

⦁	Architecture

⦁	Docker/Cloud setup

⦁	Raw ingestion

⦁	Row/column counts

⦁	Cleaning

⦁	Aggregations

⦁	Index/sharding decisions (if any)

⦁	Visualizations

⦁	What you learned and how your view of Big Data changed over the semester

No phone-recorded screens. Use screen recorder.
No faces needed if you’re not comfortable.

 

Submission
⦁	Public GitHub repo with all code

⦁	Unlisted YouTube link only

⦁	URLs must be clickable

⦁	Repo must contain

⦁		Python code

⦁		DB scripts

⦁		docker-compose (if used)

⦁		Diagram

⦁		Tableau / PowerBI Workbook (if used)

⦁		Any config files (no passwords)

Do not upload notebooks alone. This should look like a real project, not a classroom exercise.

 

Things Not To Do
⦁	Connecting Tableau/Power BI directly to flat files

⦁	Dumping the project into GitHub without structure

⦁	Sharing video via Drive

⦁	Video longer than 6-7 minutes

 

Rubric (100 percent)
⦁	Big Data System Architecture: 20

⦁	Raw Data Volume & Ingestion: 15

⦁	Cleaning & Aggregations: 15

⦁	Query Modeling & Performance: 10

⦁	Code Quality (UV project, Pydantic, Mypy, Logging, Tests): 15

⦁	Visualizations (3 views): 10

⦁	Presentation Video: 10

⦁	Wow Factor / Creativity: 5