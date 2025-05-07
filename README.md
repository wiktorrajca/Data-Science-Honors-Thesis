# Procurement Pipeline

This repository contains a full end-to-end data pipeline for processing public procurement data, matching entities, building and enriching a graph of procurement winners and their corporate networks, screening for risk, computing a proprietary “shadiness” metric, and extracting urgency and other analytical metrics.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Screening Service Setup](#screening-service-setup)
4. [Installation](#installation)
5. [Directory Structure](#directory-structure)
6. [Pipeline Components](#pipeline-components)
7. [Running the Pipeline](#running-the-pipeline)
8. [Command-Line Arguments](#command-line-arguments)
9. [Outputs](#outputs)
10. [Post-Pipeline Analysis](#post-pipeline-analysis)
11. [Examples](#examples)
12. [Troubleshooting](#troubleshooting)

## Overview

## Screening Service Setup

This project uses [OpenSanctions Yente](https://opensanctions.org/) as the backend screening API. Below are instructions to deploy Yente and its Elasticsearch backend via Docker, giving you a local screening service exposing `/search`, `/match`, `/update`, and related endpoints. You will then point the pipeline’s screening step (`screening.py`) at this service.

### Table of Contents

1. [Prerequisites](#prerequisites)
2. [Directory Layout](#directory-layout)
3. [Obtain Yente Manifests](#obtain-yente-manifests)
4. [Create docker-compose.yml](#create-docker-composeyml)
5. [Configuration and Environment Variables](#configuration-and-environment-variables)
6. [Launching the Stack](#launching-the-stack)
7. [Health Checks and Testing](#health-checks-and-testing)
8. [Customizing the Dataset Manifest](#customizing-the-dataset-manifest)
9. [Forcing a Reindex](#forcing-a-reindex)
10. [Troubleshooting](#troubleshooting-yente)
11. [Useful Endpoints](#useful-endpoints)
12. [Stopping and Cleanup](#stopping-and-cleanup)

---

### Prerequisites

* Docker (Desktop or Engine) installed and running.
* Docker Compose (v2+) available as `docker compose` or `docker-compose`.
* Network access to `data.opensanctions.org` for manifest downloads.

### Directory Layout

Choose or create a working directory, e.g.:

```bash
mkdir -p ~/yente-docker
cd ~/yente-docker
```

Your directory will contain:

```
.
├── docker-compose.yml
└── manifests/
    └── default.yml    # your custom copy of the manifest
```

### Obtain Yente Manifests

Yente uses a manifest YAML to know which datasets to index and expose. Copy the official default manifest:

```bash
mkdir -p manifests
curl -L \
  https://raw.githubusercontent.com/opensanctions/yente/main/manifests/default.yml \
  -o manifests/default.yml
```

Edit `manifests/default.yml` to restrict or add datasets as needed.

### Create docker-compose.yml

Create `docker-compose.yml` in your project root with this content:

```yaml
version: "3.9"

services:

  index:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.14.3
    container_name: yente-index
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - bootstrap.memory_lock=true
      - "ES_JAVA_OPTS=-Xms2g -Xmx2g"
    ulimits:
      memlock:
        soft: -1
        hard: -1
    ports:
      - "127.0.0.1:9200:9200"

  yente:
    image: ghcr.io/opensanctions/yente:4.3.0
    container_name: yente-app
    depends_on:
      - index
    environment:
      - YENTE_MANIFEST=/manifests/default.yml
      - YENTE_AUTO_REINDEX=true
      - YENTE_INDEX_TYPE=elasticsearch
      - YENTE_INDEX_URL=http://index:9200
      - YENTE_INDEX_NAME=yente
      - YENTE_UPDATE_TOKEN=changeme123
      - YENTE_MAX_BATCH=100
      - YENTE_QUERY_CONCURRENCY=10
    volumes:
      - ./manifests:/manifests:ro
    ports:
      - "127.0.0.1:8000:8000"
```

### Configuration and Environment Variables

* `YENTE_MANIFEST`: Path inside the container to your manifest YAML.
* `YENTE_AUTO_REINDEX`: `true` to index on startup; `false` to require manual POST `/update`.
* `YENTE_UPDATE_TOKEN`: Shared secret to secure the `/update` API.
* `YENTE_INDEX_TYPE`, `YENTE_INDEX_URL`, `YENTE_INDEX_NAME`: Configure Elasticsearch connection.
* `YENTE_MAX_BATCH`, `YENTE_QUERY_CONCURRENCY`: Tweak batching size and concurrency.

### Launching the Stack

```bash
docker compose pull
docker compose up -d
```

It may take several minutes for the initial indexing.

### Health Checks and Testing

1. **Elasticsearch**:

   ```bash
   curl -s http://localhost:9200/_cluster/health?pretty
   ```
2. **Yente liveness**:

   ```bash
   curl http://localhost:8000/health
   ```
3. **Test search**:

   ```bash
   curl 'http://localhost:8000/search/default?q=John+Doe'
   ```
4. **Test match**:

   ```bash
   curl -X POST http://localhost:8000/match/default \
     -H "Content-Type: application/json" \
     -d '{"queries":{"Q1":{ "schema":"Person","properties":{"name":["Jane Doe"]}}}}'
   ```

### Customizing the Dataset Manifest

Edit `manifests/default.yml`, e.g., to restrict to sanctions and PEPs:

```yaml
datasets:
  - name: default
  - name: peps
  - name: sanctions
```

### Forcing a Reindex

```bash
curl -X POST http://localhost:8000/update \
  -H "Authorization: Token changeme123"
```

### Troubleshooting

* **Index `yente-entities` does not exist**: Wait for indexing to complete.
* **Memory errors**: Adjust `ES_JAVA_OPTS`.
* **404 on `/match/<scope>`**: Ensure your manifest includes that scope.
* **Connection issues**:

  ```bash
  ```

docker exec yente-app curl -s [http://index:9200](http://index:9200)

````

### Useful Endpoints

- `GET  /search/{scope}` — full-text search  
- `POST /match/{scope}` — entity matching  
- `POST /update` — manual reindex (token required)  
- `GET  /datasets` — list scopes  
- `GET  /health` — service health

### Stopping and Cleanup

```bash
docker compose down    # stop
docker compose down -v # stop + remove volumes
````

---

## Overview

This pipeline:

1. **Entity Matching**: Fuzzy‑merges a base CSV of procurement winners against one or more additional CSV tables with corporate identifiers (via `matcher.py`).
2. **Cleaning & Filtering**: Filters merged results by country and consistency checks (via `cleaning_matcher.py`).
3. **Graph Construction**: Builds an initial directed graph of procurements and winners, then expands it with ownership relationships (via `graph_utilis.py`).
4. **Screening**: Queries a risk API (Yente) in parallel to attach screening scores to entities (via `screening.py`).
5. **Shadiness Metrics**: Calculates candidate‑level and procurement‑level “shadiness” based on screening and ownership weights (via `shadiness_max_only.py`).
6. **Additional Metrics**: Computes and verifies auxiliary metrics such as urgency, expected shadiness, and distribution checks (via `metrics.py`).

All steps are orchestrated by `main.py`.

---

## Prerequisites

* **Python 3.8+**
* **Unix/macOS** or compatible environment
* **Libraries**:

  * pandas
  * numpy
  * networkx
  * rapidfuzz
  * tqdm
  * requests
  * scipy
  * regex (the `regex` PyPI package)

Install via:

```bash
pip install pandas numpy networkx rapidfuzz tqdm requests scipy regex
```

---

## Installation

1. Clone the repository:

   ```bash
   ```

git clone [https://github.com/your-org/procurement-pipeline.git](https://github.com/your-org/procurement-pipeline.git)
cd procurement-pipeline

````
2. Ensure dependencies are installed (see Prerequisites).
3. Make the main script executable:
```bash
chmod +x main.py
````

---

## Directory Structure

```text
./
├── matcher.py                  # Fuzzy matching logic
├── cleaning_matcher.py         # Load & filter merged CSVs
├── graph_utilis.py             # Graph construction & expansion
├── screening.py                # Parallel screening with Yente API
├── shadiness_max_only.py       # Shadiness calculations
├── metrics.py                  # Urgency & metric computations
├── main.py                     # Pipeline orchestrator
└── README.md                   # This documentation
```

---

## Pipeline Components

### 1. `matcher.py`

* **Function**: `merge_tables_on_processed_names(base_csv, additional_csv, ...)`
* **Description**: Performs exact and fuzzy joins to link procurement winners to corporate IDs.
* **Outputs**: Per‑file `matched_<identifier>.csv` in `--matched-dir`.

### 2. `cleaning_matcher.py`

* **Function**: `load_and_filter_csvs(matched_dir, country)`
* **Description**: Reads all matched CSVs, filters by ISO and WIN country codes, returns list of DataFrames.

### 3. `graph_utilis.py`

* **Functions**:

  * `load_or_initialize_graph(country)`
  * `add_procurement_winners(G, country, df)`
  * `load_and_match_all(G, shareholders_folder, subsidiaries_folder, basic_shareholders_folder)`
  * `clean_node_attributes(G)`
  * `attach_screening_results(G, df_screen, node_id_col)`
  * `save_graph(G, path)`
* **Description**: Builds a NetworkX `DiGraph` of procurements, adds edges to winners, and expands to include ownership tiers.

### 4. `screening.py`

* **Function**: `screen_graph_multi_threads(G, yente_base, ...)`
* **Description**: Sends batched POST requests to Yente’s `/match/<dataset>` endpoint; writes incremental checkpoint CSV.

### 5. `shadiness_max_only.py`

* **Function**: `run_shadiness_pipeline(G, ...)`
* **Description**: Calculates an entity’s “shadiness” by picking the single highest‑risk node among a winner and its owners, then aggregates to procurement’s `expected_shadiness`.

### 6. `metrics.py`

* **Functions**:

  * `compute_shadiness(G)`
  * `compute_expected_shadiness(G)`
  * `compute_urgency_from_winner_values(G, ...)`
  * Verification helpers: `verify_*` methods return booleans or ranges.
* **Description**: Computes procurement‑level urgency (z‑score + CDF + weighted by expected shadiness) and verifies ranges.

---

## Running the Pipeline

Invoke `main.py` with the required arguments:

```bash
python main.py \
  --base-file <path/to/ted_all.csv> \
  --additional-dir <path/to/BvD_ID_and_Name> \
  --matched-dir <path/to/matched_results> \
  --country PL \
  --shareholders-folder <path/to/shareholders_csvs> \
  --subsidiaries-folder <path/to/subsidiaries_csvs> \
  --basic-shareholders-folder <path/to/basic_shareholders_csvs> \
  --output-dir <path/to/outputs> \
  --checkpoint screening.csv \
  --yente-base http://localhost:8000
```

This will generate:

* **Matched CSVs**: `matched_<id>.csv` in `--matched-dir`
* **Merged DF**: `merged_filtered.csv` in `--output-dir`
* **Graph snapshots**:

  * `graph_initial.graphml`
  * `graph_expanded.graphml`
  * `graph_screened.graphml`
  * `graph_final.graphml`
* **Screening checkpoint**: `screening.csv`
* **Metrics report**: `metrics_report.txt`

---

## Command‑Line Arguments

| Argument                      | Description                                             | Required | Default                 |
| ----------------------------- | ------------------------------------------------------- | -------- | ----------------------- |
| `--base-file`                 | Path to base CSV of procurement winners                 | Yes      |                         |
| `--additional-dir`            | Folder containing additional CSVs to match              | Yes      |                         |
| `--matched-dir`               | Folder where matched results are saved                  | Yes      |                         |
| `--similarity-threshold`      | Fuzzy similarity cutoff (0–1)                           | No       | `0.8`                   |
| `--country`                   | 2‑letter ISO country code                               | Yes      |                         |
| `--shareholders-folder`       | Folder of first‑level shareholder CSVs                  | Yes      |                         |
| `--subsidiaries-folder`       | Folder of subsidiary CSVs                               | Yes      |                         |
| `--basic-shareholders-folder` | Folder of additional shareholder info CSVs              | Yes      |                         |
| `--output-dir`                | Folder for all pipeline outputs                         | Yes      |                         |
| `--checkpoint`                | Filename for screening checkpoint within `--output-dir` | No       | `screening.csv`         |
| `--yente-base`                | Base URL for screening API service                      | No       | `http://localhost:8000` |

---

## Outputs

1. **Matched Results**: one CSV per additional file.
2. **Merged Filtered Data**: `merged_filtered.csv`.
3. **GraphML Snapshots**:

   * `graph_initial.graphml`
   * `graph_expanded.graphml`
   * `graph_screened.graphml`
   * `graph_final.graphml`
4. **Screening Checkpoint**: incremental CSV of screening scores.
5. **Metrics Report**: simple text file summarizing verification checks.

---

## Examples

Match a French procurement dataset:

```bash
python main.py \
  --base-file data/ted_france.csv \
  --additional-dir data/BvD/fr_matches \
  --matched-dir data/output/matched_fr \
  --country FR \
  --shareholders-folder data/relationships/shareholders_fr \
  --subsidiaries-folder data/relationships/subsidiaries_fr \
  --basic-shareholders-folder data/relationships/basic_fr \
  --output-dir data/output/fr_pipeline \
  --checkpoint screening_fr.csv \
  --yente-base https://yente.myorg.com
```

---

---

## Post-Pipeline Analysis

Once you have generated the final graph (`graph_final.graphml`), you can run two additional tools for basic analysis:

### 1. `compare_graphs.py`

**Purpose**: Compare two final graphs (e.g. different countries or scenarios) and produce CSV summaries and an optional CDF plot of urgency scores.

**Key Outputs**:

* **`stats.csv`** — basic statistics (count, mean/median/std of urgency and expected shadiness, average values).
* **`percentiles.csv`** — for each specified percentile (e.g. 25th, 50th, 75th, 100th), the winner name, urgency, and value at or above that threshold for each graph.
* **`top3.csv`** — top 3 procurements by urgency for each graph, with winner name, urgency, expected shadiness, value, and BvD ID.
* **Optional CDF Plot** — if you pass `--plot-cdf-path`, you'll save a PNG comparing the linear urgency CDFs of the two graphs.

**Usage**:

```bash
python compare_graphs.py \
  --graph1 path/to/graph_final_A.graphml \
  --graph2 path/to/graph_final_B.graphml \
  --name1 CountryA --name2 CountryB \
  --percentiles 25 50 75 100 \
  --output-dir analysis_results \
  [--plot-cdf-path analysis_results/urgency_cdf.png]
```

### 2. `diagnostic_report.py`

**Purpose**: Generate a comprehensive diagnostic report for a single final graph, including:

* Histograms of urgency buckets (nonlinear & linear)
* Boxplots by candidate bin
* CDF tables & plots
* Top 10 procurements by urgency (nonlinear & linear)
* Summary statistics (means/medians)

**Key Outputs** (all saved in the specified output folder):

* `urgency_buckets.csv`, `urgency_linear_buckets.csv`
* `boxplots_by_candidate_bin.png`
* `urgency_cdf_nonlinear_table.csv`, `urgency_cdf_linear_table.csv`
* `urgency_cdf_nonlinear.png`, `urgency_cdf_linear.png`
* `top10_procurements_by_urgency_nonlinear.csv`, `top10_procurements_by_urgency_linear.csv`
* `summary_statistics.csv`

**Usage**:

```bash
python diagnostic_report.py \
  --graph path/to/graph_final.graphml \
  --output-dir diagnostics_folder
```

---

## Troubleshooting

* **Missing arguments**: ensure you use the hyphenated flags exactly as defined.
* **Yente errors**: check `--yente-base` URL, network connectivity, and API health.
* **Empty outputs**: verify that your base CSV and additional CSVs share comparable name formats.
* **Library mismatches**: use Python 3.8+ and the specified pip dependencies.

---

*For questions or support, please contact the dev team or open an issue on the repository.*
