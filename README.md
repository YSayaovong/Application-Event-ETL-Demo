# Event Processing Pipeline: Incremental Ingestion → Idempotent Load → Curated Facts → Operational Tracking

## 1. Project Background

As a data engineer building a pipeline that mirrors how real production systems are designed and operated, I built this end-to-end event processing system to answer a question most tutorial pipelines never address:

*What happens when the same pipeline runs twice? What happens when it fails halfway through? What happens when events arrive late — and how do you know any of this happened at all?*

Most beginner pipelines are built to run once cleanly and break on the second execution. This pipeline is built to be **re-run safely, recover from failure gracefully, and leave a complete operational record** of every execution — the three properties that separate a script from a production data pipeline.

The system ingests application events from a CSV source, loads them into a raw immutable layer, transforms them into a curated fact table via upserts, and writes structured metadata — row counts, run status, retry attempts, execution duration, error messages — to a dedicated tracking table on every run.

The pipeline is structured around four engineering principles:

- **Incremental extraction** using watermarks and a configurable late-arrival grace window, so each run processes only new data
- **Idempotent loading** at both the raw and curated layers, so re-runs never corrupt or duplicate data
- **Resilience** through retry logic with exponential backoff for transient failures and fail-fast behavior for data errors
- **Operational visibility** through a `pipeline_runs` table that captures the full execution state of every run — success or failure

---

## 2. System Architecture

The pipeline follows a five-stage linear flow with a clean handoff at each layer:

```
data/raw/events.csv
        │
        ▼
┌─────────────────┐
│    Extract      │  ← watermark-based, late-arrival grace window
└─────────────────┘
        │
        ▼
┌─────────────────┐
│   Raw Load      │  → raw_events (idempotent, PK deduplication)
└─────────────────┘
        │
        ▼
┌─────────────────┐
│   Transform     │  ← field normalization, type casting
└─────────────────┘
        │
        ▼
┌─────────────────┐
│  Curated Load   │  → fact_events (upserts via ON CONFLICT DO UPDATE)
└─────────────────┘
        │
        ▼
┌─────────────────┐
│  Run Tracking   │  → pipeline_runs, pipeline_watermarks
└─────────────────┘
```

| Stage | Step | Output Table | Key Behavior |
|---|---|---|---|
| 1 | Extract | — | Watermark filter + late-arrival window |
| 2 | Raw Load | `raw_events` | PK deduplication, immutable raw layer |
| 3 | Transform | — | Field normalization, type casting |
| 4 | Curated Load | `fact_events` | Upserts, safe re-execution |
| 5 | Run Tracking | `pipeline_runs`, `pipeline_watermarks` | Full execution audit trail |

The separation between raw and curated layers is deliberate: `raw_events` is never modified after insert, preserving a full audit history regardless of how many times downstream transformations are re-run. The watermark table decouples extraction state from pipeline code, enabling safe backfills by simply resetting a value rather than modifying logic.

---

## 3. Database Schema

| Table | Layer | Purpose |
|---|---|---|
| `raw_events` | Raw | Immutable ingestion layer; PK-deduplicated on load |
| `fact_events` | Curated | Query-ready fact table; updated via upserts on re-run |
| `pipeline_runs` | Operational | Execution metadata for every pipeline run |
| `pipeline_watermarks` | Operational | Per-pipeline extraction state and high-water mark |

### `pipeline_runs` — What Gets Tracked Per Execution

| Field | Type | Description |
|---|---|---|
| `run_id` | UUID | Unique identifier for each execution |
| `pipeline_name` | String | Pipeline identifier from `.env` |
| `status` | Enum | `running` → `success` or `failed` |
| `rows_extracted` | Int | Records pulled from source in this run |
| `rows_raw_loaded` | Int | Records written to `raw_events` |
| `rows_upserted` | Int | Records written or updated in `fact_events` |
| `retry_attempts` | Int | Number of retry cycles triggered |
| `duration_ms` | Int | Total execution time in milliseconds |
| `error_message` | Text | Populated on failure; null on success |
| `run_at` | Timestamp | UTC execution timestamp |

Every run writes a record regardless of outcome. A failed run with a partial load still produces a `pipeline_runs` row — with `status = failed`, the error message, and the row counts up to the point of failure. This makes debugging deterministic: the run log is the source of truth for what happened and when.

---

## 4. Key Engineering Decisions

### Incremental Extraction via Watermarks

Rather than re-reading the full source file on every run, the pipeline tracks the most recent successfully processed event timestamp in `pipeline_watermarks`. Each run extracts only events with a timestamp greater than the stored watermark, with a configurable grace window to capture late-arriving records that fall just below the cutoff.

This means pipeline cost and execution time scale with new data volume — not total dataset size — and historical records are never reprocessed unless a backfill is explicitly triggered.

### Idempotent Loading at Both Layers

| Layer | Mechanism | Effect |
|---|---|---|
| Raw | Primary key deduplication on insert | Duplicate source records are silently skipped |
| Curated | `ON CONFLICT DO UPDATE` (upsert) | Re-runs update existing facts rather than inserting duplicates |

A pipeline can be run five times against the same source file and produce identical results in both tables. This is the property that makes re-runs safe without manual intervention.

### Retry Logic with Exponential Backoff

Transient database failures — connection drops, lock timeouts, temporary unavailability — trigger automatic retries up to a configurable maximum (`MAX_RETRIES`). Each retry waits longer than the last using exponential backoff to avoid thundering-herd reconnection patterns. Errors that indicate data or logic problems (schema mismatches, constraint violations) bypass retries entirely and fail fast, preventing incorrect data from propagating through the pipeline.

### Environment-Driven Configuration

All connection strings, pipeline identifiers, grace windows, retry limits, and failure modes are managed via `.env`. No environment-specific values are hardcoded in pipeline logic, making the same codebase portable across local development, staging, and production environments without code changes.

---

## 5. Running the Pipeline

### Prerequisites

- Python 3.12+
- PostgreSQL
- Virtual environment (`.venv`)

### Install dependencies

```bash
pip install -r requirements.txt
```

### Run the pipeline

```bash
python -m src.etl
```

The pipeline is safe to run multiple times. Each execution will process only new events since the last successful run and write a complete record to `pipeline_runs`.

### Backfill historical data

To reprocess all historical events (resets the watermark to the beginning of time):

```bash
python -m src.backfill
python -m src.etl
```

---

## 6. Configuration Reference

All pipeline behavior is controlled through `.env`:

```env
DATABASE_URL=postgresql://user:password@localhost:5432/event_processing
PIPELINE_NAME=event-processing-pipeline
SOURCE_NAME=application_events
RAW_DATA_PATH=data/raw/events.csv
LATE_ARRIVAL_GRACE_MINUTES=60
FAIL_MODE=none
MAX_RETRIES=4
```

| Variable | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string | — |
| `PIPELINE_NAME` | Identifier written to `pipeline_runs` | — |
| `SOURCE_NAME` | Source label for tracking | — |
| `RAW_DATA_PATH` | Path to the input CSV | `data/raw/events.csv` |
| `LATE_ARRIVAL_GRACE_MINUTES` | Grace window added to watermark cutoff | `60` |
| `FAIL_MODE` | Override for testing failure paths | `none` |
| `MAX_RETRIES` | Maximum retry attempts on transient failure | `4` |

---

## 7. Current Status

This pipeline is complete and production-correct for its current scope:

| Capability | Status |
|---|---|
| Incremental extraction with watermarks | ✅ Complete |
| Late-arrival grace window | ✅ Complete |
| Idempotent raw layer (PK dedup) | ✅ Complete |
| Idempotent curated layer (upserts) | ✅ Complete |
| Retry logic with exponential backoff | ✅ Complete |
| Operational run tracking | ✅ Complete |
| Backfill support | ✅ Complete |
| Environment-driven configuration | ✅ Complete |

---

## 8. Planned Enhancements

### Phase 3 — Observability

| Enhancement | Value |
|---|---|
| Structured JSON logging | Machine-readable logs compatible with log aggregation platforms (Datadog, CloudWatch) |
| Step-level execution timing | Identify which pipeline stage is slowest across runs |
| Data quality validation checks | Flag nulls, type mismatches, and out-of-range values before curated load |
| Dead-letter / quarantine table | Isolate invalid records for investigation without halting the pipeline |

### Phase 4 — Deployment & DevOps

| Enhancement | Value |
|---|---|
| Dockerized pipeline | Reproducible execution environment with a single `docker-compose up` |
| Health checks | Endpoint or exit code confirming pipeline readiness before orchestration |
| CI pipeline | Automated linting, unit tests, and pipeline execution on every push |
| Scheduled execution | Cron or task scheduler integration for unattended production runs |

### Phase 5 — Platform Integration

| Enhancement | Value |
|---|---|
| Airflow or Dagster orchestration | DAG-based dependency management, retries, and alerting |
| Metrics export | Prometheus / Grafana integration for run duration and row count dashboards |
| Infrastructure-as-code | Terraform definitions for database provisioning and environment setup |
| Secrets management | Vault or cloud-native secrets manager replacing `.env` in production |

---

## 9. Summary

This pipeline implements the foundational properties of a production data system: incremental extraction that scales with new data volume, idempotent loading that makes re-runs safe at both the raw and curated layers, resilience through retry logic that distinguishes transient failures from data errors, and operational tracking that leaves a complete audit trail of every execution. The architecture separates concerns cleanly — raw storage, curated facts, and operational metadata each live in dedicated tables — reflecting the design patterns used in real data engineering teams building on PostgreSQL, Airflow, and dbt.

---

## Tools Used

- Python 3.12 — pipeline orchestration, extraction, transformation, retry logic
- PostgreSQL — raw layer, curated fact table, watermark and run tracking
- psycopg2 — Python-to-PostgreSQL client
- python-dotenv — environment-driven configuration management
- Git / GitHub — version control and pipeline auditability
