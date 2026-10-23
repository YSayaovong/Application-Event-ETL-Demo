# Application Event ETL Demo
## Scope
Python/PostgreSQL demonstration processing CSV application events with incremental extraction, transformations, raw and curated tables, and run tracking.

## Included implementation
- `data/raw/events.csv`: small example input.
- `sql/001_create_tables.sql`: database setup.
- `src/settings.py`, `src/db.py`: configuration and database helpers.
- `src/extract.py`, `src/transform.py`, `src/load.py`: extraction, field transformation, raw insertion and curated upsert.
- `src/etl.py`: run tracking, retry orchestration and failure simulation.
- `src/backfill.py`: backfill entry point.
- `requirements.txt`: Python dependencies.

## Behavior
Raw insert uses event_id conflict handling. Curated upsert updates user_id and amount on an existing event; it does not update every event attribute. Pipeline execution records counts/status and advances a timestamp watermark after processing. OperationalError triggers bounded exponential-backoff retries.

## Running
Use a disposable local PostgreSQL database. Install `requirements.txt`, create the schema with `sql/001_create_tables.sql`, and set these required environment variables (or use an untracked local `.env`):

- `DATABASE_URL`: PostgreSQL connection URL.
- `PIPELINE_NAME`: run-tracking identifier.
- `SOURCE_NAME`: source/watermark identifier.
- `RAW_DATA_PATH`: `data/raw/events.csv`.

Optional settings are `LATE_ARRIVAL_GRACE_MINUTES` (60), `FAIL_MODE` (`none`), and `MAX_RETRIES` (4). Then run from the repository root:
```bash
python -m src.etl
```
Inspect settings and backfill arguments before use. Do not commit credentials.

## Limitations
Separate loading, watermark and tracking calls do not constitute one end-to-end transaction. Crash recovery, late arrivals, reruns and row-count semantics need automated verification. Failure modes in the script are demonstration hooks, not a comprehensive test suite. No production-readiness or regulatory-compliance claim is made. Committed bytecode under `src/__pycache__/` is generated material and should be excluded in a later cleanup.
