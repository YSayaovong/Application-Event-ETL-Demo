# Incremental Application Event ETL Demonstration
## Scope
Python/PostgreSQL project processing CSV application events. The repository's historical name does not describe its current inputs: no regulatory-document parsing is implemented in the inspected modules.

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
Install requirements, configure settings/database credentials locally, create the PostgreSQL schema using the SQL file, then run the module from the repository root:
```bash
python -m src.etl
```
Inspect settings and backfill arguments before use. Do not commit credentials.

## Limitations
Separate loading, watermark and tracking calls do not constitute one end-to-end transaction. Crash recovery, late arrivals, reruns and row-count semantics need automated verification. Failure modes in the script are demonstration hooks, not a comprehensive test suite. No production-readiness or regulatory-compliance claim is made. Committed bytecode under `src/__pycache__/` is generated material and should be excluded in a later cleanup.
