from datetime import datetime, timedelta, timezone
from .db import fetch_one
from .settings import settings
from .validation import read_events

def effective_watermark():
    row=fetch_one("SELECT watermark_ts FROM pipeline_watermarks WHERE pipeline_name=%s AND source_name=%s",(settings.PIPELINE_NAME,settings.SOURCE_NAME))
    base=row['watermark_ts'] if row else datetime(2000,1,1,tzinfo=timezone.utc)
    return base-timedelta(minutes=settings.LATE_ARRIVAL_GRACE_MINUTES)

def extract():
    rows=read_events(settings.RAW_DATA_PATH)
    if any(r['source']!=settings.SOURCE_NAME for r in rows):
        raise ValueError('CSV source does not match SOURCE_NAME; use one source per run')
    watermark=effective_watermark()
    return [r for r in rows if r['event_ts']>=watermark]
