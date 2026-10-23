"""Strict offline CSV validation before database writes."""
import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path

def read_events(path):
    result, seen = [], set()
    with Path(path).open(newline='', encoding='utf-8-sig') as file:
        reader=csv.DictReader(file)
        required={'event_id','event_ts','source','event_type'}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError('Required CSV headers are missing')
        for line,row in enumerate(reader,2):
            if None in row or any(row.get(k) is None or not row[k].strip() for k in required):
                raise ValueError(f'CSV line {line}: extra fields or missing required value')
            if row['event_id'] in seen:
                raise ValueError(f'CSV line {line}: duplicate event_id')
            seen.add(row['event_id'])
            try:
                ts=datetime.fromisoformat(row['event_ts'].replace('Z','+00:00'))
                if ts.utcoffset() is None:raise ValueError('timezone required')
                payload=json.loads(row['payload_json']) if row.get('payload_json') else {}
                if not isinstance(payload,dict):raise ValueError('payload must be an object')
                user=row.get('user_id') or payload.get('user_id')
                amount=row.get('amount') or payload.get('amount')
                if amount is not None:
                    amount=str(amount)
                    if not Decimal(amount).is_finite():raise ValueError('nonfinite amount')
            except (ValueError,TypeError,InvalidOperation) as exc:
                raise ValueError(f'CSV line {line}: invalid timestamp, payload or amount') from exc
            result.append(dict(event_id=row['event_id'],event_ts=ts,source=row['source'],event_type=row['event_type'],user_id=user,amount=amount,payload_json=row.get('payload_json') or None))
    return result

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('path',nargs='?',default='data/raw/events.csv')
    print(f'Validated {len(read_events(p.parse_args().path))} events without a database connection.')
