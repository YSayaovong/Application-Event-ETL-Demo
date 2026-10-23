import tempfile
from pathlib import Path
import unittest
from src.validation import read_events
from src.transform import transform

class ValidationTests(unittest.TestCase):
    def check_csv(self,text):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'events.csv';p.write_text(text)
            return read_events(p)

    def test_included_events(self):
        rows=read_events('data/raw/events.csv')
        self.assertEqual(len(rows),3)
        self.assertEqual([r['amount'] for r in rows],['10','25','50'])
        self.assertTrue(all(r['payload_json'] for r in transform(rows)))

    def test_malformed_row_rejected(self):
        with self.assertRaises(ValueError):
            self.check_csv('event_id,event_ts,source,event_type\ne1,2025-01-01T00:00:00Z,app,click,extra\n')

    def test_timezone_required(self):
        with self.assertRaises(ValueError):
            self.check_csv('event_id,event_ts,source,event_type\ne1,2025-01-01,app,click\n')

    def test_duplicate_id_rejected(self):
        with self.assertRaises(ValueError):
            self.check_csv('event_id,event_ts,source,event_type\ne1,2025-01-01T00:00:00Z,app,click\ne1,2025-01-01T00:00:00Z,app,click\n')

    def test_nonfinite_amount_rejected(self):
        with self.assertRaises(ValueError):
            self.check_csv('event_id,event_ts,source,event_type,amount\ne1,2025-01-01T00:00:00Z,app,click,NaN\n')

if __name__=='__main__':unittest.main()
