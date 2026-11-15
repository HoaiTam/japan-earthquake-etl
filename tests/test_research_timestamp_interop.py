"""Synthetic INT96 interop fixtures; never mutate or bless real export bytes."""
import importlib.util
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys
import tempfile
import unittest
import json
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'notebooks'))
import research_collection as c


class InteropTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.p=Path(self.temp.name)/'file.parquet'
        self.time=datetime(2026,11,15,tzinfo=timezone.utc)
        self.m={'code_version':c.INT96_WRITER,'dataset_config_version':'dat03-prepared-ml-v2','created_at_utc':'2026-11-15T00:00:00Z',
                'filter_config_json':json.dumps({'source_coverage_contract':c.PREPARED_COVERAGE,'resource_profile_version':'dat03-whole-window-90k-v1'})}
    def write(self,*,int96=True,submicro=False,offset=False):
        event=pa.array([int(self.time.timestamp())*1000000000+(1 if submicro else 0)],pa.timestamp('ns'))
        created=pa.array([self.time+(timedelta(hours=7) if offset else timedelta())],pa.timestamp('us',tz='UTC'))
        pq.write_table(pa.table({'candidate_time_utc':event,'created_at_utc':created}),self.p,use_deprecated_int96_timestamps=int96,store_schema=False)
        return pq.ParquetFile(self.p)
    def test_known_writer_int96_is_a_lossless_utc_view_without_rewriting(self):
        p=self.write();before=self.p.read_bytes();table,changed=c._timestamp_view(p,self.m)
        self.assertTrue(changed);self.assertEqual(pa.timestamp('us',tz='UTC'),table['candidate_time_utc'].type)
        self.assertEqual(self.time,table['candidate_time_utc'][0].as_py());self.assertEqual(before,self.p.read_bytes())
    def test_unknown_writer_or_profile_never_gets_timezone_assumed(self):
        p=self.write()
        for m in ({**self.m,'code_version':'artifact_'+'0'*64},{**self.m,'filter_config_json':'{}'}):
            with self.assertRaisesRegex(ValueError,'WRITER_PROOF'):c._timestamp_view(p,m)
    def test_no_precision_loss_or_timezone_offset_is_accepted(self):
        with self.assertRaisesRegex(ValueError,'SUBMICROSECOND'):c._timestamp_view(self.write(submicro=True),self.m)
        with self.assertRaisesRegex(ValueError,'UTC_ANCHOR_CHANGED'):c._timestamp_view(self.write(offset=True),self.m)
    def test_naive_non_int96_cannot_use_the_exception(self):
        with self.assertRaisesRegex(ValueError,'PHYSICAL_TYPE'):c._timestamp_view(self.write(int96=False),self.m)


if __name__=='__main__':unittest.main()
