"""Synthetic security artifacts only; never a cloud/scientific certification."""
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT / "scripts"))
import security_handoff as h
spec = importlib.util.spec_from_file_location("sec_audit_handoff_test",ROOT / "scripts/audit-security.py")
a = importlib.util.module_from_spec(spec); spec.loader.exec_module(a)


def raw(value):
    return json.dumps(value,sort_keys=True).encode()


def bundle(pilot=False, extra=None):
    files = {"memberships.parquet":b"PAR1 synthetic", "sequence_summary.parquet":b"PAR1 synthetic",
             "experiment_metrics.json":b"{}", "experiment_config.json":b"{}", "requirements-lock.txt":b"numpy==2.0.0\n"}
    files.update(extra or {})
    marker = {"status":"DEVELOPMENT_RUN_COMPLETED" if pilot else "COMPLETED",
              "artifact_kind":"PILOT_EXPERIMENT_RESULT" if pilot else "OFFICIAL",
              "checksums":{k:hashlib.sha256(v).hexdigest() for k,v in files.items()}}
    files["pilot_result_manifest.json" if pilot else "_SUCCESS.json"] = raw(marker)
    return files


class HandoffTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve()
        pilot={"pilot_manifest.json":raw({"artifact_kind":"DEVELOPMENT_PILOT","research_ready":False}),
               "coverage_report.json":b"{}","mainshocks.parquet":b"PAR1 synthetic","features-part-00000.parquet":b"PAR1 synthetic"}
        pilot["checksums.sha256"] = self.checks(pilot)
        self.zip = self.root / "pilot.zip"
        with zipfile.ZipFile(self.zip,"w") as z:
            for name,data in pilot.items(): z.writestr(name,data)
        notebook = self.root / "notebook.ipynb"; notebook.write_bytes(raw({"nbformat":4,"cells":[{"cell_type":"code","source":["print(1)"]}]}))
        dataset={"dataset_manifest.json":raw({"dataset_status":"EXPORTED"}),"features-part-00000.parquet":b"PAR1 synthetic"}
        dataset["export-checksums.sha256"]=self.checks({"features-part-00000.parquet":dataset["features-part-00000.parquet"]})
        dataset["checksums.sha256"]=self.checks(dataset)
        self.save("dataset",dataset); self.save("result",bundle(pilot=True)); self.save("import",bundle())
        self.items=[{"task_id":task,"kind":kind,"path":name,"sha256":h.files_for(self.root/name,kind)[1]}
                    for task,kind,name in (("DAT-02","pilot_zip","pilot.zip"),("EXP-01","notebook","notebook.ipynb"),
                       ("EXP-01","result_bundle","result"),("MLD-05","dataset_bundle","dataset"),("MLI-03","result_bundle","import"))]

    def checks(self,files):
        return "".join(hashlib.sha256(v).hexdigest()+"  "+k+"\n" for k,v in sorted(files.items())).encode()

    def save(self,name,files):
        path=self.root/name; path.mkdir(exist_ok=True)
        for key,value in files.items(): (path/key).write_bytes(value)

    def collect(self,items=None,secrets=()):
        path=self.root/"handoff.json";path.write_bytes(raw({"version":h.VERSION,"artifacts":self.items if items is None else items}))
        with patch.object(h,"decoded_text",side_effect=lambda n,d:"" if n.endswith(".parquet") else d.decode()):
            return h.audit(self.root,path,a.scan_text,secrets)

    def repin(self,index):
        item=self.items[index];item["sha256"]=h.files_for(self.root/item["path"],item["kind"])[1]

    def test_all_required_actual_bytes_and_pins(self):
        result=self.collect();self.assertTrue(result["passed"]);self.assertEqual(5,len(result["artifacts"]))

    def test_missing_manifest_and_role_cannot_pass(self):
        self.assertFalse(h.audit(self.root,None,a.scan_text,())["passed"])
        self.assertFalse(self.collect(self.items[:-1])["passed"])

    def test_changed_pin_or_bytes_rejected(self):
        self.items[0]["sha256"]="a"*64
        self.assertFalse(self.collect()["passed"])

    def test_marker_and_checksums_cannot_be_forged(self):
        (self.root/"import/experiment_config.json").write_bytes(b'{"changed":true}')
        self.repin(4);self.assertFalse(self.collect()["passed"])

    def test_import_requires_official_marker_not_renamed_pilot(self):
        (self.root/"import/_SUCCESS.json").unlink();self.save("import",bundle(True));self.repin(4)
        self.assertFalse(self.collect()["passed"])

    def test_text_leaks_redacted_to_categories_even_in_filename(self):
        secret="ghp_"+"x"*40
        self.save("result",bundle(True,{secret+".txt":secret.encode()}));self.repin(2)
        result=self.collect(secrets=[secret]);self.assertFalse(result["passed"])
        self.assertNotIn(secret,json.dumps(result));self.assertTrue(result["findings"])

    def test_decoded_json_escape_and_nested_values_scanned(self):
        data=b'{"nested":["/\\u0055sers/person",{"x":"private-value"}]}'
        text=h.decoded_text("config.json",data)
        self.assertIn("personal_account_path",a.scan_text(text,ml=True));self.assertIn("private-value",text)

    def test_unsafe_names_symlinks_and_zip_duplicates(self):
        for value in ("../outside","/outside","a//b","a/./b","a\\b","C:private"):
            with self.assertRaises(ValueError):h.safe_name(value)
        link=self.root/"link";link.symlink_to(self.root/"import",target_is_directory=True)
        with self.assertRaises(ValueError):h.safe_path(self.root,"link")
        with zipfile.ZipFile(self.zip,"w") as z:
            z.writestr("../outside",b"x")
        self.items[0]["sha256"]=hashlib.sha256(self.zip.read_bytes()).hexdigest()
        self.assertFalse(self.collect()["passed"])

    def test_zip_symlink_and_budget_rejected(self):
        entry=zipfile.ZipInfo("link");entry.external_attr=0o120777<<16
        with zipfile.ZipFile(self.zip,"w") as z:z.writestr(entry,b"target")
        with self.assertRaises(ValueError):h.files_for(self.zip,"pilot_zip")
        with patch.object(h,"FILE_LIMIT",1),self.assertRaises(ValueError):h.files_for(self.root/"notebook.ipynb","notebook")

    def test_json_duplicate_keys_and_unreadable_parquet_fail_closed(self):
        with self.assertRaises(ValueError):h.strict_json(b'{"a":1,"a":2}')
        with self.assertRaises(Exception):h.decoded_text("rows.parquet",b"not parquet")

    def test_parquet_compressed_values_footer_and_windows_paths(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            self.skipTest("optional pyarrow absent; production audit remains pending")
        out=io.BytesIO(); secret="private-pipeline-identifier"
        schema=pa.schema([("value",pa.string())],metadata={b"note":secret.encode()})
        table=pa.Table.from_pylist([{"value":"C:"+"\\Users\\person\\private"}],schema=schema)
        pq.write_table(table,out,compression="gzip")
        text=h.decoded_text("rows.parquet",out.getvalue())
        self.assertIn(secret,text);self.assertIn("personal_account_path",a.scan_text(text,ml=True))


if __name__ == "__main__":unittest.main()
