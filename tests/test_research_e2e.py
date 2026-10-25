"""Offline DAT-03 runtime metadata guards. Synthetic records are NOT source/Gold evidence."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("research_e2e", ROOT / "scripts/research-e2e.py")
e2e = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(e2e)


class ResearchE2ETest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.ns = "iceberg.dat03_e2e_" + "a" * 32

    def record(self):
        context = {"dag_id": "dat03_bounded_e2e", "run_id": "fixture", "contract_version": "orc-07-v1", "mode": "real",
                   "resolver_request": {"namespace": self.ns}}
        op = "orc07-" + hashlib.sha256(b"dat03_bounded_e2e|fixture").hexdigest()[:40]
        context["operation_id"] = op; context["context_sha256"] = e2e.digest(context)
        commit = {"namespace": self.ns, "gold_run_id": op, "status": "COMMITTED", "published": False, "identity_sha256": "b" * 64,
                  "tables": {t: {"snapshot_id": n + 1, "count": 1, "table": self.ns + "." + t} for n, t in enumerate(sorted(e2e.TABLES))}}
        checks = {}
        for table in e2e.TABLES:
            for kind in ("count", "identity"):
                checks[kind + ":" + table] = {"sql": "synthetic test metadata", "actual": 1, "expected": 1}
            checks["count:" + table]["sql"] = "SELECT count(*) FROM " + self.ns + "." + table + " FOR VERSION AS OF " + str(commit["tables"][table]["snapshot_id"])
        blockers = {"required_fields", "bridge_lineage", "source_counts", "primary_provenance", "utc_jst", "dimensions", "aggregate_count",
                    "changed_run_scope", "outside:event_current", "outside:event_source_bridge"} | {"unique:" + t for t in e2e.TABLES}
        for b in blockers:
            checks[b] = {"sql": "synthetic test metadata", "actual": 0, "expected": 0}
        checksum = hashlib.sha256(json.dumps(commit, separators=(",", ":")).encode()).hexdigest()
        pub = {"namespace": self.ns, "gold_run_id": op, "identity_sha256": "b" * 64, "engine": "TRINO", "checks": checks,
               "bundle_sha256": checksum, "publication_id": "pub_" + checksum}
        row = {"context": context, "operation_id": op, "commit": commit, "publication": pub, "silver": {"reconciliation": {"balanced": True}}, "receipts": []}
        folder = self.root / op; folder.mkdir()
        previous = None
        for phase in e2e.PHASES:
            artifact = row["silver"] if phase == "silver" else commit if phase == "gold" else pub if phase == "publish" else {"phase": phase}
            (folder / (phase + ".json")).write_text(json.dumps(artifact))
            receipt = {"phase": phase, "operation_id": op, "context_sha256": context["context_sha256"], "contract_version": "orc-07-v1",
                       "mode": "real", "status": "ok", "upstream_sha256": None if previous is None else e2e.digest(previous),
                       "artifact_sha256": e2e.digest(artifact), "artifact_uri": "s3://test/real-etl/" + op + "/" + phase + ".json"}
            if phase in ("gold", "verify", "publish"):
                receipt["snapshots"] = {t: p["snapshot_id"] for t, p in commit["tables"].items()}
            if phase == "publish":
                receipt.update(publication_id=pub["publication_id"], published=True)
            row["receipts"].append(receipt); previous = receipt
        return row

    def test_receipt_artifact_commit_snapshot_chain(self):
        e2e.publication(self.record(), self.ns, self.root)

    def test_context_upstream_artifact_and_snapshot_tamper_rejected(self):
        row = self.record()
        changes = [lambda r: r["context"].update(run_id="changed"),
                   lambda r: r["receipts"][1].update(upstream_sha256="e" * 64),
                   lambda r: r["receipts"][2].update(artifact_sha256="e" * 64),
                   lambda r: r["commit"]["tables"]["event_current"].update(snapshot_id=True)]
        for change in changes:
            altered = copy.deepcopy(row); change(altered)
            with self.assertRaises(RuntimeError):
                e2e.publication(altered, self.ns, self.root)

    def test_snapshot_sql_cannot_read_latest_or_different_pin(self):
        row = self.record()
        row["publication"]["checks"]["count:event_current"]["sql"] = "SELECT count(*) FROM " + self.ns + ".event_current"
        path = self.root / row["operation_id"] / "publish.json"; path.write_text(json.dumps(row["publication"]))
        row["receipts"][-1]["artifact_sha256"] = e2e.digest(row["publication"])
        with self.assertRaisesRegex(RuntimeError, "SNAPSHOT_QUERY_CHANGED"):
            e2e.publication(row, self.ns, self.root)

    def test_all_staged_input_bytes_and_capture_metadata_are_checked_before_lease(self):
        files = {"runner.jar": "runner_sha256", "baseline.csv": "baseline_inventory_sha256", "boundary.csv": "boundary_inventory_sha256"}
        request = {"version": e2e.VERSION, "namespace": self.ns, "run_id": "dat03-" + "a" * 32,
                   "lease_owner": {"dag_id": "dat03_bounded_e2e", "run_id": "dat03-" + "a" * 32}, "jma_captures": []}
        inputs = self.root / "inputs"; inputs.mkdir()
        for name, field in files.items():
            file = self.root / name; file.write_text(name); request[field] = e2e.sha(file)
        for year in e2e.YEARS:
            file = inputs / f"h{year}.zip"; file.write_bytes(b"raw")
            meta = {"sha256": e2e.sha(file), "bytes": 3, "http_status": 200, "inventory_metadata_drift": False}
            (inputs / f"h{year}-capture.json").write_text(json.dumps(meta))
            request["jma_captures"].append({"year": year, "sha256": e2e.sha(file), "capture_sha256": e2e.digest(meta)})
        usgs = inputs / "usgs-december2023.geojson"; usgs.write_bytes(b"{}")
        meta = {"url": e2e.QUERY, "http_status": 200, "sha256": e2e.sha(usgs), "bytes": 2}
        (inputs / "usgs-capture.json").write_text(json.dumps(meta)); request["usgs_capture_sha256"] = e2e.digest(meta)
        e2e.validate_inputs(self.root, request)
        (inputs / "h2011.zip").write_bytes(b"bad")
        with self.assertRaisesRegex(RuntimeError, "JMA_PIN_CHANGED"):
            e2e.validate_inputs(self.root, request)

    def test_missing_artifact_and_symlink_rejected(self):
        row = self.record(); file = self.root / row["operation_id"] / "readiness.json"
        file.rename(file.with_suffix(".old")); file.symlink_to(file.with_suffix(".old"))
        with self.assertRaises(RuntimeError):
            e2e.publication(row, self.ns, self.root)

    def test_modified_blocker_or_readback_rejected_even_when_rehashed(self):
        row = self.record()
        for name, actual, expected in (("required_fields", 1, 1), ("count:event_current", 2, 2)):
            altered = copy.deepcopy(row); altered["publication"]["checks"][name].update(actual=actual, expected=expected)
            file = self.root / row["operation_id"] / "publish.json"; file.write_text(json.dumps(altered["publication"]))
            altered["receipts"][-1]["artifact_sha256"] = e2e.digest(altered["publication"])
            with self.assertRaises(RuntimeError):
                e2e.publication(altered, self.ns, self.root)

    def test_capture_reuse_never_silently_repins_changed_bytes(self):
        folder = self.root / "staging/usgs"; folder.mkdir(parents=True)
        raw = folder / "usgs-december2023.geojson"; raw.write_bytes(b"{}")
        meta = {"url": e2e.QUERY, "http_status": 200, "sha256": e2e.sha(raw), "bytes": 2}
        (folder / "capture.json").write_text(json.dumps(meta))
        with patch.object(e2e, "ROOT", self.root), patch.object(e2e.subprocess, "run", side_effect=AssertionError("network forbidden")):
            self.assertEqual(meta, e2e.capture(folder))
            raw.write_bytes(b"[]")
            with self.assertRaises(RuntimeError):
                e2e.capture(folder)

    def test_report_cannot_claim_full_catalog_research_or_missing_cases(self):
        request = {"namespace": self.ns, "run_id": "dat03-" + "a" * 32, "runner_sha256": "b" * 64}
        report = {**request, "version": e2e.VERSION, "status": "PASSED", "research_ready": False,
                  "full_research_catalog": False, "synthetic_data_used": False, "cases": []}
        for name in ("research_ready", "full_research_catalog", "synthetic_data_used"):
            changed = {**report, name: True}
            with self.assertRaises(RuntimeError):
                e2e.validate(request, changed, self.root)
        with self.assertRaises(RuntimeError):
            e2e.validate(request, report, self.root)


if __name__ == "__main__":
    unittest.main()
