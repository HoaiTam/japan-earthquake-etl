"""Offline storage fixtures: no real ETL, source network, Docker, or scientific approval."""
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location("observation_test",ROOT/"scripts/research-observation.py")
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class ObservationTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name).resolve()
        self.catalog=self.base/"staging/dat03-catalog"/("a"*32);self.catalog.mkdir(parents=True)
        self.frozen={"request.json","inputs/source.zip"}
        (self.catalog/"inputs").mkdir();(self.catalog/"inputs/source.zip").write_bytes(b"original-input")
        (self.catalog/"request.json").write_bytes(b'{"fixture":true}\n')
        self.source=self.catalog/"runtime";self.source.mkdir();(self.source/"inputs").mkdir()
        for name in self.frozen:(self.source/name).write_bytes((self.catalog/name).read_bytes())
        (self.source/"months/2000-01").mkdir(parents=True)
        (self.source/"months/2000-01/record.json").write_bytes(b'{}\n')
        (self.source/"months/2000-01/duplicate.json").write_bytes(b'{}\n')
        (self.source/"empty").mkdir()
        self.target=self.catalog/("runtime-observation-"+"b"*32)

    def imported(self):return m.import_local(self.catalog,self.source,self.target,self.frozen)

    def proof(self,root):
        tree=m.view(root,self.frozen) if (root/m.MARKER).exists() else root
        self.assertEqual(b'{}\n',(tree/"months/2000-01/record.json").read_bytes())
        return {"status":"ACCEPTED_CATALOG_CHECKPOINT","completed_months":1}

    def test_frozen_input_once_and_duplicate_payload_one_object(self):
        tree=self.imported()
        self.assertEqual(b"original-input",(tree/"inputs/source.zip").read_bytes())
        objects=list((self.catalog/m.STORE/"blobs").glob("*/*"))
        self.assertEqual(1,len(objects));self.assertEqual(b'{}\n',objects[0].read_bytes())
        self.assertEqual({m.MARKER},{p.name for p in self.target.iterdir()})

    def test_second_snapshot_reuses_objects_without_new_payload(self):
        self.imported();before=list((self.catalog/m.STORE/"blobs").glob("*/*"))
        second=self.catalog/("runtime-observation-"+"c"*32)
        m.import_local(self.catalog,self.source,second,self.frozen)
        self.assertEqual(before,list((self.catalog/m.STORE/"blobs").glob("*/*")))

    def test_view_glob_parent_iterdir_and_readonly(self):
        tree=self.imported();files=list((tree/"months").glob("*/record.json"))
        self.assertEqual("2000-01",files[0].parent.name)
        self.assertEqual(["2000-01"],[p.name for p in (tree/"months").iterdir()])
        with self.assertRaisesRegex(RuntimeError,"READ_ONLY"):(tree/"request.json").open("w")

    def test_full_restore_has_identical_files_modes_and_empty_directories(self):
        self.imported();target=self.catalog/"restored-fixture"
        m.materialize(self.target,target,self.frozen)
        self.assertEqual(m.inventory(self.source),m.inventory(target))
        self.assertTrue((target/"empty").is_dir())
        (target/"months/2000-01/record.json").write_bytes(b'changed restore')
        self.assertEqual(b'{}\n',(m.view(self.target,self.frozen)/"months/2000-01/record.json").read_bytes())

    def test_restore_never_overlays_existing_path(self):
        self.imported()
        with self.assertRaisesRegex(RuntimeError,"NEW_RESTORE"):m.materialize(self.target,self.source,self.frozen)

    def test_missing_object_fails_entire_view(self):
        tree=self.imported();meta=tree.manifest["files"]["months/2000-01/record.json"]
        m.blob(self.catalog,meta["sha256"]).unlink()
        with self.assertRaisesRegex(RuntimeError,"HASH_OR_SIZE"):m.view(self.target,self.frozen)

    def test_corrupt_object_is_never_accepted_or_rehashed_as_new_proof(self):
        tree=self.imported();meta=tree.manifest["files"]["months/2000-01/record.json"]
        path=m.blob(self.catalog,meta["sha256"]);path.chmod(0o600);path.write_bytes(b"bad")
        with self.assertRaisesRegex(RuntimeError,"HASH_OR_SIZE"):m.view(self.target,self.frozen)

    def test_catalog_input_change_invalidates_snapshot(self):
        self.imported();(self.catalog/"inputs/source.zip").write_bytes(b"changed")
        with self.assertRaisesRegex(RuntimeError,"HASH_OR_SIZE"):m.view(self.target,self.frozen)

    def test_wrong_request_or_manifest_version_is_rejected(self):
        self.imported();path=self.target/m.MARKER
        value=m.load(path);value["catalog_request_sha256"]="f"*64;path.write_text(json.dumps(value))
        with self.assertRaisesRegex(RuntimeError,"MANIFEST_OR_REQUEST"):m.view(self.target,self.frozen)

    def test_traversal_and_fake_metadata_are_rejected(self):
        for name in ("../other","/absolute","a/../b","a\\b","a//b","."):
            with self.assertRaises(RuntimeError):m.relative(name)
        data=m.inventory(self.source);data["files"]["request.json"]["bytes"]=True
        with self.assertRaisesRegex(RuntimeError,"INVALID_FILE"):m.validate_inventory(**data)

    def test_symlink_and_hardlink_inventory_rejected(self):
        link=self.source/"link";link.symlink_to(self.source/"request.json")
        with self.assertRaisesRegex(RuntimeError,"SYMLINK"):m.inventory(self.source)
        link.unlink();os.link(self.source/"request.json",link)
        with self.assertRaisesRegex(RuntimeError,"REGULAR_BOUNDED"):m.inventory(self.source)

    def test_short_or_bad_stream_keeps_pending_and_never_publishes_blob(self):
        for payload in (b"x",b"bad"):
            meta={"bytes":3,"sha256":hashlib.sha256(b"yes").hexdigest(),"mode":0o600}
            with self.assertRaises(RuntimeError):m.put(self.catalog,meta,io.BytesIO(payload))
            self.assertFalse(m.blob(self.catalog,meta["sha256"]).exists())

    def test_import_never_overwrites_existing_descriptor(self):
        self.imported()
        with self.assertRaises(FileExistsError):self.imported()

    def test_legacy_compaction_verifies_before_removing_and_preserves_bytes(self):
        result=m.compact_legacy(self.catalog,self.source,self.frozen,self.proof)
        self.assertEqual({m.MARKER},{p.name for p in self.source.iterdir()})
        self.assertTrue(result["accepted_progress_unchanged"]);self.proof(self.source)

    def test_failed_import_validation_never_deletes_original(self):
        def validate(root):
            if root!=self.source:raise RuntimeError("fixture-reject")
            return self.proof(root)
        with self.assertRaisesRegex(RuntimeError,"fixture-reject"):
            m.compact_legacy(self.catalog,self.source,self.frozen,validate)
        self.assertFalse((self.source/m.MARKER).exists());self.assertTrue((self.source/"inputs/source.zip").is_file())

    def test_interrupted_unlink_keeps_a_complete_readable_descriptor(self):
        actual=Path.unlink
        def fail(path,*args,**kwargs):
            if path==self.source/"request.json":raise OSError("fixture-disk-error")
            return actual(path,*args,**kwargs)
        with patch.object(Path,"unlink",fail):
            with self.assertRaisesRegex(OSError,"fixture-disk-error"):
                m.compact_legacy(self.catalog,self.source,self.frozen,self.proof)
        self.assertTrue((self.source/m.MARKER).is_file());self.proof(self.source)

    def test_latest_requires_accepted_receipt_and_cannot_regress(self):
        self.imported();proof={"status":"ACCEPTED_CATALOG_CHECKPOINT","lease_release":{"status":"RELEASED"},
                              "completed_months":2,"session_id":"c"*32}
        bad={**proof,"status":"FAILED"}
        with self.assertRaisesRegex(RuntimeError,"REQUIRES_ACCEPTANCE"):m.publish_latest(self.catalog,self.target,bad)
        m.publish_latest(self.catalog,self.target,proof)
        proof["completed_months"]=1
        with self.assertRaisesRegex(RuntimeError,"CANNOT_REGRESS"):m.publish_latest(self.catalog,self.target,proof)

    def test_remote_protocol_has_no_etl_or_service_mutations(self):
        for script in (m.REMOTE_INVENTORY,m.REMOTE_STREAM):
            for word in ("unlink(","rmtree","compose","restart","lease","spark-submit"):
                self.assertNotIn(word,script)
        self.assertNotIn("docker",m.materialize.__doc__ or "")

    def test_foreign_writer_lock_is_never_replaced_or_removed(self):
        store=self.catalog/m.STORE;store.mkdir()
        m.persist(store/"writer.lock",{"pid":123,"token":"foreign"})
        with self.assertRaises(FileExistsError):
            m.compact_legacy(self.catalog,self.source,self.frozen,self.proof)
        self.assertEqual("foreign",m.load(store/"writer.lock")["token"])
        self.assertFalse((self.source/m.MARKER).exists())

    def test_failed_writer_keeps_lock_without_promoting_latest(self):
        with self.assertRaisesRegex(RuntimeError,"fixture-fail"):
            with m.exclusive(self.catalog):raise RuntimeError("fixture-fail")
        self.assertTrue((self.catalog/m.STORE/"writer.lock").is_file())
        self.assertFalse((self.catalog/m.STORE/"latest.json").exists())

    def test_unchanged_runtime_capture_transfers_no_old_payload(self):
        self.imported();listing=m.inventory(self.source)
        # Remote runtime layout has months/sessions, not the synthetic empty directory.
        listing["directories"].remove("empty")
        target=self.catalog/("runtime-observation-"+"d"*32)
        with patch.object(m,"remote_inventory",return_value=listing), \
                patch.object(m.subprocess,"Popen",side_effect=AssertionError("no unchanged payload transfer")):
            tree,measurement=m.capture(self.catalog,target,"fixture-container","fixture-root",self.frozen,lambda:{"fixture":True})
        self.assertEqual(0,measurement["unique_payload_bytes_transferred"])
        self.assertEqual(0,measurement["new_payload_objects"])
        self.assertEqual(b'{}\n',(tree/"months/2000-01/record.json").read_bytes())

    def test_recovery_resolution_has_exact_filename_allowlist_and_is_captured(self):
        name="session-resolutions/"+"a"*32+".json"
        self.assertTrue(m.allowed_runtime_file(name,self.frozen))
        for bad in ("session-resolutions/.env","session-resolutions/../request.json","session-resolutions/"+"a"*31+".json",
                    "session-resolutions/"+"a"*32+".json/private","session-resolutions/latest.json",".env"):
            self.assertFalse(m.allowed_runtime_file(bad,self.frozen))
        (self.source/"session-resolutions").mkdir();(self.source/name).write_bytes(b'{}\n')
        self.imported();listing=m.inventory(self.source);listing["directories"].remove("empty")
        target=self.catalog/("runtime-observation-"+"d"*32)
        with patch.object(m,"remote_inventory",return_value=listing),patch.object(m.subprocess,"Popen",side_effect=AssertionError("no old payload transfer")):
            tree,measurement=m.capture(self.catalog,target,"fixture-container","fixture-root",self.frozen,lambda:{})
        self.assertEqual(b'{}\n',(tree/name).read_bytes());self.assertEqual(0,measurement["unique_payload_bytes_transferred"])

    def test_runtime_drift_cannot_publish_a_descriptor(self):
        self.imported();listing=m.inventory(self.source)
        target=self.catalog/("runtime-observation-"+"d"*32)
        changed=copy.deepcopy(listing);changed["directories"].append("new-empty")
        with patch.object(m,"remote_inventory",side_effect=[listing,changed]):
            with self.assertRaisesRegex(RuntimeError,"RUNTIME_CHANGED"):
                m.capture(self.catalog,target,"fixture-container","fixture-root",self.frozen,lambda:{})
        self.assertFalse(target.exists());self.assertTrue((self.source/"request.json").exists())

    def test_progress_report_reads_the_same_receipts_from_a_compact_view(self):
        spec=importlib.util.spec_from_file_location("observation_progress_test",ROOT/"scripts/research-catalog-progress.py")
        p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
        request={"run_id":"fixture"}
        for root in (self.catalog,self.source):(root/"request.json").write_text(json.dumps(request))
        session=self.source/"sessions"/("c"*32);session.mkdir(parents=True)
        execution={"exit_code":0,"timed_out":False,"wall_seconds":1200,"child_peak_rss_kib":1000}
        (session/"execution.json").write_text(json.dumps(execution));(session/"result.json").write_text('{}')
        acceptance={"version":p.c.VERSION,"status":"CHECKPOINT_ACCEPTED",
                    "request_sha256":m.sha(self.catalog/"request.json"),"result_sha256":m.sha(session/"result.json"),
                    "execution_sha256":m.sha(session/"execution.json"),"lease_release":{"status":"RELEASED","dag_id":p.c.DAG,"run_id":"fixture"},
                    "official_exported":False,"research_ready":False}
        (session/"acceptance.json").write_text(json.dumps(acceptance))
        result={"completed_months":0,"completed_in_session":0,"reproduction_months":0,"extension_months":0,"current_snapshots":{}}
        self.imported()
        with patch.object(p.c,"files",return_value=self.frozen),patch.object(p.c,"validate",return_value=request), \
                patch.object(p.c,"session_result",return_value=result):
            original=p.report(self.catalog,self.source,session.name)
            compact=p.report(self.catalog,self.target,session.name)
        self.assertEqual(original,compact);self.assertFalse(compact["official_exported"])

    def test_framed_stream_reader_stops_at_exact_bytes_and_eof(self):
        read,write=os.pipe()
        os.write(write,b'abc');os.close(write)
        with os.fdopen(read,'rb') as stream:
            reader=m.DeadlineReader(stream)
            self.assertEqual(b'ab',reader.read(2));self.assertEqual(b'c',reader.read(1))
            self.assertEqual(b'',reader.read(1))

    def test_stream_timeout_is_bounded(self):
        read,write=os.pipe()
        try:
            with os.fdopen(read,'rb') as stream:
                reader=m.DeadlineReader(stream,seconds=-1)
                with self.assertRaisesRegex(RuntimeError,'TRANSFER_TIMEOUT'):reader.read(1)
        finally:os.close(write)

    def test_capture_checks_remaining_disk_before_transferring_or_sealing(self):
        self.imported();listing=m.inventory(self.source)
        target=self.catalog/("runtime-observation-"+"d"*32)
        with patch.object(m,"remote_inventory",return_value=listing), \
                patch.object(m.shutil,"disk_usage",return_value=type('Disk',(),{'free':0})()), \
                patch.object(m.subprocess,"Popen",side_effect=AssertionError('no transfer')):
            with self.assertRaisesRegex(RuntimeError,'TRANSFER_DISK_RESERVE'):
                m.capture(self.catalog,target,'fixture-container','fixture-root',self.frozen,lambda:{})
        self.assertFalse(target.exists())

    def test_boolean_prefix_cannot_become_latest(self):
        self.imported();proof={'status':'ACCEPTED_CATALOG_CHECKPOINT','lease_release':{'status':'RELEASED'},
                              'completed_months':True,'session_id':'c'*32}
        with self.assertRaisesRegex(RuntimeError,'TYPED_PREFIX'):m.publish_latest(self.catalog,self.target,proof)


if __name__=="__main__":unittest.main()
