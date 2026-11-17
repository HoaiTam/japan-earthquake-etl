"""OPS-01 tiny deterministic graphs; offline, no Docker/source network."""
import importlib.util
import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import ops_backup as b
import fastavro

cli_spec = importlib.util.spec_from_file_location('ops01_cli', Path(b.__file__).with_name('ops-backup.py'))
cli = importlib.util.module_from_spec(cli_spec); cli_spec.loader.exec_module(cli)

ROOT = 's3://qa-bucket/warehouse/source/events'
DATA = ROOT + '/data/a.parquet'
MANIFEST = ROOT + '/metadata/m.avro'
LIST = ROOT + '/metadata/l.avro'
META = ROOT + '/metadata/v1.metadata.json'
MANIFEST_SCHEMA = {'type': 'record', 'name': 'entry', 'fields': [
    {'name': 'status', 'type': 'int'}, {'name': 'data_file', 'type': {'type': 'record', 'name': 'file', 'fields': [
        {'name': 'content', 'type': 'int'}, {'name': 'file_path', 'type': 'string'},
        {'name': 'file_format', 'type': 'string'}, {'name': 'file_size_in_bytes', 'type': 'long'}]}}]}
LIST_SCHEMA = {'type': 'record', 'name': 'list', 'fields': [
    {'name': 'manifest_path', 'type': 'string'}, {'name': 'manifest_length', 'type': 'long'}]}


def avro(schema, rows, codec='null'):
    f = io.BytesIO(); fastavro.writer(f, schema, rows, codec=codec, metadata={'spec-version': '2'}); return f.getvalue()


def graph():
    data = b'PARQUET-BYTES-UNCHANGED'
    m = avro(MANIFEST_SCHEMA, [{'status': 1, 'data_file': {'content': 0, 'file_path': DATA, 'file_format': 'PARQUET', 'file_size_in_bytes': len(data)}}])
    objects = {DATA: data, MANIFEST: m, LIST: avro(LIST_SCHEMA, [{'manifest_path': MANIFEST, 'manifest_length': len(m)}]),
               META: b.encode({'format-version': 2, 'location': ROOT, 'table-uuid': 'original', 'current-snapshot-id': 9007199254740993,
                               'snapshots': [{'snapshot-id': 9007199254740993, 'manifest-list': LIST}], 'metadata-log': []})}
    g = b.Graph(objects.__getitem__); g.add(META, 'metadata', ROOT)
    tables = [{'namespace': 'source', 'name': 'events', 'root': ROOT, 'metadata_uri': META,
               'uuid': 'original', 'snapshot_id': 9007199254740993, 'signature': [1, 'ABCD']}]
    return objects, g, tables


class OpsBackupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve(); self.path = self.root / 'backup.zip'
        self.objects, self.graph, self.tables = graph()

    def bundle(self):
        return b.make_bundle(self.path, self.tables, self.graph, {'selected.json': b'{}'})

    def test_graph_contains_full_metadata_avro_data_closure(self):
        self.assertEqual(set(self.objects), set(self.graph.objects)); self.assertEqual(4, len(self.graph.objects))

    def test_shared_manifest_downloaded_once(self):
        get = Mock(side_effect=self.objects.__getitem__); g = b.Graph(get)
        g.add(META, 'metadata', ROOT); g.add(META, 'metadata', ROOT); self.assertEqual(4, get.call_count)

    def test_relative_external_encoded_credential_uris_rejected(self):
        for value in ('file:///etc/passwd', 's3://qa-bucket/../x', 's3://qa-bucket/a//x', 's3://qa-bucket/a%2fx', 's3://a:' + 'b@qa-bucket/a', 's3://qa-bucket/a?token=x'):
            with self.subTest(value=value), self.assertRaises(ValueError): b.uri(value)

    def test_table_root_boundary(self):
        with self.assertRaisesRegex(ValueError, 'EXTERNAL_GRAPH_REFERENCE'):
            self.graph.add(ROOT + '-other/file', 'data', ROOT)

    def test_restore_partition_equal_sign_allowed_only_under_own_exact_prefix(self):
        good = 's3://qa-bucket/warehouse/ops01-restores/' + 'a' * 32 + '/ops01_' + 'a' * 32 + '_0/events/data/month=2000-01/file.parquet'
        b.restore_uri(good)
        for bad in (good.replace('/ops01_' + 'a' * 32, '/ops01_' + 'b' * 32), good.replace('ops01-restores/', 'production/'), good.replace('month=2000-01', '..')):
            with self.assertRaises(ValueError): b.restore_uri(bad)

    def test_delete_files_fail_closed(self):
        self.objects[MANIFEST] = avro(MANIFEST_SCHEMA, [{'status': 1, 'data_file': {'content': 1, 'file_path': DATA, 'file_format': 'PARQUET', 'file_size_in_bytes': 23}}])
        with self.assertRaisesRegex(ValueError, 'DELETE_OR_NONPARQUET'): b.Graph(self.objects.__getitem__).add(META, 'metadata', ROOT)

    def test_unsupported_statistics_fail_closed(self):
        m = b.decode(self.objects[META]); m['partition-statistics'] = [{}]; self.objects[META] = b.encode(m)
        with self.assertRaisesRegex(ValueError, 'PARTITION_STATISTICS_NOT_SUPPORTED'): b.Graph(self.objects.__getitem__).add(META, 'metadata', ROOT)

    def test_puffin_footer_copy_and_unknown_compression_rejected(self):
        footer = b.encode({'blobs': [], 'properties': {'created-by': 'unit-test'}})
        raw = b'PFA1PFA1' + footer + len(footer).to_bytes(4, 'little') + b'\0' * 4 + b'PFA1'
        self.assertEqual([], b.puffin_footer(raw)['blobs'])
        with self.assertRaises(ValueError): b.puffin_footer(raw[:-8] + b'\1\0\0\0PFA1')

    def test_byte_bounds(self):
        with patch.object(b, 'MAX_TOTAL', 2), self.assertRaisesRegex(ValueError, 'BACKUP_BYTE_BOUND'):
            b.Graph(self.objects.__getitem__).add(META, 'metadata', ROOT)

    def test_object_count_bound(self):
        with patch.object(b, 'MAX_OBJECTS', 1), self.assertRaisesRegex(ValueError, 'OBJECT_COUNT_BOUND'):
            b.Graph(self.objects.__getitem__).add(META, 'metadata', ROOT)

    def test_wrong_data_size(self):
        self.objects[DATA] += b'changed'
        with self.assertRaisesRegex(ValueError, 'DATA_FILE_SIZE'): b.Graph(self.objects.__getitem__).add(META, 'metadata', ROOT)

    def test_duplicate_json_and_nonfinite_rejected(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}'):
            with self.assertRaises(ValueError): b.decode(raw)

    def test_backup_hash_and_exact_graph_roundtrip(self):
        sha = self.bundle(); m, g, a = b.read_bundle(self.path, sha)
        self.assertEqual(set(self.objects), set(g.objects)); self.assertEqual({'selected.json': b'{}'}, a)
        self.assertEqual(9007199254740993, m['tables'][0]['snapshot_id'])

    def test_archive_immutable(self):
        self.bundle()
        with self.assertRaises(FileExistsError): b.make_bundle(self.path, self.tables, self.graph, {})

    def test_wrong_archive_pin_rejected_before_any_object_read(self):
        self.bundle()
        with self.assertRaisesRegex(ValueError, 'BUNDLE_SHA_CHANGED'): b.read_bundle(self.path, '0' * 64)

    def altered(self, change):
        self.bundle()
        with zipfile.ZipFile(self.path) as z: members = {n: z.read(n) for n in z.namelist()}
        m = b.decode(members['manifest.json']); change(members, m); members['manifest.json'] = b.encode(m)
        second = self.root / 'changed.zip'
        with zipfile.ZipFile(second, 'w') as z:
            for name, raw in members.items(): z.writestr(name, raw)
        return second, b.digest(second.read_bytes())

    def test_missing_object_rejected(self):
        p, sha = self.altered(lambda members, m: members.pop('objects/' + m['objects'][0]['sha256']))
        with self.assertRaisesRegex(ValueError, 'EXACT_ZIP_MEMBER_SET'): b.read_bundle(p, sha)

    def test_declared_object_size_rejected(self):
        p, sha = self.altered(lambda members, m: m['objects'][0].update({'bytes': m['objects'][0]['bytes'] + 1}))
        with self.assertRaisesRegex(ValueError, 'ZIP_OBJECT_SIZE_OR_TYPE'): b.read_bundle(p, sha)

    def test_duplicate_object_uri_rejected(self):
        p, sha = self.altered(lambda members, m: m['objects'].append(m['objects'][0].copy()))
        with self.assertRaisesRegex(ValueError, 'DUPLICATE_OBJECT_URI'): b.read_bundle(p, sha)

    def test_artifact_traversal_not_extracted(self):
        p, sha = self.altered(lambda members, m: m['artifacts'][0].update({'name': '../escape'}))
        with self.assertRaisesRegex(ValueError, 'ARTIFACT_PATH'): b.read_bundle(p, sha)

    def test_changed_object_rejected_even_when_archive_sha_matches(self):
        def change(members, m):
            k = 'objects/' + m['objects'][0]['sha256']; members[k] = b'x' * len(members[k])
        p, sha = self.altered(change)
        with self.assertRaisesRegex(ValueError, 'OBJECT_SHA_CHANGED'): b.read_bundle(p, sha)

    def test_unlisted_member_rejected(self):
        p, sha = self.altered(lambda members, m: members.update({'../escape': b'bad'}))
        with self.assertRaisesRegex(ValueError, 'EXACT_ZIP_MEMBER_SET'): b.read_bundle(p, sha)

    def test_duplicate_zip_member_rejected(self):
        sha = self.bundle()
        with zipfile.ZipFile(self.path, 'a') as z:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter('ignore'); z.writestr('manifest.json', b'{}')
        with self.assertRaisesRegex(ValueError, 'DUPLICATE_ZIP_MEMBER'): b.read_bundle(self.path, b.digest(self.path.read_bytes()))

    def test_relocation_keeps_snapshot_ids_and_parquet_bytes(self):
        m, g, _ = b.read_bundle(self.path, self.bundle()); tables, objects = b.relocated(m, g, 'a' * 32)
        t = tables[0]; meta = b.decode(objects[t['metadata_uri']])
        self.assertEqual(self.tables[0]['snapshot_id'], meta['current-snapshot-id'])
        self.assertNotEqual(ROOT, meta['location']); self.assertTrue(all('/ops01-restores/' in u for u in objects))
        data = next(raw for k, raw in objects.items() if k.endswith('.parquet'))
        self.assertEqual(self.objects[DATA], data)

    def test_relocated_graph_is_complete_and_manifest_lengths_updated(self):
        m, g, _ = b.read_bundle(self.path, self.bundle()); tables, objects = b.relocated(m, g, 'a' * 32)
        t = tables[0]; restored = b.Graph(objects.__getitem__); restored.add(t['metadata_uri'], 'metadata', t['root'])
        self.assertEqual(set(objects), set(restored.objects))
        for key, raw in objects.items():
            if key.endswith('/l.avro'):
                row = b.avro_read(raw)[2][0]; self.assertEqual(row['manifest_length'], len(objects[row['manifest_path']]))

    def test_restore_plan_deterministic(self):
        m, g, _ = b.read_bundle(self.path, self.bundle())
        self.assertEqual(b.relocated(m, g, 'a' * 32), b.relocated(m, g, 'a' * 32))

    def test_wrong_run_identity_rejected(self):
        m, g, _ = b.read_bundle(self.path, self.bundle())
        for run in ('main', '../data', 'A' * 32):
            with self.assertRaisesRegex(ValueError, 'RESTORE_RUN_ID'): b.relocated(m, g, run)

    def test_no_overwrite_of_host_receipt(self):
        p = self.root / 'receipt.json'; b.private_write(p, b'{}')
        with self.assertRaises(FileExistsError): b.private_write(p, b'changed')
        self.assertEqual(0o600, p.stat().st_mode & 0o777)

    def test_symlink_backup_rejected(self):
        self.bundle(); p = self.root / 'link'; p.symlink_to(self.path)
        with self.assertRaisesRegex(ValueError, 'SYMLINK'): b.read_bundle(p, b.digest(self.path.read_bytes()))

    def test_restore_source_drift_has_no_remote_writes(self):
        sha = self.bundle(); remote = Mock(); remote.load.return_value = {'metadata-location': 'changed'}
        with self.assertRaisesRegex(ValueError, 'SOURCE_PIN_DRIFT'): b.restore(remote, self.path, sha, 'a' * 32, self.root)
        remote.put.assert_not_called(); remote.call.assert_not_called()

    def test_partial_put_failure_has_intent_and_no_register_or_auto_retry(self):
        sha = self.bundle(); remote = Mock(); remote.load.return_value = {'metadata-location': META}
        remote.put.side_effect = ValueError('OPS01_SIMULATED_FAILURE')
        with self.assertRaisesRegex(ValueError, 'SIMULATED_FAILURE'): b.restore(remote, self.path, sha, 'a' * 32, self.root)
        self.assertTrue((self.root / 'restore-intent.json').is_file())
        self.assertEqual(1, remote.put.call_count); remote.call.assert_not_called()
        self.assertEqual(b'{}', (self.root / 'artifacts/selected.json').read_bytes())

    def test_put_readback_failure_never_registers_table(self):
        sha = self.bundle(); remote = Mock(); remote.load.return_value = {'metadata-location': META}; remote.get.return_value = b'changed'
        with self.assertRaisesRegex(ValueError, 'RESTORED_OBJECT_READBACK'): b.restore(remote, self.path, sha, 'a' * 32, self.root)
        self.assertEqual(1, remote.put.call_count); remote.call.assert_not_called()

    def test_remote_put_rejects_production_before_docker_call(self):
        r = b.Remote()
        with patch.object(r, 'call') as call, self.assertRaises(ValueError): r.put('s3://qa-bucket/warehouse/production/data.parquet', b'payload')
        call.assert_not_called()

    def test_cli_rejects_existing_output_and_repository_root(self):
        with self.assertRaises(ValueError): cli.folder(Path(b.__file__).resolve().parents[1])
        with self.assertRaises(ValueError): cli.folder(self.root)

    def test_real_profile_is_exact_and_has_two_bronze_pins(self):
        sources, artifacts, run, bronze = cli.config(Path(b.__file__).resolve().parents[1] / 'config/ops/backup-smoke-v1.json')
        self.assertEqual(13, len(sources)); self.assertEqual(2, len(bronze)); self.assertIn('selection.json', artifacts)

    def test_config_cannot_inject_table_or_add_ignored_key(self):
        p = Path(b.__file__).resolve().parents[1] / 'config/ops/backup-smoke-v1.json'
        value = b.decode(p.read_bytes()); value['arbitrary_sql'] = 'anything'
        local = self.root / 'bad.json'; local.write_bytes(b.encode(value))
        with self.assertRaisesRegex(ValueError, 'CONFIG_KEYS'): cli.config(local)

    @staticmethod
    def handoff():
        import pyarrow as pa
        import pyarrow.parquet as pq
        artifacts = {}
        for name in ('memberships.parquet', 'sequence_summary.parquet'):
            f = io.BytesIO(); pq.write_table(pa.table({'id': [1]}), f); artifacts['valid/' + name] = f.getvalue()
        for name in ('experiment_metrics.json', 'experiment_config.json', 'requirements-lock.txt'):
            artifacts['valid/' + name] = b'{}'
        export = b.encode({'dataset_id': 'ds_test'})
        artifacts['registry/export.json'] = export
        artifacts['registry/dataset.json'] = b.encode({'dataset_id': 'ds_test', 'dataset_status': 'EXPORTED',
            'export_manifest_ref': 'export.json', 'export_manifest_sha256': b.digest(export),
            'candidate_snapshot_id': '1', 'mainshock_snapshot_id': '2', 'registry_snapshot_id': '3'})
        artifacts['valid/_SUCCESS.json'] = b.encode({'status': 'COMPLETED', 'dataset_id': 'ds_test', 'experiment_run_id': 'exp_test',
            'membership_row_count': 1, 'summary_row_count': 1,
            'checksums': {k.removeprefix('valid/'): b.digest(v) for k, v in artifacts.items() if k.startswith('valid/')}})
        manifest = {'tables': [{'name': name, 'snapshot_id': i} for i, name in enumerate(('candidate_event','mainshock_event','dataset_registry'), 1)]}
        return manifest, artifacts

    def test_original_artifact_lineage_positive(self):
        m, a = self.handoff(); self.assertEqual('ds_test', b.verify_artifacts(m, a)['dataset_id'])

    def test_changed_result_config_rejected(self):
        m, a = self.handoff(); a['valid/experiment_config.json'] = b'changed'
        with self.assertRaisesRegex(ValueError, 'RESULT_ARTIFACT_SHA_CHANGED'): b.verify_artifacts(m, a)

    def test_manifest_missing_checksum_rejected(self):
        m, a = self.handoff(); s = b.decode(a['valid/_SUCCESS.json']); s['checksums'].pop('memberships.parquet'); a['valid/_SUCCESS.json'] = b.encode(s)
        with self.assertRaisesRegex(ValueError, 'COMPLETION_FILE_SET'): b.verify_artifacts(m, a)

    def test_registry_must_be_exported_and_bound_to_exact_export(self):
        m, a = self.handoff(); d = b.decode(a['registry/dataset.json']); d['dataset_status'] = 'BUILDING'; a['registry/dataset.json'] = b.encode(d)
        with self.assertRaisesRegex(ValueError, 'DATASET_EXPORT_LINEAGE'): b.verify_artifacts(m, a)

    def test_snapshot_pin_change_rejected(self):
        m, a = self.handoff(); m['tables'][0]['snapshot_id'] = 4
        with self.assertRaisesRegex(ValueError, 'DATASET_REGISTRY_SNAPSHOT_PIN'): b.verify_artifacts(m, a)

    def test_result_parquet_count_change_rejected(self):
        m, a = self.handoff(); s = b.decode(a['valid/_SUCCESS.json']); s['membership_row_count'] = 2; a['valid/_SUCCESS.json'] = b.encode(s)
        with self.assertRaisesRegex(ValueError, 'RESULT_ARTIFACT_ROW_COUNT'): b.verify_artifacts(m, a)

    def test_lineage_join_mismatch_fails_without_compensation(self):
        r = Mock(); r.query.return_value = [[99]]
        with self.assertRaisesRegex(ValueError, 'RESTORED_ML_LINEAGE_PUBLISHED_EXPERIMENT'):
            b.lineage_readback(r, [{'namespace': 'ops01_test', 'name': 'experiment_run'}], {'membership_rows': 1, 'summary_rows': 1})
        self.assertEqual(1, r.query.call_count); r.put.assert_not_called(); r.call.assert_not_called()

    def test_identifier_and_snapshot_guard(self):
        with self.assertRaises(ValueError): b.signature(Mock(), 'source;DROP', 'events', 1)
        with self.assertRaisesRegex(ValueError, 'SNAPSHOT_INT64'): b.signature(Mock(), 'source', 'events', True)

    def test_registry_decimal_string_and_json_int64_match_without_float(self):
        self.assertEqual(9007199254740993, b.snapshot_id('9007199254740993'))
        for bad in (True, 1.0, '1e3', '1.0', '0', '9223372036854775808'):
            with self.assertRaisesRegex(ValueError, 'SNAPSHOT_INT64'): b.snapshot_id(bad)

    def test_avro_metadata_preserved_for_null_deflate_snappy(self):
        for codec in ('null', 'deflate', 'snappy'):
            raw = avro(LIST_SCHEMA, [{'manifest_path': MANIFEST, 'manifest_length': 1}], codec)
            schema, meta, rows = b.avro_read(raw)
            s2, m2, r2 = b.avro_read(b.avro_write(schema, meta, rows))
            self.assertEqual(rows, r2); self.assertEqual(meta['spec-version'], m2['spec-version']); self.assertEqual(codec, m2['avro.codec'])

    def test_secret_and_private_path_scan(self):
        for raw in (b'private-' + b'value', b'/Users/' + b'person/notebook', b'https://person:' + b'real-password@internal/'):
            with self.assertRaisesRegex(ValueError, 'SECRET_OR_PRIVATE_PATH'): b.scan_bytes(raw, 'artifact', ('private-value',))

    def test_decoded_parquet_scan_catches_compressed_secret(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        out = io.BytesIO(); pq.write_table(pa.table({'text': ['private-' + 'value']}), out, compression='gzip')
        with self.assertRaisesRegex(ValueError, 'SECRET_OR_PRIVATE_PATH'): b.scan_bytes(out.getvalue(), 'data', ('private-value',))


if __name__ == '__main__': unittest.main()
