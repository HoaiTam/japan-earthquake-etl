#!/usr/bin/env python3
"""OPS-01 explicit backup / offline verification / isolated restore. No auto-retry."""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
import sys
import uuid

from ops_backup import (Remote, VERSION, capture, decode, digest, encode, identifier,
                        private_write, read_bundle, require, restore, safe, scan_bytes)
from ops_backup import verify_artifacts, lineage_readback

ROOT = Path(__file__).resolve().parents[1]


def folder(path):
    p = safe(path)
    require(p.is_relative_to(ROOT / 'staging') and p != ROOT / 'staging' and not p.exists(), 'NEW_STAGING_FOLDER_REQUIRED')
    require(shutil.disk_usage(ROOT).free >= 4 * 1024**3, 'HOST_DISK_RESERVE_4GIB')
    p.mkdir(mode=0o700, parents=True)
    return p


def scan(manifest, graph, artifacts, deployment, require_configured=False):
    # Exact configured values stay in memory, never in the archive/report/argv.
    import importlib.util
    spec = importlib.util.spec_from_file_location('ops01_scan', ROOT / 'scripts/audit-security.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    secrets = module.configured_secrets(safe(deployment))
    require(not require_configured or secrets, 'DEPLOYMENT_SECRET_SCAN_COVERAGE_REQUIRED')
    for entry in graph.objects.values(): scan_bytes(entry['data'], entry['role'], secrets)
    for name, raw in artifacts.items(): scan_bytes(raw, 'data' if name.endswith('.parquet') else 'artifact', secrets)
    scan_bytes(encode(manifest), 'artifact', secrets)
    return {'decoded_patterns': 'PASSED', 'configured_secrets': 'CHECKED' if secrets else 'NOT_AVAILABLE_OFFLINE'}


def config(path):
    raw = safe(path).read_bytes(); c = decode(raw)
    require(set(c) == {'version', 'gold_namespace', 'ml_namespace', 'gold_tables', 'ml_tables', 'evidence_files', 'result_run', 'bronze_manifests'}
            and c['version'] == VERSION, 'CONFIG_KEYS')
    require(re.fullmatch(r'mli03-qa-[a-f0-9]{32}', c['result_run']), 'RESULT_RUN_ID')
    require(0 < len(c['bronze_manifests']) <= 8, 'BRONZE_MANIFEST_BOUND')
    for pin in c['bronze_manifests']:
        require(set(pin) == {'name', 'uri', 'sha256'} and re.fullmatch(r'[a-z0-9_-]+\.json', pin['name'])
                and re.fullmatch(r'[a-f0-9]{64}', pin['sha256']) and re.fullmatch(r's3://[a-z0-9.-]+/bronze/[^?#%\\]+/manifest\.json', pin['uri']), 'EXACT_BRONZE_MANIFEST_PIN')
    require(len({p['name'] for p in c['bronze_manifests']}) == len(c['bronze_manifests']), 'DUPLICATE_BRONZE_NAME')
    for k in ('gold_namespace', 'ml_namespace'): identifier(c[k])
    require(set(c['gold_tables']) == {'event_current', 'event_source_bridge', 'dim_date', 'dim_region', 'dim_depth_band', 'dim_magnitude_band', 'publication_status'}, 'GOLD_TABLE_SET')
    require(set(c['ml_tables']) == {'candidate_event', 'mainshock_event', 'dataset_registry', 'experiment_run', 'sequence_membership', 'sequence_summary'}, 'ML_TABLE_SET')
    require(len(c['gold_tables']) == 7 and len(c['ml_tables']) == 6 and len(c['evidence_files']) <= 8, 'TABLE_COUNT')
    sources = [(c[k + '_namespace'], identifier(t)) for k in ('gold', 'ml') for t in c[k + '_tables']]
    artifacts = {'selection.json': raw}
    for i, value in enumerate(c['evidence_files']):
        p = safe(ROOT / value)
        require(p.is_relative_to(ROOT / 'docs/evidence') and p.suffix == '.json' and p.stat().st_size <= 1024**2, 'EXACT_EVIDENCE_PATH')
        artifacts['evidence-' + str(i) + '.json'] = p.read_bytes()
    return sources, artifacts, c['result_run'], c['bronze_manifests']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('backup', 'verify', 'restore'))
    parser.add_argument('--config'); parser.add_argument('--bundle'); parser.add_argument('--sha256')
    parser.add_argument('--output'); parser.add_argument('--deployment-root', default=str(ROOT))
    parser.add_argument('--project', default='japan-earthquake-etl')
    parser.add_argument('--context', choices=('desktop-linux', 'default'), default='desktop-linux')
    args = parser.parse_args(); out = None
    try:
        if args.mode == 'verify':
            m, g, a = read_bundle(args.bundle, args.sha256); scan(m, g, a, args.deployment_root)
            verify_artifacts(m, a)
            print('event=ops01_offline_verified tables=' + str(len(m['tables'])) + ' objects=' + str(len(g.objects)))
            return 0
        remote = Remote(args.project, args.context)
        before = remote.services(); remote.call('idle')
        out = folder(args.output)
        remote.private_root = out
        if args.mode == 'backup':
            sources, artifacts, result_run, bronze = config(args.config)
            for pin in bronze:
                raw = remote.get(pin['uri']); require(digest(raw) == pin['sha256'], 'BRONZE_MANIFEST_SHA_CHANGED')
                artifacts['bronze/' + pin['name']] = raw
            for sub, names in {'valid': ('_SUCCESS.json','experiment_config.json','experiment_metrics.json','memberships.parquet','requirements-lock.txt','sequence_summary.parquet'),
                               'registry': ('dataset.json','export.json')}.items():
                for name in names:
                    response = remote.call('artifact', path='/opt/pipeline/staging/mli-03-qa/' + result_run + '/' + sub + '/' + name)
                    import base64
                    artifacts[sub + '/' + name] = base64.b64decode(response['data'], validate=True)
            artifacts['airflow-schema.sql'] = remote.pg_backup()
            artifacts['airflow-schema-columns.txt'] = remote.pg_signature()
            # Only object identifiers, not Airflow rows/connections/variables/XCom.
            sources_sha = digest(encode(sources))
            private_write(out / 'intent.json', encode({'version': VERSION, 'mode': 'backup', 'sources_sha256': sources_sha}))
            bundle = out / 'backup.zip'
            sha = capture(remote, sources, bundle, artifacts)
            m, g, a = read_bundle(bundle, sha); scan_result = scan(m, g, a, args.deployment_root, require_configured=True)
            binding = verify_artifacts(m, a)
            private_write(out / 'backup.SHA256', (sha + '\n').encode())
            result = {'version': VERSION, 'status': 'BACKUP_VERIFIED', 'bundle_sha256': sha,
                      'tables': len(m['tables']), 'objects': len(g.objects), 'uncompressed_bytes': g.total,
                      'zip_bytes': bundle.stat().st_size, 'secret_scan': 'PASSED', 'airflow_scope': 'schema_only_no_rows',
                      'secret_scan_coverage': scan_result,
                      'artifact_lineage': binding,
                      'full_disaster_recovery': False}
        else:
            m, g, a = read_bundle(args.bundle, args.sha256); scan_result = scan(m, g, a, args.deployment_root, require_configured=True)
            binding = verify_artifacts(m, a)
            require({'airflow-schema.sql', 'airflow-schema-columns.txt', 'selection.json'} <= set(a), 'RESTORE_ARTIFACTS_REQUIRED')
            require(remote.pg_signature() == a['airflow-schema-columns.txt'], 'AIRFLOW_SCHEMA_DRIFT')
            run = uuid.uuid4().hex
            private_write(out / 'operation.json', encode({'version': VERSION, 'run': run, 'bundle_sha256': args.sha256}))
            print('event=ops01_restore_started run=' + run, flush=True)
            result = restore(remote, args.bundle, args.sha256, run, out)
            intent = decode((out / 'restore-intent.json').read_bytes())
            result['ml_lineage'] = lineage_readback(remote, intent['tables'], binding)
            database = remote.pg_restore(a['airflow-schema.sql'], run)
            require(remote.pg_signature(database) == a['airflow-schema-columns.txt'], 'AIRFLOW_RESTORED_SCHEMA_CHANGED')
            result.update({'version': VERSION, 'status': 'RESTORE_VERIFIED', 'bundle_sha256': args.sha256,
                           'airflow_database': database, 'airflow_schema_verified': True, 'full_disaster_recovery': False,
                           'secret_scan_coverage': scan_result,
                           'secret_scan': 'PASSED', 'scope': 'accepted_component_qa_not_official_scientific_run'})
        after = remote.services(); remote.call('idle')
        require(before == after, 'RUNTIME_CHANGED')
        result['services_unchanged'] = True; result['completed_at_utc'] = datetime.now(timezone.utc).isoformat()
        private_write(out / 'receipt.json', encode(result))
        print('event=ops01_' + result['status'].lower() + ' receipt=' + str(out / 'receipt.json'))
        return 0
    except Exception as exc:
        reason = str(exc) if isinstance(exc, ValueError) and re.fullmatch(r'OPS01_[A-Z0-9_]+', str(exc)) else 'OPS01_UNEXPECTED_FAILURE'
        if out is not None and out.exists():
            private_write(out / 'failed.json', encode({'status': 'FAILED_RETAINED_NO_RETRY', 'reason': reason}))
        print('event=ops01_failed reason=' + reason + ' checkpoint_retained=true', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
