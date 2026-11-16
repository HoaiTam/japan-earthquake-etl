#!/usr/bin/env python3
"""COL-01 small handoff pack and identical local/cloud inspection entry point."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'notebooks'))
from col_01_eda import inspect

PILOT = 'pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c'
FEATURE_SHA = '3baddf613f237d2117248ad9095230ec17e428ea47f73d499623035d3097d2e6'
INDEX_SHA = '310b5f232ae34ca4f0a197b0181bff81efe6bee38897b6e151e9b58e8d5a6502'
SOURCES = ('notebooks/col_01_eda.py', 'notebooks/pilot_bundle.py',
           'notebooks/requirements-col01-lock.txt', 'scripts/col01-eda.py')
BUNDLE_FILES = ('features-part-00000.parquet', 'mainshocks.parquet',
                'pilot_manifest.json', 'coverage_report.json', 'checksums.sha256')


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def pack(bundle, archive, code_version):
    from pilot_bundle import load_bundle
    bundle = Path(bundle)
    data = load_bundle(bundle)
    if data['manifest']['pilot_id'] != PILOT or sha(bundle / BUNDLE_FILES[0]) != FEATURE_SHA or sha(bundle / 'checksums.sha256') != INDEX_SHA:
        raise ValueError('COL01_TRUSTED_PILOT_REQUIRED')
    if Path(archive).resolve().is_relative_to(bundle.resolve()):
        raise ValueError('COL01_PACK_OUTSIDE_BUNDLE_REQUIRED')
    files = {name: ROOT / name for name in SOURCES}
    files.update({'bundle/' + name: bundle / name for name in BUNDLE_FILES})
    manifest = {'version': 'col01-handoff-v1', 'pilot_id': PILOT,
                'code_version': code_version, 'files': {name: sha(path) for name, path in files.items()}}
    archive = Path(archive)
    archive.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive, 'x', compression=zipfile.ZIP_DEFLATED) as z:
        for name, path in sorted(files.items()):
            z.write(path, name)
        z.writestr('handoff.json', json.dumps(manifest, sort_keys=True, indent=2))
    return {'zip_sha256': sha(archive), 'bytes': archive.stat().st_size, 'manifest': manifest}


def run(bundle, output, code_version, runtime):
    bundle = Path(bundle)
    if sha(bundle / 'features-part-00000.parquet') != FEATURE_SHA or sha(bundle / 'checksums.sha256') != INDEX_SHA:
        raise ValueError('COL01_TRUSTED_PILOT_REQUIRED')
    report = inspect(bundle, output, code_version, expected_pilot_id=PILOT)
    if (report['feature_rows'], report['mainshocks']) != (279, 2):
        raise ValueError('COL01_REAL_PILOT_COUNTS_REQUIRED')
    # LOCAL is the CLI default. Cloud identity is separately evidenced by the
    # executed notebook importing google.colab and its downloaded outputs.
    report['runtime_kind'] = runtime
    report['loader_module_sha256'] = sha(ROOT / 'notebooks/pilot_bundle.py')
    report['runner_sha256'] = sha(Path(__file__))
    (Path(output) / 'pilot-eda.json').write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + '\n', encoding='utf-8')
    pins = {name: sha(Path(output) / name) for name in ('pilot-eda.json', 'windows.csv', 'window-counts.png')}
    (Path(output) / 'output-checksums.json').write_text(json.dumps(pins, indent=2) + '\n')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('pack', 'run'))
    parser.add_argument('--bundle', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--code-version', required=True)
    parser.add_argument('--runtime', choices=('LOCAL', 'GOOGLE_COLAB'), default='LOCAL')
    args = parser.parse_args()
    result = pack(args.bundle, args.output, args.code_version) if args.mode == 'pack' else run(args.bundle, args.output, args.code_version, args.runtime)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
