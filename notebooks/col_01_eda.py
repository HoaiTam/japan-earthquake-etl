"""COL-01: read-only pilot inspection; never fit a model or change input."""
from collections import Counter
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import platform
from importlib import metadata
from datetime import datetime, timezone

from pilot_bundle import load_bundle, VECTOR


def inspect(bundle_dir, output_dir, code_version, expected_pilot_id=None):
    for path in (Path(bundle_dir).absolute(), Path(output_dir).absolute()):
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError('COL01_SYMLINK_REJECTED')
    root = Path(bundle_dir).resolve(strict=True)
    out = Path(output_dir).resolve()
    if root == out or root in out.parents or out in root.parents:
        raise ValueError("COL01_OUTPUT_MUST_BE_OUTSIDE_BUNDLE")
    if not code_version or code_version == "unknown":
        raise ValueError("COL01_CODE_VERSION_REQUIRED")
    if out.exists() and any(out.iterdir()):
        raise ValueError("COL01_OUTPUT_MUST_BE_EMPTY")
    data = load_bundle(root)
    before = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()}
    if expected_pilot_id is not None and data['manifest']['pilot_id'] != expected_pilot_id:
        raise ValueError('COL01_PILOT_ID_MISMATCH')
    rows = data['features'].to_pylist()
    manifest, coverage = data['manifest'], data['coverage']
    grain = [(r['mainshock_event_id'], r['candidate_event_id']) for r in rows]
    grouped = []
    for window in coverage['windows']:
        mid = window['mainshock_event_id']
        subset = [r for r in rows if r['mainshock_event_id'] == mid]
        roles = Counter(r['relative_time_role'] for r in subset)
        grouped.append({'mainshock_event_id': mid, 'candidate_count': len(subset),
                        **{role: roles[role] for role in ('PRE', 'MAINSHOCK', 'POST')},
                        'self_only': len(subset) == 1})
    report = {'pilot_id': manifest['pilot_id'], 'dataset_id': manifest['source_dataset_id'],
              'feature_sha256': before['features-part-00000.parquet'],
              'analysis_module_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'checksum_index_sha256': before['checksums.sha256'], 'code_version': code_version,
              'python_version': platform.python_version(), 'executed_at_utc': datetime.now(timezone.utc).isoformat(),
              'packages': {name: metadata.version(name) for name in ('pyarrow', 'matplotlib', 'numpy')},
              'runtime_platform': platform.system(),
              'allowed_usage': 'DEVELOPMENT_ONLY', 'research_ready': False,
              'coverage_status': coverage['status'], 'feature_rows': len(rows),
              'mainshocks': data['mainshocks'].num_rows,
              'unique_candidate_ids': len({r['candidate_event_id'] for r in rows}),
              'unique_mainshock_candidate_pairs': len(set(grain)),
              'duplicate_pairs': len(grain) - len(set(grain)),
              'null_values_by_column': {k: sum(r[k] is None for r in rows) for k in data['features'].column_names},
              'nonfinite_values_by_column': {k: sum(isinstance(r[k], float) and not math.isfinite(r[k]) for r in rows) for k in data['features'].column_names},
              'self_vectors': {r['mainshock_event_id']: {k: r[k] for k in VECTOR} for r in rows if r['is_mainshock']},
              'feature_version': manifest['feature_version'], 'scaling_config_version': manifest['scaling_config_version'],
              'scaling': json.loads(manifest['scaling_config_json']), 'coverage_windows': coverage['windows'],
              'coverage_meaning': coverage.get('meaning', 'Temporal input-scope only; not catalog completeness'),
              'windows': grouped, 'units': {'dx_km': 'km', 'dy_km': 'km', 'dz_km': 'km', 'delta_time_hours': 'hour', 'scaled_vectors': 'dimensionless'},
              'limitation': 'PARTIAL, development only; overlapping windows are not independent events; no inference to full Japan or full period.'}
    # Import before writing anything so an unavailable plot dependency fails early.
    # Colab exports matplotlib_inline to subprocesses, but this isolated
    # PNG-only environment intentionally does not install an inline backend.
    # Override BEFORE import (matplotlib.use after import is too late).
    inherited_backend = os.environ.get('MPLBACKEND')
    os.environ['MPLBACKEND'] = 'Agg'
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    finally:
        if inherited_backend is None:
            os.environ.pop('MPLBACKEND', None)
        else:
            os.environ['MPLBACKEND'] = inherited_backend
    out.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots()
    ax.bar([str(i + 1) for i in range(len(grouped))], [x['candidate_count'] for x in grouped])
    ax.set(xlabel='Mainshock window (ordered as windows.csv)', ylabel='Candidate rows', title='Development pilot — partial coverage')
    for i, window in enumerate(grouped):
        ax.text(i, window['candidate_count'], str(window['candidate_count']), ha='center', va='bottom')
    fig.text(0.01, 0.01, manifest['pilot_id'][:24] + '… | ' + before['features-part-00000.parquet'][:12], fontsize=7)
    fig.tight_layout()
    fig.savefig(out / 'window-counts.png')
    plt.close(fig)
    after = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir()}
    if before != after:
        raise ValueError('COL01_INPUT_CHANGED')
    report['input_unchanged'] = True
    report['input_sha256'] = before
    report['plot_sha256'] = hashlib.sha256((out / 'window-counts.png').read_bytes()).hexdigest()
    (out / 'pilot-eda.json').write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + '\n', encoding='utf-8')
    with (out / 'windows.csv').open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=['mainshock_event_id', 'candidate_count', 'PRE', 'MAINSHOCK', 'POST', 'self_only'])
        writer.writeheader()
        writer.writerows(grouped)
    return report
