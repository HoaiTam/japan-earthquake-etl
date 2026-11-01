"""EXP-01 portability gates; shared models only, no credentials/ETL/official writes."""
from datetime import datetime, timezone
import hashlib
import importlib.metadata as metadata
import importlib.util
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import zipfile

VERSION = "exp01-colab-portability-v1"
RECIPIENTS = {"HoaiTam", "rosy179", "ThanhTris"}
MODELS = {"WINDOW": "window_baseline_v1.json", "DBSCAN": "dbscan_global_dev_v1.json",
          "HDBSCAN_GLOBAL": "hdbscan_global_dev_v1.json"}
PILOT = {"zip_sha256": "f58291b72873e1b26791fef97818fdcf5e2b028a5dfa528885a666d8cdc1b870",
         "handoff_sha256": "9121d6839f9ef8e5646a06cd0bf870fcde50b65da00c03e7b268760237cddc9a",
         "pilot_id": "pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c",
         "feature_sha256": "3baddf613f237d2117248ad9095230ec17e428ea47f73d499623035d3097d2e6",
         "index_sha256": "310b5f232ae34ca4f0a197b0181bff81efe6bee38897b6e151e9b58e8d5a6502"}
SOURCES = ("notebooks/experiment_harness.py", "notebooks/pilot_bundle.py", "notebooks/baseline_models.py",
           "notebooks/adaptive_hdbscan.py", "notebooks/colab_runtime.py", "notebooks/requirements-lock.txt",
           "notebooks/requirements-colab-lock.txt", "notebooks/exp_01_portability_local.ipynb", "notebooks/exp_01_colab_bootstrap.ipynb",
           "scripts/inspect-exp02-notebook.py", "scripts/verify-team-pilot.py", "scripts/exp01-colab.py",
           *("config/ml/models/" + name for name in (*MODELS.values(), "hdbscan_adaptive_dev_v1.json")))
ARTIFACTS = {"memberships.parquet", "sequence_summary.parquet", "experiment_config.json",
             "experiment_metrics.json", "requirements-lock.txt", "pilot_result_manifest.json", "checksums.sha256"}


def require(ok, reason):
    if not ok: raise ValueError(reason)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def sha(path):
    with Path(path).open("rb") as stream: return hashlib.file_digest(stream, "sha256").hexdigest()


def safe(path):
    p = Path(path).absolute()
    require(not any(x.is_symlink() for x in (p, *p.parents)), "EXP01_SYMLINK_REJECTED")
    return p.resolve()


def load(path, limit=2*1024**2):
    p = safe(path); require(p.is_file() and 0 < p.stat().st_size <= limit, "EXP01_JSON_BOUND")
    def pairs(items):
        out = {}
        for key, value in items:
            require(key not in out, "EXP01_DUPLICATE_KEY"); out[key] = value
        return out
    return json.loads(p.read_bytes(), object_pairs_hook=pairs,
                      parse_constant=lambda _: require(False, "EXP01_NONFINITE_JSON"))


def save(path, value):
    p = safe(path)
    with p.open("x", encoding="utf-8") as f:
        json.dump(value, f, sort_keys=True, indent=2, allow_nan=False); f.write("\n")


def module(root, relative, name):
    source = safe(Path(root) / relative)
    spec = importlib.util.spec_from_file_location(name, source)
    result = importlib.util.module_from_spec(spec); spec.loader.exec_module(result); return result


def source_identity(root):
    root = safe(root)
    files = {n: sha(safe(root / n)) for n in SOURCES}
    return {"version": VERSION, "files": files, "sha256": digest(files)}


def pins(root):
    result = {}
    for name in ("requirements-lock.txt", "requirements-colab-lock.txt"):
        for line in (Path(root) / "notebooks" / name).read_text().splitlines():
            text = line.strip()
            if not text or text.startswith("#") or text == "-r requirements-lock.txt": continue
            require(re.fullmatch(r"[A-Za-z0-9_.-]+==[A-Za-z0-9_.+!-]+", text), "EXP01_EXACT_DEPENDENCY_PIN_REQUIRED")
            key, value = text.split("==")
            require(key not in result or result[key] == value, "EXP01_CONFLICTING_PIN")
            result[key] = value
    return result


def environment(root):
    require((3, 11) <= sys.version_info[:2] <= (3, 13), "EXP01_UNSUPPORTED_PYTHON")
    require(sys.prefix != sys.base_prefix, "EXP01_ISOLATED_ENV_REQUIRED")
    expected = pins(root)
    actual = {name: metadata.version(name) for name in expected}
    require(actual == expected, "EXP01_PACKAGE_PIN_MISMATCH")
    checked = subprocess.run([sys.executable, "-m", "pip", "check"], capture_output=True, text=True, timeout=120)
    require(checked.returncode == 0, "EXP01_PIP_CHECK_FAILED")
    listed = subprocess.run([sys.executable, "-m", "pip", "list", "--format=json"], capture_output=True, text=True, check=True, timeout=120)
    return {"python_version": sys.version.split()[0], "direct_packages": actual,
            "installed_packages": json.loads(listed.stdout), "pip_check": "PASSED", "isolated": True,
            "requirements_sha256": {n: sha(Path(root) / "notebooks" / n)
                                    for n in ("requirements-lock.txt", "requirements-colab-lock.txt")}}


def extract(archive, destination, expected_sha, max_bytes=8*1024**2):
    archive, destination = safe(archive), safe(destination)
    require(re.fullmatch(r"[a-f0-9]{64}", expected_sha or "") and sha(archive) == expected_sha, "EXP01_EXTERNAL_ZIP_PIN_MISMATCH")
    require(not destination.exists(), "EXP01_NEW_DIRECTORY_REQUIRED")
    with zipfile.ZipFile(archive) as z:
        rows = z.infolist(); require(0 < len(rows) <= 128 and len(rows) == len({r.filename for r in rows}), "EXP01_ZIP_ENTRY_SET")
        require(sum(r.file_size for r in rows) <= max_bytes, "EXP01_ZIP_SIZE_BOUND")
        for row in rows:
            path = PurePosixPath(row.filename)
            require(not path.is_absolute() and ".." not in path.parts and "\\" not in row.filename
                    and not stat.S_ISLNK(row.external_attr >> 16) and not row.flag_bits & 1
                    and str(path) == row.filename.rstrip("/") and not re.match(r"^[A-Za-z]:", row.filename), "EXP01_UNSAFE_ZIP_MEMBER")
        destination.mkdir(parents=True)
        for row in rows:
            p = destination / row.filename
            if row.is_dir(): p.mkdir(parents=True, exist_ok=True)
            else:
                p.parent.mkdir(parents=True, exist_ok=True)
                with p.open("xb") as out: out.write(z.read(row))
    return destination


def pilot(root, archive, work, recipient):
    require(recipient in RECIPIENTS, "EXP01_RECIPIENT_REQUIRED")
    kit = extract(archive, Path(work) / "kit", PILOT["zip_sha256"], 2*1024**2)
    verifier = module(root, "scripts/verify-team-pilot.py", "exp01_team_verify")
    verifier.verify(kit, PILOT["handoff_sha256"], recipient, Path(work) / "pilot-receipt.json")
    sys.path.insert(0, str(Path(root) / "notebooks"))
    from experiment_harness import load_feature_bundle, MODE_DEVELOPMENT
    bundle = load_feature_bundle(kit / "bundle", mode=MODE_DEVELOPMENT)
    require(bundle["bundle_id"] == PILOT["pilot_id"] and bundle["is_official"] is False
            and bundle["features_table"].num_rows == 279 and bundle["mainshocks_table"].num_rows == 2
            and bundle["checksums"]["features-part-00000.parquet"] == PILOT["feature_sha256"]
            and sha(kit / "bundle/checksums.sha256") == PILOT["index_sha256"], "EXP01_REAL_PILOT_REQUIRED")
    return bundle


def baselines(root, bundle, output, identity):
    from baseline_models import run_baseline_experiment
    from experiment_harness import MODE_DEVELOPMENT
    output = safe(output); require(not output.exists(), "EXP01_NEW_RESULT_DIRECTORY_REQUIRED"); output.mkdir()
    results = {}
    for algo, name in MODELS.items():
        config = load(Path(root) / "config/ml/models" / name)
        results[algo] = run_baseline_experiment(bundle=bundle, algorithm_name=algo, config=config,
            model_config_version=config["model_config_version"], output_dir=output / algo.lower(),
            mode=MODE_DEVELOPMENT, code_version="source-" + identity["sha256"], random_seed=42)
        require(not results[algo]["write_result"]["has_success_marker"], "EXP01_PILOT_OFFICIAL_MARKER")
    return results


def validate_gate(root, gate, smoke, expected_sha, env):
    require(sha(smoke) == expected_sha, "EXP01_LOCAL_REPORT_PIN_CHANGED")
    report = load(smoke)
    require(gate["version"] == VERSION and gate["source"] == source_identity(root)
            and gate["pilot"] == PILOT and gate["local_smoke_sha256"] == expected_sha
            and gate["inspection"]["runner_mode"] == "nbclient"
            and gate["inspection"]["fresh_kernel_acceptance"] is True
            and gate["inspection"]["notebook_status"] == "RESTART_RUN_ALL_PASSED"
            and gate["inspection"]["notebook_sha256"] == sha(Path(root) / "notebooks/exp_01_portability_local.ipynb"), "EXP01_SOURCE_OR_LOCAL_EXECUTION_MISMATCH")
    require(report["report_version"] == "exp-02-local-smoke-v2" and report["runner_mode"] == "nbclient"
            and report["fresh_kernel_acceptance"] is True and report["local_smoke_status"] == "PASSED"
            and report["colab_migration_allowed"] is True and report["pilot_id"] == PILOT["pilot_id"]
            and report["bundle_checksum"] == PILOT["feature_sha256"]
            and report["bundle_fingerprint"]["checksums_index_sha256"] == PILOT["index_sha256"]
            and report["code_version"] == "source-" + gate["source"]["sha256"]
            and set(report["algorithms_evaluated"]) == set(MODELS), "EXP01_LOCAL_GATE_NOT_REAL_PILOT")
    require(gate["environment"]["direct_packages"] == env["direct_packages"] == pins(root)
            and gate["environment"]["requirements_sha256"] == env["requirements_sha256"]
            and gate["environment"]["pip_check"] == env["pip_check"] == "PASSED", "EXP01_LOCAL_PACKAGE_PARITY_FAILED")
    require(all(report["package_versions"].get(n) == env["direct_packages"][n]
                for n in ("numpy", "scipy", "pyarrow", "scikit-learn", "hdbscan", "nbclient", "nbformat")), "EXP01_LOCAL_REPORT_PACKAGES_CHANGED")
    for algo in MODELS:
        require(report["reconciliation"][algo]["total_candidates"] == 279
                and report["reconciliation"][algo]["windows_processed"] == 2, "EXP01_LOCAL_COUNTS_MISMATCH")
    return report


def verify_result(folder, algo, code_sha):
    """Read all files and protected checksums; completion is not inferred from existence."""
    folder = safe(folder)
    require({p.name for p in folder.iterdir()} == ARTIFACTS and not any(p.is_symlink() for p in folder.iterdir()), "EXP01_RESULT_FILE_SET")
    manifest = load(folder / "pilot_result_manifest.json")
    require(manifest["status"] == "DEVELOPMENT_RUN_COMPLETED" and manifest["pilot_id"] == PILOT["pilot_id"]
            and manifest["artifact_kind"] == "PILOT_EXPERIMENT_RESULT"
            and manifest["allowed_usage"] == "DEVELOPMENT_ONLY" and manifest["research_ready"] is False
            and set(manifest["checksums"]) == ARTIFACTS - {"pilot_result_manifest.json", "checksums.sha256"}, "EXP01_INVALID_PILOT_RESULT")
    for n, pin in manifest["checksums"].items(): require(sha(folder / n) == pin, "EXP01_RESULT_HASH_MISMATCH")
    lines = (folder / "checksums.sha256").read_text().splitlines()
    require(all(re.fullmatch(r"[a-f0-9]{64}  [A-Za-z0-9_.-]+", line) for line in lines), "EXP01_RESULT_CHECKSUM_INDEX_INVALID")
    index = {line.split("  ")[1]: line.split("  ")[0] for line in lines}
    require(len(index) == len(lines) and index == manifest["checksums"], "EXP01_RESULT_CHECKSUM_INDEX_MISMATCH")
    config = load(folder / "experiment_config.json")
    require(config["algorithm_name"] == algo and config["code_version"] == "source-" + code_sha
            and config["random_seed"] == 42, "EXP01_RESULT_CONFIG_MISMATCH")
    import pyarrow.parquet as pq
    from experiment_harness import MEMBERSHIP_SCHEMA, SEQUENCE_SUMMARY_SCHEMA
    table = pq.read_table(folder / "memberships.parquet"); summary_table = pq.read_table(folder / "sequence_summary.parquet")
    require(table.schema.remove_metadata().equals(MEMBERSHIP_SCHEMA) and summary_table.schema.remove_metadata().equals(SEQUENCE_SUMMARY_SCHEMA), "EXP01_RESULT_SCHEMA_MISMATCH")
    rows = table.to_pylist(); summaries = summary_table.to_pylist()
    require(len(rows) == 279 and len(summaries) == 2, "EXP01_RESULT_COUNTS_MISMATCH")
    return rows, summaries


def parity(local, cloud):
    """Compare memberships by grain and partition, not arbitrary numeric cluster labels."""
    def keyed(rows, keys):
        out = {tuple(row[k] for k in keys): row for row in rows}
        require(len(out) == len(rows), "EXP01_DUPLICATE_RESULT_GRAIN"); return out
    left, right = (keyed(rows, ("mainshock_event_id", "candidate_event_id")) for rows in (local[0], cloud[0]))
    require(left.keys() == right.keys(), "EXP01_PARITY_GRAIN_MISMATCH")
    ignored = {"experiment_run_id", "created_at_utc", "cluster_id"}
    for key in left:
        for column in set(left[key]) - ignored:
            a, b = left[key][column], right[key][column]
            if type(a) is float and type(b) is float: require(math.isclose(a, b, rel_tol=1e-6, abs_tol=1e-8), "EXP01_PARITY_NUMERIC_MISMATCH")
            else: require(a == b, "EXP01_PARITY_MEMBERSHIP_MISMATCH")
    def groups(rows):
        out = {}
        for key, row in rows.items():
            if row["cluster_id"] is not None and not row["is_noise"]:
                out.setdefault((key[0], row["cluster_id"]), set()).add(key)
        return {frozenset(v) for v in out.values()}
    require(groups(left) == groups(right), "EXP01_PARITY_CLUSTER_PARTITION_MISMATCH")
    a, b = (keyed(rows, ("mainshock_event_id",)) for rows in (local[1], cloud[1]))
    require(a.keys() == b.keys(), "EXP01_PARITY_SUMMARY_GRAIN_MISMATCH")
    for key in a:
        for column in set(a[key]) - {"experiment_run_id", "created_at_utc", "runtime_seconds", "peak_memory_mb"}:
            x, y = a[key][column], b[key][column]
            if type(x) is float and type(y) is float: require(math.isclose(x, y, rel_tol=1e-6, abs_tol=1e-8), "EXP01_PARITY_SUMMARY_NUMERIC_MISMATCH")
            else: require(x == y, "EXP01_PARITY_SUMMARY_MISMATCH")
    return {"status": "PASSED", "memberships": len(left), "mainshocks": len(a), "rtol": 1e-6, "atol": 1e-8}


def verify_handoff(root):
    root = safe(root); m = load(root / "handoff.json")
    expected = {"code/"+n for n in SOURCES} | {"pilot.zip", "local-gate.json", "local-smoke-report.json"} | {
        "baseline/"+algo.lower()+"/"+n for algo in MODELS for n in ARTIFACTS}
    actual = {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}
    require(m["version"] == VERSION and set(m["files"]) == expected and actual == expected | {"handoff.json"}, "EXP01_HANDOFF_FILE_SET")
    for n, pin in m["files"].items(): require(sha(safe(root / n)) == pin, "EXP01_HANDOFF_FILE_CHANGED")
    require(m["source"] == source_identity(root / "code") and m["pilot"] == PILOT, "EXP01_HANDOFF_SOURCE_CHANGED")
    return m


def run_cloud(root, output, recipient):
    require(recipient in RECIPIENTS, "EXP01_RECIPIENT_REQUIRED")
    # A local test may exercise the same runner, but is never cloud acceptance.
    # google.colab stays in the host environment, not the model venv. Probe the
    # base interpreter in isolated mode; never install a fake Colab package/flag.
    host = subprocess.run([sys._base_executable, "-I", "-c",
        "import google.colab,platform,json; print(json.dumps({'system':platform.system(),'module':google.colab.__file__}))"],
        capture_output=True, text=True, timeout=30)
    require(host.returncode == 0 and Path("/content").is_dir(), "EXP01_REAL_COLAB_RUNTIME_REQUIRED")
    probe = json.loads(host.stdout)
    require(probe["system"] == "Linux" and probe["module"].startswith(("/usr/lib/", "/usr/local/lib/")), "EXP01_HOST_RUNTIME_PROBE_FAILED")
    root, output = safe(root), safe(output)
    require(not output.exists() and not output.is_relative_to(root), "EXP01_NEW_OUTPUT_OUTSIDE_INPUT_REQUIRED")
    m = verify_handoff(root); code = root / "code"; env = environment(code)
    gate = load(root / "local-gate.json")
    validate_gate(code, gate, root / "local-smoke-report.json", m["files"]["local-smoke-report.json"], env)
    # Pin baseline artifact bytes/config before fitting or creating output.
    references = {a: verify_result(root / "baseline" / a.lower(), a, m["source"]["sha256"]) for a in MODELS}
    output.mkdir(parents=True); bundle = pilot(code, root / "pilot.zip", output, recipient)
    results = baselines(code, bundle, output / "results", m["source"])
    checks = {a: parity(references[a], verify_result(output / "results" / a.lower(), a, m["source"]["sha256"])) for a in MODELS}
    # Check plotting imports in the same isolated environment used by models.
    os.environ["MPLBACKEND"] = "Agg"; os.environ["MPLCONFIGDIR"] = str(output / "plot-cache")
    import pandas as pd
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    frame = pd.DataFrame([{"algorithm": a, "candidates": results[a]["metrics"]["total_candidates"]} for a in MODELS])
    fig, ax = plt.subplots(); ax.bar(frame.algorithm, frame.candidates); fig.savefig(output / "environment-plot-smoke.png"); plt.close(fig)
    # Do not export the private cache or input. Only the explicit results allowlist.
    report = {"version": VERSION, "status": "COLAB_HARNESS_PASSED", "runtime_kind": "GOOGLE_COLAB",
              "recipient": recipient, "recipient_self_reported": True, "host_runtime_probe": probe, "source": m["source"], "pilot": PILOT,
              "local_report_sha256": m["files"]["local-smoke-report.json"], "environment": env, "parity": checks,
              "created_at_utc": datetime.now(timezone.utc).isoformat(), "research_ready": False,
              "official_exported": False, "scientific_approval": False, "allowed_usage": "DEVELOPMENT_ONLY"}
    save(output / "colab-report.json", report)
    names = {"colab-report.json", "pilot-receipt.json", "environment-plot-smoke.png"} | {
        "results/"+a.lower()+"/"+n for a in MODELS for n in ARTIFACTS}
    from experiment_harness import validate_safe_metadata
    validate_safe_metadata(report)
    save(output / "output-checksums.json", {n: sha(output / n) for n in sorted(names)})
    return report


def output_copy(output, destination, input_root):
    output, destination = safe(output), safe(destination)
    pins = load(output / "output-checksums.json")
    allowed = {"colab-report.json", "pilot-receipt.json", "environment-plot-smoke.png"} | {
        "results/"+a.lower()+"/"+n for a in MODELS for n in ARTIFACTS}
    require(set(pins) == allowed and not destination.exists()
            and not destination.is_relative_to(output) and not destination.is_relative_to(safe(input_root)), "EXP01_COPY_SCOPE_INVALID")
    require(all(sha(safe(output / n)) == pin for n, pin in pins.items()), "EXP01_OUTPUT_CHANGED")
    destination.mkdir(parents=True)
    for n in (*sorted(pins), "output-checksums.json"):
        target = destination / n; target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(output / n, target)
        require(sha(target) == sha(output / n), "EXP01_REMOTE_READBACK_FAILED")
    return {"status": "OUTPUT_COPY_READBACK_VERIFIED", "files": len(pins)+1, "research_ready": False}
