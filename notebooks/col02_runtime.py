"""COL-02 portability: bounded shared sweep, immutable inputs, no official writes."""
from pathlib import Path
import json
import shutil
import subprocess
import sys
import zipfile

import colab_runtime as cr
import col02_portability as cpu
cpu.configure_process()
# Must be after process configuration: parameter_sweep imports NumPy.
from parameter_sweep import SWEEP_CONFIG_FILES, run_bounded_parameter_sweep, verify_sweep_rerun_invariants

VERSION = "col02-colab-portability-v1"
SOURCES = tuple(sorted(set(cr.SOURCES) | {
    "notebooks/parameter_sweep.py", "notebooks/col02_runtime.py", "notebooks/col02_portability.py", "scripts/col02-colab.py",
    "notebooks/col_02_portability_local.ipynb", "notebooks/col_02_colab_acceptance.ipynb",
    *SWEEP_CONFIG_FILES.values(),
}))
KEYS = tuple(SWEEP_CONFIG_FILES)
RUNS = (*KEYS, "RERUN_DBSCAN_C1")
BASELINE_PINS = {
    "handoff": "d6be707111be8d2b1a2ba64434d6c0cacaa67b73c5aae3235f9772403c544333",
    "output": "df5dae61e79cb707e8136af2b46037b27b5818f0abc89bedb59dab218d4b8665",
}
FLAGS = {"allowed_usage": "DEVELOPMENT_ONLY", "research_ready": False,
         "official_exported": False, "scientific_approval": False}


def identity(root):
    files = {n: cr.sha(cr.safe(Path(root) / n)) for n in SOURCES}
    return {"version": VERSION, "files": files, "sha256": cr.digest(files)}


def config_pins(root):
    return {key: cr.sha(Path(root) / name) for key, name in SWEEP_CONFIG_FILES.items()}


def file_map(root):
    root = cr.safe(root)
    paths = list(root.rglob("*"))
    cr.require(not any(p.is_symlink() for p in paths), "COL02_SYMLINK_REJECTED")
    return {p.relative_to(root).as_posix(): cr.sha(p) for p in paths if p.is_file()}


def result_files(prefix):
    return {prefix + "/" + key.lower() + "/" + n for key in RUNS for n in cr.ARTIFACTS}


def algorithm(root, key):
    return cr.load(Path(root) / SWEEP_CONFIG_FILES["DBSCAN_C1" if key == "RERUN_DBSCAN_C1" else key])["algorithm_name"]


def verify_results(root, results, source_sha):
    """Read actual Parquet and bind config contents, not just a matrix status."""
    verified = {}
    ids = []
    for key in RUNS:
        folder = Path(results) / key.lower()
        verified[key] = cr.verify_result(folder, algorithm(root, key), source_sha)
        expected = cr.load(Path(root) / SWEEP_CONFIG_FILES["DBSCAN_C1" if key == "RERUN_DBSCAN_C1" else key])
        cfg = cr.load(folder / "experiment_config.json")
        cr.require(cfg["model_config"] == expected and cfg["model_config_version"] == expected["model_config_version"],
                   "COL02_RESULT_CONFIG_CHANGED")
        ids.append(cfg["experiment_run_id"])
    cr.require(len(set(ids)) == len(RUNS), "COL02_RESULT_RUN_ID_REUSED")
    matrix = cr.load(Path(results) / "sweep_results_matrix.json")
    rows = matrix["configurations"]
    cr.require(matrix["pilot_id"] == cr.PILOT["pilot_id"] and matrix["total_configurations"] == 9
               and [r["config_key"] for r in rows] == list(KEYS), "COL02_MATRIX_CONFIG_SET")
    for row in rows:
        key = row["config_key"]
        expected = cr.load(Path(root) / SWEEP_CONFIG_FILES[key])
        metrics = cr.load(Path(results) / key.lower() / "experiment_metrics.json")["metrics"]
        cr.require(row["config_sha256"] == cr.digest(expected)
                   and row["algorithm_name"] == expected["algorithm_name"]
                   and row["model_config_version"] == expected["model_config_version"], "COL02_MATRIX_CONFIG_CHANGED")
        for name in ("total_candidates", "windows_processed", "successful_windows", "failed_windows",
                     "total_sequence_members", "total_aftershocks", "total_noise"):
            cr.require(row[name] == metrics.get(name, 0), "COL02_MATRIX_METRICS_CHANGED")
        cr.require(row["total_candidates"] == 279 and row["windows_processed"] == 2, "COL02_POPULATION_CHANGED")
    cr.parity(verified["DBSCAN_C1"], verified["RERUN_DBSCAN_C1"])
    return verified


def sweep(root, pilot_zip, work, recipient, runtime):
    """Only real pinned pilot. Local/Colab use exactly the same fitting path."""
    root, work = cr.safe(root), cr.safe(work)
    cr.require(runtime in {"LOCAL_FRESH_KERNEL", "GOOGLE_COLAB"}, "COL02_RUNTIME_KIND")
    cr.require(not work.exists(), "COL02_NEW_WORK_REQUIRED")
    if runtime == "GOOGLE_COLAB": cloud_probe()
    source, env = identity(root), cr.environment(root)
    cr.require(cr.sha(pilot_zip) == cr.PILOT["zip_sha256"], "COL02_REAL_PILOT_REQUIRED")
    numerical_policy = cpu.evidence()
    work.mkdir(parents=True)
    bundle = cr.pilot(root, pilot_zip, work, recipient)
    before = file_map(work / "kit")
    results = work / "results"
    actual = run_bounded_parameter_sweep(bundle, root, results, code_version="source-" + source["sha256"])
    rerun = run_bounded_parameter_sweep(bundle, root, work / "rerun", config_keys=["DBSCAN_C1"],
                                       code_version="source-" + source["sha256"])
    invariants = verify_sweep_rerun_invariants(actual["results"]["DBSCAN_C1"], rerun["results"]["DBSCAN_C1"])
    shutil.copytree(work / "rerun/dbscan_c1", results / "rerun_dbscan_c1")
    verify_results(root, results, source["sha256"])
    # Keep the raw local matrix; ship a separate portable copy without home paths.
    matrix = cr.load(results / "sweep_results_matrix.json")
    for row in matrix["configurations"]: row["output_directory"] = row["config_key"].lower()
    cr.save(work / "portable-matrix.json", matrix)
    cr.require(file_map(work / "kit") == before and identity(root) == source, "COL02_INPUT_OR_SOURCE_MUTATED")
    proof = {"version": VERSION, "status": "SWEEP_AND_RERUN_PASSED", "runtime_kind": runtime,
             "source": source, "config_sha256": config_pins(root), "pilot": cr.PILOT,
             "candidate_rows": 279, "mainshocks": 2, "configurations": 9,
             "parameter_configurations": 8, "random_seed": 42, "environment": env,
             "numerical_runtime_policy": numerical_policy,
             "input_unchanged": True, "rerun": invariants, **FLAGS}
    cr.save(work / "sweep-report.json", proof)
    return proof


def verify_upstream(root, handoff_zip, output_zip, work):
    """Independently re-read the accepted EXP-02 default baseline artifacts."""
    handoff = cr.extract(handoff_zip, Path(work) / "baseline-handoff", BASELINE_PINS["handoff"])
    output = cr.extract(output_zip, Path(work) / "baseline-output", BASELINE_PINS["output"])
    manifest = cr.verify_handoff(handoff)
    report = cr.load(output / "colab-report.json")
    # Bind unchanged shared math/configs; source closure differs for this new task.
    shared = ("notebooks/experiment_harness.py", "notebooks/baseline_models.py",
              "notebooks/requirements-lock.txt", "notebooks/requirements-colab-lock.txt",
              *("config/ml/models/" + n for n in cr.MODELS.values()))
    cr.require(all(cr.sha(Path(root) / n) == manifest["source"]["files"][n] for n in shared),
               "COL02_DEFAULT_BASELINE_REVALIDATION_REQUIRED")
    source_sha = manifest["source"]["sha256"]
    parity = {algo: cr.parity(cr.verify_result(handoff / "baseline" / algo.lower(), algo, source_sha),
                             cr.verify_result(output / "results" / algo.lower(), algo, source_sha)) for algo in cr.MODELS}
    cr.require(report["runtime_kind"] == "GOOGLE_COLAB", "COL02_UPSTREAM_NOT_COLAB")
    return {"pr": 114, "zip_sha256": BASELINE_PINS, "parity": parity,
            "shared_source_sha256": {n: cr.sha(Path(root) / n) for n in shared}}


def validate_gate(handoff, env):
    root = Path(handoff) / "code"
    gate = cr.load(Path(handoff) / "local-gate.json")
    report = cr.load(Path(handoff) / "local-sweep-report.json")
    inspection = gate["inspection"]
    cr.require(gate["version"] == VERSION and gate["source"] == identity(root)
               and gate["report_sha256"] == cr.sha(Path(handoff) / "local-sweep-report.json")
               and inspection["runner_mode"] == "nbclient" and inspection["fresh_kernel_acceptance"] is True
               and inspection["notebook_status"] == "RESTART_RUN_ALL_PASSED"
               and inspection["executed_code_cells"] == 1
               and inspection["notebook_sha256"] == cr.sha(root / "notebooks/col_02_portability_local.ipynb"),
               "COL02_FRESH_LOCAL_GATE_REQUIRED")
    validate_report(root, report, "LOCAL_FRESH_KERNEL")
    cr.require(report["environment"]["direct_packages"] == env["direct_packages"] == cr.pins(root)
               and report["environment"]["requirements_sha256"] == env["requirements_sha256"]
               and report["environment"]["pip_check"] == env["pip_check"] == "PASSED", "COL02_PACKAGE_DRIFT")
    upstream = gate["upstream_defaults"]
    cr.require(upstream["zip_sha256"] == BASELINE_PINS and set(upstream["parity"]) == set(cr.MODELS)
               and all(p["status"] == "PASSED" and p["memberships"] == 279 and p["mainshocks"] == 2
                       for p in upstream["parity"].values())
               and all(cr.sha(root / n) == pin for n, pin in upstream["shared_source_sha256"].items()),
               "COL02_DEFAULT_BASELINE_GATE_REQUIRED")
    verify_results(root, Path(handoff) / "reference", gate["source"]["sha256"])
    return gate


def validate_report(root, report, runtime):
    cpu.validate_evidence(report["numerical_runtime_policy"])
    cr.require(report["version"] == VERSION and report["status"] == "SWEEP_AND_RERUN_PASSED"
               and report["runtime_kind"] == runtime and report["source"] == identity(root)
               and report["config_sha256"] == config_pins(root) and report["pilot"] == cr.PILOT
               and report["candidate_rows"] == 279 and report["mainshocks"] == 2
               and report["configurations"] == 9 and report["parameter_configurations"] == 8
               and report["input_unchanged"] is True and report["random_seed"] == 42
               and report["rerun"]["status"] == "RERUN_INVARIANTS_PASSED"
               and all(report[k] == v for k, v in FLAGS.items()), "COL02_INVALID_SWEEP_REPORT")


def verify_handoff(folder):
    folder = cr.safe(folder)
    m = cr.load(folder / "handoff.json")
    expected = {"code/" + n for n in SOURCES} | result_files("reference") | {
        "pilot.zip", "local-gate.json", "local-sweep-report.json", "reference/sweep_results_matrix.json"}
    actual = file_map(folder)
    cr.require(m["version"] == VERSION and set(m["files"]) == expected
               and set(actual) == expected | {"handoff.json"}
               and m["source"] == identity(folder / "code") and m["pilot"] == cr.PILOT,
               "COL02_HANDOFF_FILE_SET")
    cr.require(all(actual[n] == pin for n, pin in m["files"].items()), "COL02_HANDOFF_BYTES_CHANGED")
    return m


def cloud_probe():
    # Never certify a local flag or a google.colab package installed in the model venv.
    host = subprocess.run([sys._base_executable, "-I", "-c",
        "import google.colab,platform,json; print(json.dumps({'system':platform.system(),'module':google.colab.__file__}))"],
        capture_output=True, text=True, timeout=30)
    cr.require(host.returncode == 0 and Path("/content").is_dir(), "COL02_ACTUAL_COLAB_REQUIRED")
    probe = json.loads(host.stdout)
    cr.require(probe["system"] == "Linux" and probe["module"].startswith(("/usr/local/lib/", "/usr/lib/")),
               "COL02_ACTUAL_COLAB_REQUIRED")
    return probe


def run_cloud(handoff, work, recipient):
    probe = cloud_probe()
    handoff, work = cr.safe(handoff), cr.safe(work)
    m = verify_handoff(handoff)
    root = handoff / "code"
    validate_gate(handoff, cr.environment(root))
    report = sweep(root, handoff / "pilot.zip", work, recipient, "GOOGLE_COLAB")
    local = verify_results(root, handoff / "reference", m["source"]["sha256"])
    cloud = verify_results(root, work / "results", m["source"]["sha256"])
    parity = {key: cr.parity(local[key], cloud[key]) for key in RUNS}
    proof = {"version": VERSION, "status": "COLAB_SWEEP_PARITY_PASSED", "runtime_probe": probe,
             "recipient_self_reported": recipient, "cloud_report_sha256": cr.sha(work / "sweep-report.json"),
             "handoff_manifest_sha256": cr.sha(handoff / "handoff.json"), "parity": parity, **FLAGS}
    cr.save(work / "parity-report.json", proof)
    return proof


def export_output(work, destination):
    work, destination = cr.safe(work), cr.safe(destination)
    cr.require(not destination.exists(), "COL02_NEW_OUTPUT_REQUIRED")
    destination.mkdir(parents=True)
    names = result_files("results") | {"results/sweep_results_matrix.json", "sweep-report.json",
                                      "parity-report.json", "pilot-receipt.json"}
    for name in sorted(names):
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        source = work / "portable-matrix.json" if name == "results/sweep_results_matrix.json" else work / name
        shutil.copyfile(cr.safe(source), target)
    cr.save(destination / "output-checksums.json", file_map(destination))
    return destination


def archive_output(folder, archive):
    cr.require(not Path(archive).exists(), "COL02_NEW_ZIP_REQUIRED")
    with zipfile.ZipFile(archive, "x", compression=zipfile.ZIP_DEFLATED) as z:
        for name in sorted(file_map(folder)): z.write(Path(folder) / name, name)
    # Read every ZIP member back, not just the central directory.
    with zipfile.ZipFile(archive) as z:
        import hashlib
        cr.require({n: hashlib.sha256(z.read(n)).hexdigest() for n in z.namelist()} == file_map(folder),
                   "COL02_ZIP_READBACK_FAILED")
    return {"status": "OUTPUT_ZIP_READBACK_VERIFIED", "sha256": cr.sha(archive), "bytes": Path(archive).stat().st_size}


def readback(handoff, output):
    handoff, output = cr.safe(handoff), cr.safe(output)
    manifest = verify_handoff(handoff)
    root = handoff / "code"
    validate_gate(handoff, cr.environment(root))
    expected = result_files("results") | {"results/sweep_results_matrix.json", "sweep-report.json",
                                         "parity-report.json", "pilot-receipt.json"}
    files = file_map(output)
    cr.require(set(files) == expected | {"output-checksums.json"}
               and cr.load(output / "output-checksums.json") == {n: files[n] for n in expected}, "COL02_OUTPUT_HASHES_CHANGED")
    report = cr.load(output / "sweep-report.json")
    validate_report(root, report, "GOOGLE_COLAB")
    cr.require(report["environment"]["direct_packages"] == cr.pins(root)
               and report["environment"]["requirements_sha256"] == cr.environment(root)["requirements_sha256"]
               and report["environment"]["pip_check"] == "PASSED", "COL02_CLOUD_PACKAGE_DRIFT")
    proof = cr.load(output / "parity-report.json")
    cr.require(proof["version"] == VERSION and proof["status"] == "COLAB_SWEEP_PARITY_PASSED"
               and proof["handoff_manifest_sha256"] == cr.sha(handoff / "handoff.json")
               and proof["cloud_report_sha256"] == files["sweep-report.json"]
               and proof["runtime_probe"]["system"] == "Linux"
               and proof["runtime_probe"]["module"].startswith(("/usr/lib/", "/usr/local/lib/"))
               and all(proof[k] == v for k, v in FLAGS.items()), "COL02_CLOUD_EVIDENCE_CHANGED")
    a = verify_results(root, handoff / "reference", manifest["source"]["sha256"])
    b = verify_results(root, output / "results", manifest["source"]["sha256"])
    parity = {key: cr.parity(a[key], b[key]) for key in RUNS}
    cr.require(proof["parity"] == parity, "COL02_PARITY_REPORT_CHANGED")
    return {"version": VERSION, "status": "INDEPENDENT_READBACK_PASSED", "source_sha256": manifest["source"]["sha256"],
            "config_sha256": config_pins(root), "parity": parity, "files_verified": len(files), **FLAGS}
