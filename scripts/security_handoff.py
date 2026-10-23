"""Read-only SEC-01 artifact audit. Security coverage, never scientific approval."""
import hashlib
import io
import json
from pathlib import Path
import re
import stat
import zipfile

VERSION = "sec-01-handoff-v1"
REQUIRED = {("DAT-02", "pilot_zip"), ("EXP-01", "notebook"),
            ("EXP-01", "result_bundle"), ("MLD-05", "dataset_bundle"),
            ("MLI-03", "result_bundle")}
FILE_LIMIT = 32 * 1024 * 1024
TOTAL_LIMIT = 128 * 1024 * 1024
MAX_FILES = 256


def require(value):
    if not value:
        raise ValueError("SEC_HANDOFF_INVALID_OR_INCOMPLETE")


def safe_name(name):
    require(isinstance(name, str) and name and len(name) <= 512
            and not name.startswith("/") and "\\" not in name and ":" not in name
            and all(part not in {"", ".", ".."} for part in name.split("/")))
    return name


def safe_path(root, name):
    path = root / safe_name(name)
    require(not any(p.is_symlink() for p in (path, *path.parents)))
    require(root in path.resolve().parents)
    return path


def strict_json(data):
    def pairs(items):
        result = {}
        for k, v in items:
            require(k not in result); result[k] = v
        return result
    return json.loads(data, object_pairs_hook=pairs,
                      parse_constant=lambda _: require(False))


def tree_pin(files):
    return hashlib.sha256(json.dumps({name:hashlib.sha256(data).hexdigest()
        for name,data in sorted(files.items())},sort_keys=True,separators=(",", ":")).encode()).hexdigest()


def files_for(path, kind):
    if kind in {"notebook", "pilot_zip"}:
        require(path.is_file() and path.stat().st_size <= FILE_LIMIT)
        data = path.read_bytes()
        if kind == "notebook":
            require(path.suffix == ".ipynb")
            return {path.name:data}, hashlib.sha256(data).hexdigest()
        files = {}
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            entries = archive.infolist()
            require(0 < len(entries) <= MAX_FILES and sum(i.file_size for i in entries) <= TOTAL_LIMIT)
            for entry in entries:
                name = safe_name(entry.filename)
                require(name not in files and not entry.is_dir() and not (entry.flag_bits & 1)
                        and not stat.S_ISLNK(entry.external_attr >> 16)
                        and entry.file_size <= FILE_LIMIT)
                files[name] = archive.read(entry)
        return files, hashlib.sha256(data).hexdigest()
    require(path.is_dir())
    files = {}; total = 0
    # Bounded enumeration, rejects even directory symlinks before descent.
    pending = [path]; directories = 0
    while pending:
        directories += 1; require(directories <= MAX_FILES)
        folder = pending.pop()
        for child in folder.iterdir():
            require(not child.is_symlink())
            if child.is_dir():
                require(len(pending) < MAX_FILES); pending.append(child)
            else:
                require(child.is_file() and child.stat().st_size <= FILE_LIMIT)
                data = child.read_bytes(); total += len(data)
                require(len(files) < MAX_FILES and total <= TOTAL_LIMIT)
                files[safe_name(child.relative_to(path).as_posix())] = data
    require(files)
    return files, tree_pin(files)


def checksum_file(data):
    result = {}
    for line in data.decode("utf-8").splitlines():
        match = re.fullmatch(r"([a-f0-9]{64})  (.+)", line)
        require(match is not None)
        name = safe_name(match[2]); require(name not in result); result[name] = match[1]
    require(result); return result


def validate_layout(files, kind):
    if kind == "notebook":
        notebook = strict_json(next(iter(files.values())))
        require(notebook.get("nbformat") == 4
                and isinstance(notebook.get("cells"), list) and notebook["cells"])
        return
    if kind == "pilot_zip":
        require({"pilot_manifest.json", "coverage_report.json", "checksums.sha256", "mainshocks.parquet"} <= files.keys()
                and any(n.startswith("features") and n.endswith(".parquet") for n in files))
        m = strict_json(files["pilot_manifest.json"])
        require(m.get("artifact_kind") == "DEVELOPMENT_PILOT" and m.get("research_ready") is False)
        checks = checksum_file(files["checksums.sha256"]); excluded = {"checksums.sha256"}
    elif kind == "dataset_bundle":
        require({"dataset_manifest.json", "checksums.sha256", "export-checksums.sha256"} <= files.keys())
        m = strict_json(files["dataset_manifest.json"]); require(m.get("dataset_status") == "EXPORTED")
        checks = checksum_file(files["checksums.sha256"]); excluded = {"checksums.sha256"}
        parts = checksum_file(files["export-checksums.sha256"])
        require(all(n.endswith(".parquet") and n in files and hashlib.sha256(files[n]).hexdigest() == h for n,h in parts.items()))
    else:
        markers = {"_SUCCESS.json", "pilot_result_manifest.json"} & files.keys(); require(len(markers) == 1)
        marker = next(iter(markers)); m = strict_json(files[marker])
        if marker == "_SUCCESS.json":
            require(m.get("status") == "COMPLETED")
        else:
            require(m.get("status") == "DEVELOPMENT_RUN_COMPLETED" and m.get("artifact_kind") == "PILOT_EXPERIMENT_RESULT")
        require({"memberships.parquet", "sequence_summary.parquet", "experiment_config.json",
                 "experiment_metrics.json", "requirements-lock.txt"} <= files.keys())
        checks = m.get("checksums"); excluded = {marker}
        if "checksums.sha256" in files:
            outer = checksum_file(files["checksums.sha256"])
            require(outer == checks and set(outer) == files.keys()-{marker,"checksums.sha256"})
            require(all(hashlib.sha256(files[n]).hexdigest() == v for n,v in outer.items()))
            excluded.add("checksums.sha256")
    require(isinstance(checks, dict) and set(checks) == files.keys()-excluded)
    for name, checksum in checks.items():
        require(re.fullmatch(r"[a-f0-9]{64}", str(checksum)) and hashlib.sha256(files[name]).hexdigest() == checksum)


def strings(value, depth=0):
    require(depth <= 64)
    if isinstance(value, str):
        yield value
    elif isinstance(value, bytes):
        yield value.decode("utf-8",errors="ignore")
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from strings(key,depth+1); yield from strings(item,depth+1)
    elif isinstance(value, (list,tuple)):
        for item in value:
            yield from strings(item,depth+1)


def decoded_text(name, data):
    if name.endswith((".json", ".ipynb")):
        return "\n".join(strings(strict_json(data)))
    if not name.endswith(".parquet"):
        return data.decode("utf-8",errors="strict")
    # Scan decompressed Arrow values AND schema/footer metadata, not just PAR1 bytes.
    import pyarrow.parquet as pq
    parquet = pq.ParquetFile(io.BytesIO(data))
    require(parquet.metadata.num_rows <= 100000 and parquet.metadata.num_columns <= 256)
    require(sum(parquet.metadata.row_group(i).total_byte_size
                for i in range(parquet.metadata.num_row_groups)) <= TOTAL_LIMIT)
    text = [str(parquet.schema_arrow), *strings(parquet.metadata.metadata)]
    total = 0
    for batch in parquet.iter_batches(batch_size=1024):
        total += batch.nbytes; require(total <= TOTAL_LIMIT)
        text.extend(strings(batch.to_pylist()))
    return "\n".join(text)


def audit(root, manifest_path, scan_text, secrets):
    result = {"version":VERSION,"passed":False,"findings":[],"pending":[],"artifacts":[]}
    if manifest_path is None:
        result["pending"].append({"scope":"ml_handoff","reason":"actual_notebook_export_import_acceptance_not_verified"})
        return result
    try:
        root = Path(root).resolve(); manifest_path = Path(manifest_path)
        require(not manifest_path.is_symlink() and manifest_path.is_file() and manifest_path.stat().st_size <= 1048576)
        raw = manifest_path.read_bytes(); manifest = strict_json(raw)
        require(set(manifest) == {"version", "artifacts"} and manifest["version"] == VERSION)
        artifacts = manifest["artifacts"]; require(isinstance(artifacts,list) and 5 <= len(artifacts) <= 32)
        seen = set(); identities = set(); total = 0
        for index, item in enumerate(artifacts):
            require(set(item) == {"task_id", "kind", "path", "sha256"})
            pair = (item["task_id"],item["kind"]); require(pair in REQUIRED)
            if pair == ("MLI-03","result_bundle"):
                marker_required = True
            else:
                marker_required = False
            require(item["path"] not in identities); identities.add(item["path"])
            path = safe_path(root,item["path"]); files, checksum = files_for(path,item["kind"])
            require(re.fullmatch(r"[a-f0-9]{64}",item["sha256"]) and checksum == item["sha256"])
            validate_layout(files,item["kind"])
            require(not marker_required or "_SUCCESS.json" in files)
            seen.add(pair)
            file_proofs = {}
            for name,data in files.items():
                total += len(data); require(total <= TOTAL_LIMIT)
                categories = set(scan_text(name,ml=True))
                categories.update(scan_text(data.decode("utf-8",errors="ignore"),ml=True))
                text = decoded_text(name,data); categories.update(scan_text(text,ml=True))
                if any(s in text or s.encode() in data for s in secrets):
                    categories.add("configured_secret_in_artifact")
                for category in sorted(categories):
                    result["findings"].append({"scope":"ml_handoff","artifact_index":index,"category":category})
                file_proofs[hashlib.sha256(name.encode()).hexdigest()] = hashlib.sha256(data).hexdigest()
            result["artifacts"].append({"task_id":pair[0],"kind":pair[1],"sha256":checksum,
                                        "files":file_proofs,"file_count":len(files)})
        require(seen == REQUIRED)
        result["manifest_sha256"] = hashlib.sha256(raw).hexdigest()
        result["passed"] = not result["findings"]
        result["scope"] = "explicit_local_handoff_bytes_not_remote_account_or_scientific_approval"
    except Exception:
        result["pending"].append({"scope":"ml_handoff","reason":"SEC_HANDOFF_INVALID_OR_INCOMPLETE"})
    return result
