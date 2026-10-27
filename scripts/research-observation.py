#!/usr/bin/env python3
"""Host-only content-addressed observations. Never execute artifacts or mutate the lake."""
import argparse
from contextlib import contextmanager
from functools import wraps
import fnmatch
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import select
import shutil
import stat
import subprocess
import sys
import time
import uuid

VERSION = "dat03-host-observation-store-v1"
STORE = "host-observations-v1"
MARKER = "observation.json"
MAX_FILE = 256 * 1024**2
MAX_FILES = 50000
MAX_TOTAL = 4 * 1024**3
MAX_MANIFEST = 16 * 1024**2


def require(ok, reason):
    if not ok:
        raise RuntimeError("OBSERVATION_" + reason)


def safe(path):
    path = Path(path).absolute()
    require(not any(p.is_symlink() for p in (path, *path.parents)), "SYMLINK_REJECTED")
    return path


def relative(name):
    require(isinstance(name, str) and 0 < len(name) <= 1024 and "\\" not in name
            and not name.startswith("/") and str(PurePosixPath(name)) == name
            and all(p not in ("", ".", "..") for p in name.split("/")), "UNSAFE_MEMBER")
    return name


def sha(path):
    with safe(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def load(path):
    path = safe(path)
    require(path.is_file() and 0 < path.stat().st_size <= MAX_MANIFEST, "METADATA_BOUND")
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "DUPLICATE_JSON_KEY")
            result[key] = value
        return result
    def bad(value):
        raise RuntimeError("OBSERVATION_NONFINITE_JSON")
    return json.loads(path.read_bytes(), object_pairs_hook=pairs, parse_constant=bad)


def sync_directory(path):
    descriptor = os.open(safe(path), os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def sync_parents(path, catalog):
    path = safe(path)
    require(path == catalog or path.is_relative_to(catalog), "SYNC_OUTSIDE_CATALOG")
    for directory in (path, *path.parents):
        sync_directory(directory)
        if directory == catalog:
            break


def persist(path, value):
    path = safe(path)
    with path.open("x", encoding="utf-8") as stream:
        os.chmod(path, 0o600)
        json.dump(value, stream, sort_keys=True, indent=2, allow_nan=False)
        stream.write("\n"); stream.flush(); os.fsync(stream.fileno())
    require(load(path) == value, "PERSISTED_METADATA_CHANGED")
    sync_directory(path.parent)


def catalog_of(root):
    root = safe(root)
    for candidate in (root, *root.parents):
        if candidate.parent.name == "dat03-catalog" and re.fullmatch(r"[a-f0-9]{32}", candidate.name):
            return candidate
    raise RuntimeError("OBSERVATION_EXACT_CATALOG_REQUIRED")


def inventory(root):
    root = safe(root); files = {}; directories = []
    require(root.is_dir(), "DIRECTORY_REQUIRED")
    for path in sorted(root.rglob("*")):
        safe(path); name = relative(path.relative_to(root).as_posix()); info = path.stat()
        if stat.S_ISDIR(info.st_mode):
            directories.append(name)
        else:
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= MAX_FILE,
                    "REGULAR_BOUNDED_FILE_REQUIRED")
            files[name] = {"bytes": info.st_size, "sha256": sha(path), "mode": stat.S_IMODE(info.st_mode)}
    validate_inventory(files, directories)
    return {"files": files, "directories": directories}


def validate_inventory(files, directories):
    require(isinstance(files, dict) and isinstance(directories, list)
            and 0 < len(files) <= MAX_FILES and len(directories) <= MAX_FILES, "INVENTORY_BOUND")
    require(len(directories) == len(set(directories)), "DUPLICATE_DIRECTORY")
    for name in directories:
        relative(name); require(name not in files, "FILE_DIRECTORY_COLLISION")
    total = 0
    for name, meta in files.items():
        relative(name)
        require(isinstance(meta, dict) and set(meta) == {"bytes", "sha256", "mode"}
                and type(meta["bytes"]) is int and 0 <= meta["bytes"] <= MAX_FILE
                and isinstance(meta["sha256"], str) and re.fullmatch(r"[a-f0-9]{64}", meta["sha256"])
                and type(meta["mode"]) is int and 0 <= meta["mode"] <= 0o777, "INVALID_FILE_METADATA")
        total += meta["bytes"]
        require(all(parent.as_posix() in directories for parent in PurePosixPath(name).parents
                    if parent.as_posix() != "."), "MISSING_PARENT_DIRECTORY")
    require(total <= MAX_TOTAL, "TOTAL_BYTES_BOUND")


def blob(catalog, digest):
    require(isinstance(digest, str) and re.fullmatch(r"[a-f0-9]{64}", digest), "INVALID_BLOB_SHA")
    return safe(catalog / STORE / "blobs" / digest[:2] / digest)


def verify_file(path, meta):
    path = safe(path)
    require(path.is_file() and path.stat().st_nlink == 1 and path.stat().st_size == meta["bytes"]
            and sha(path) == meta["sha256"], "FILE_HASH_OR_SIZE_CHANGED")
    return path


def put(catalog, meta, stream):
    target = blob(catalog, meta["sha256"])
    require(not target.exists(), "EXISTING_BLOB_MUST_BE_REUSED")
    target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    temp = safe(target.parent / ("pending-" + uuid.uuid4().hex))
    # A failure retains the exact pending object for diagnosis; never promotes it.
    remaining = meta["bytes"]; digest = hashlib.sha256()
    with temp.open("xb") as out:
        os.chmod(temp, 0o600)
        while remaining:
            block = stream.read(min(1024**2, remaining))
            require(block, "TRUNCATED_TRANSFER")
            out.write(block); digest.update(block); remaining -= len(block)
        out.flush(); os.fsync(out.fileno())
    require(digest.hexdigest() == meta["sha256"], "TRANSFER_HASH_CHANGED")
    require(not target.exists(), "CONCURRENT_BLOB_WRITE")
    os.chmod(temp, 0o400); os.rename(temp, target); sync_parents(target.parent, catalog)
    return verify_file(target, meta)


def import_blob(catalog, source, meta):
    """Independent reflink when available; no hardlinks and no rewrite of source."""
    source = verify_file(source, meta); target = blob(catalog, meta["sha256"])
    target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    require(not target.exists(), "CONCURRENT_BLOB_WRITE")
    if sys.platform == "darwin" and source.stat().st_dev == target.parent.stat().st_dev:
        temp = safe(target.parent / ("pending-" + uuid.uuid4().hex))
        checked = subprocess.run(["/bin/cp", "-c", str(source), str(temp)], capture_output=True, timeout=60)
        require(checked.returncode == 0, "LOCAL_REFLINK_FAILED_NO_AUTOMATIC_FALLBACK")
        require(temp.stat().st_ino != source.stat().st_ino, "INDEPENDENT_BLOB_REQUIRED")
        verify_file(temp, meta); os.chmod(temp, 0o400)
        with temp.open("rb") as stream:
            os.fsync(stream.fileno())
        require(not target.exists(), "CONCURRENT_BLOB_WRITE"); os.rename(temp, target); sync_parents(target.parent, catalog)
        return verify_file(target, meta)
    require(shutil.disk_usage(target.parent).free >= meta["bytes"] + 1024**3, "IMPORT_DISK_RESERVE")
    with source.open("rb") as stream:
        return put(catalog, meta, stream)


@contextmanager
def exclusive(catalog):
    root = safe(catalog / STORE); root.mkdir(mode=0o700, exist_ok=True)
    lock = root / "writer.lock"; identity = {"pid": os.getpid(), "token": uuid.uuid4().hex}
    persist(lock, identity)  # O_EXCL: never take over a stale/foreign operation.
    try:
        yield
    except BaseException:
        raise  # Preserve lock and partial objects/descriptors for explicit diagnosis.
    else:
        require(load(lock) == identity, "LOCK_OWNER_CHANGED"); lock.unlink()


def locked(function):
    @wraps(function)
    def wrapped(catalog, *args, **kwargs):
        with exclusive(safe(catalog)):
            return function(safe(catalog), *args, **kwargs)
    return wrapped


class View:
    """Read-only virtual tree; file access resolves SHA-pinned bytes, never links."""
    def __init__(self, root, catalog, manifest, name=""):
        self.root = root; self.catalog = catalog; self.manifest = manifest; self.relative = name

    def __truediv__(self, name):
        name = str(name)
        joined = self.relative + "/" + name if self.relative else name
        return View(self.root, self.catalog, self.manifest, relative(joined))

    @property
    def name(self):
        return PurePosixPath(self.relative).name if self.relative else self.root.name

    @property
    def parent(self):
        name = str(PurePosixPath(self.relative).parent)
        return View(self.root, self.catalog, self.manifest, "" if name == "." else name)

    def __eq__(self, other):
        return isinstance(other, View) and self.root == other.root and self.relative == other.relative

    def __fspath__(self):
        if self.relative in self.manifest["files"]:
            meta = self.manifest["files"][self.relative]
            target = self.catalog / self.relative if self.relative in self.manifest["catalog_inputs"] else blob(self.catalog, meta["sha256"])
            return str(verify_file(target, meta))
        require(self.is_dir(), "MISSING_ARTIFACT")
        return str(self.root / self.relative)

    def __str__(self):
        return str(self.root / self.relative)

    def is_file(self):
        return self.relative in self.manifest["files"]

    def is_dir(self):
        return not self.relative or self.relative in self.manifest["directories"]

    def exists(self):
        return self.is_file() or self.is_dir()

    def open(self, mode="r", *args, **kwargs):
        require(mode in ("r", "rb"), "READ_ONLY_VIEW")
        return Path(os.fspath(self)).open(mode, *args, **kwargs)

    def read_bytes(self):
        with self.open("rb") as stream:
            return stream.read()

    def read_text(self, **kwargs):
        with self.open("r", **kwargs) as stream:
            return stream.read()

    def stat(self):
        return Path(os.fspath(self)).stat()

    def glob(self, pattern):
        prefix = self.relative + "/" if self.relative else ""
        for name in sorted(set(self.manifest["files"]) | set(self.manifest["directories"])):
            if name.startswith(prefix) and fnmatch.fnmatchcase(name[len(prefix):], pattern):
                yield View(self.root, self.catalog, self.manifest, name)

    def iterdir(self):
        prefix = self.relative + "/" if self.relative else ""
        for name in sorted(set(self.manifest["files"]) | set(self.manifest["directories"])):
            if name.startswith(prefix) and "/" not in name[len(prefix):]:
                yield View(self.root, self.catalog, self.manifest, name)


def view(root, frozen):
    root = safe(root); catalog = catalog_of(root); manifest = load(root / MARKER)
    require(set(manifest) == {"version", "catalog_request_sha256", "catalog_inputs", "files", "directories"}
            and manifest["version"] == VERSION and manifest["catalog_request_sha256"] == sha(catalog / "request.json")
            and manifest["catalog_inputs"] == sorted(frozen), "MANIFEST_OR_REQUEST_CHANGED")
    validate_inventory(manifest["files"], manifest["directories"])
    require(set(frozen) <= set(manifest["files"]), "FROZEN_INPUT_MISSING")
    result = View(root, catalog, manifest)
    # Full snapshot integrity, not just the files a particular reporter happens to read.
    for name in manifest["files"]:
        os.fspath(result / name)
    return result


def materialize(root, target, frozen):
    source = view(root, frozen); target = safe(target)
    require(not target.exists() and not target.is_relative_to(safe(root)), "NEW_RESTORE_TARGET_REQUIRED")
    total = sum(meta["bytes"] for meta in source.manifest["files"].values())
    require(target.parent.is_dir() and shutil.disk_usage(target.parent).free >= total + 1024**3, "RESTORE_DISK_RESERVE")
    target.mkdir(mode=0o700)
    for name in sorted(source.manifest["directories"], key=lambda n: len(PurePosixPath(n).parts)):
        safe(target / name).mkdir(mode=0o700)
    for name, meta in source.manifest["files"].items():
        destination = safe(target / name)
        if sys.platform == "darwin" and Path(os.fspath(source / name)).stat().st_dev == target.stat().st_dev:
            copied = subprocess.run(["/bin/cp", "-c", os.fspath(source / name), str(destination)], capture_output=True, timeout=60)
            require(copied.returncode == 0, "RESTORE_REFLINK_FAILED")
            require(destination.stat().st_ino != Path(os.fspath(source / name)).stat().st_ino, "INDEPENDENT_RESTORE_REQUIRED")
            os.chmod(destination, meta["mode"])
        else:
            with (source / name).open("rb") as inp, destination.open("xb") as out:
                os.chmod(out.name, meta["mode"]); shutil.copyfileobj(inp, out, 1024**2)
                out.flush(); os.fsync(out.fileno())
        verify_file(target / name, meta)
    require(inventory(target) == {key: source.manifest[key] for key in ("files", "directories")}, "RESTORE_CHANGED")
    return target


def seal(catalog, target, listing, frozen):
    validate_inventory(listing["files"], listing["directories"])
    require(set(frozen) <= set(listing["files"]), "FROZEN_INPUT_MISSING")
    manifest = {"version": VERSION, "catalog_request_sha256": sha(catalog / "request.json"),
                "catalog_inputs": sorted(frozen), **listing}
    safe(target).mkdir(mode=0o700)
    persist(target / MARKER, manifest)
    sync_parents(target, catalog)
    return view(target, frozen)


def import_local(catalog, source, target, frozen):
    catalog, source, target = map(safe, (catalog, source, target))
    require(catalog_of(source) == catalog == catalog_of(target) and source != target
            and source.parent == catalog and not (source / MARKER).exists(), "EXACT_LEGACY_SOURCE_REQUIRED")
    listing = inventory(source)
    for name, meta in listing["files"].items():
        if name in frozen:
            verify_file(catalog / name, meta)
        elif blob(catalog, meta["sha256"]).exists():
            verify_file(blob(catalog, meta["sha256"]), meta)
        else:
            import_blob(catalog, source / name, meta)
    require(inventory(source) == listing, "LEGACY_SOURCE_CHANGED")
    return seal(catalog, target, listing, frozen)


class DeadlineReader:
    def __init__(self, stream, seconds=120):
        self.stream = stream; self.deadline = time.monotonic() + seconds

    def read(self, size):
        remaining = self.deadline - time.monotonic()
        require(remaining > 0 and select.select([self.stream], [], [], remaining)[0], "TRANSFER_TIMEOUT")
        return os.read(self.stream.fileno(), size)


@locked
def compact_legacy(catalog, source, frozen, validate):
    """Explicit verified cutover, retaining every byte in CAS/catalog and its manifest."""
    before = validate(source)
    require(before["status"] == "ACCEPTED_CATALOG_CHECKPOINT", "ONLY_ACCEPTED_HISTORY_CAN_BE_COMPACTED")
    (catalog / STORE / "snapshots").mkdir(parents=True, mode=0o700, exist_ok=True)
    target = catalog / STORE / "snapshots" / uuid.uuid4().hex
    result = import_local(catalog, source, target, frozen)
    require(validate(target) == before, "IMPORTED_ACCEPTANCE_CHANGED")
    listing = {key: result.manifest[key] for key in ("files", "directories")}
    require(inventory(source) == listing, "SOURCE_CHANGED_BEFORE_CUTOVER")
    receipt = {"version": VERSION, "source_name": source.name, "recovery_name": target.name,
               "manifest_sha256": sha(target / MARKER), "accepted_progress": before,
               "source_removal_authorized": True, "all_bytes_verified": True}
    receipt_path = safe(catalog / STORE / ("migration-" + uuid.uuid4().hex + ".json"))
    persist(receipt_path, receipt)
    # Publish the recoverable descriptor BEFORE unlinking any original file.
    # Interrupted pruning leaves a valid view plus redundant originals, never lost proof.
    persist(source / MARKER, result.manifest)
    require(validate(source) == before, "CUTOVER_ACCEPTANCE_CHANGED")
    for name, meta in listing["files"].items():
        verify_file(source / name, meta).unlink()
    for name in sorted(listing["directories"], key=lambda n: len(PurePosixPath(n).parts), reverse=True):
        safe(source / name).rmdir()
    require(validate(source) == before and set(p.name for p in source.iterdir()) == {MARKER}, "FINAL_COMPACTION_CHANGED")
    return {"observation": source.name, "completed_months": before["completed_months"],
            "manifest_sha256": receipt["manifest_sha256"], "receipt_sha256": sha(receipt_path),
            "logical_bytes_referenced": sum(m["bytes"] for m in listing["files"].values()),
            "accepted_progress_unchanged": True}


REMOTE_INVENTORY = r'''
import hashlib,json,pathlib,stat,sys,re
r=pathlib.Path(sys.argv[1]);assert re.fullmatch(r"/opt/pipeline/staging/dat03-catalog/[a-f0-9]{32}",str(r))
assert not any(p.is_symlink() for p in (r,*r.parents))
files={};dirs=[]
for p in sorted(r.rglob("*")):
 assert not p.is_symlink();name=p.relative_to(r).as_posix();s=p.stat()
 if stat.S_ISDIR(s.st_mode):dirs.append(name)
 else:
  assert stat.S_ISREG(s.st_mode) and s.st_nlink==1 and s.st_size<=268435456
  with p.open("rb") as f:h=hashlib.file_digest(f,"sha256").hexdigest()
  files[name]={"bytes":s.st_size,"sha256":h,"mode":stat.S_IMODE(s.st_mode)}
assert len(files)<=50000 and sum(m["bytes"] for m in files.values())<=4294967296
print(json.dumps({"files":files,"directories":dirs},sort_keys=True))
'''

REMOTE_STREAM = r'''
import pathlib,sys,json,hashlib,stat,re
r=pathlib.Path(sys.argv[1]);assert re.fullmatch(r"/opt/pipeline/staging/dat03-catalog/[a-f0-9]{32}",str(r))
items=json.load(sys.stdin);assert isinstance(items,list) and len(items)<=50000
for item in items:
 n=item["name"];assert isinstance(n,str) and not n.startswith("/") and ".." not in n.split("/") and "\\" not in n
 p=r/n;assert not any(q.is_symlink() for q in (p,*p.parents));s=p.stat()
 assert stat.S_ISREG(s.st_mode) and s.st_nlink==1 and s.st_size==item["bytes"]<=268435456
 h=hashlib.sha256();left=s.st_size
 with p.open("rb") as f:
  while left:
   b=f.read(min(left,1048576));assert b;sys.stdout.buffer.write(b);h.update(b);left-=len(b)
 assert h.hexdigest()==item["sha256"]
sys.stdout.buffer.flush()
'''


def remote_inventory(container, inside):
    result = subprocess.run(["docker", "exec", container, "python3", "-B", "-c", REMOTE_INVENTORY, inside],
                            check=True, capture_output=True, timeout=120)
    require(len(result.stdout) <= MAX_MANIFEST, "REMOTE_INVENTORY_BOUND")
    value = json.loads(result.stdout); validate_inventory(value["files"], value["directories"])
    return value


def allowed_runtime_file(name, frozen):
    return (name in frozen or name == "bronze-pins.json" or name.startswith(("months/", "sessions/", "__pycache__/"))
            or re.fullmatch(r"session-resolutions/[a-f0-9]{32}\.json", name) is not None)


@locked
def capture(catalog, target, container, inside, frozen, host_note):
    catalog, target = map(safe, (catalog, target))
    require(target.parent == catalog and re.fullmatch(r"runtime(?:-observation-[a-f0-9]{32})?", target.name)
            and not target.exists(), "NEW_HOST_TARGET_REQUIRED")
    listing = remote_inventory(container, inside)
    missing = {}; transferred = 0
    for name, meta in listing["files"].items():
        require(allowed_runtime_file(name, frozen), "UNEXPECTED_RUNTIME_FILE")
        if name in frozen:
            verify_file(catalog / name, meta)
        elif blob(catalog, meta["sha256"]).exists():
            verify_file(blob(catalog, meta["sha256"]), meta)
        else:
            missing.setdefault(meta["sha256"], {"name": name, **meta})
    # Only missing unique content crosses the container boundary, not the complete tree.
    require(shutil.disk_usage(catalog).free >= sum(item["bytes"] for item in missing.values()) + 1024**3,
            "TRANSFER_DISK_RESERVE")
    if missing:
        command = ["docker", "exec", "-i", container, "python3", "-B", "-c", REMOTE_STREAM, inside]
        with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as child:
            try:
                child.stdin.write(json.dumps(list(missing.values())).encode()); child.stdin.close()
                stream = DeadlineReader(child.stdout)
                for item in missing.values():
                    put(catalog, {key: item[key] for key in ("bytes", "sha256", "mode")}, stream)
                    transferred += item["bytes"]
                require(not stream.read(1), "EXCESS_TRANSFER_BYTES")
                require(child.wait(timeout=120) == 0, "REMOTE_STREAM_FAILED")
            except BaseException:
                child.kill(); child.wait(); raise
            finally:
                child.stdout.close()
    require(remote_inventory(container, inside) == listing, "RUNTIME_CHANGED_DURING_CAPTURE")
    note = json.dumps(host_note(), sort_keys=True, indent=2, allow_nan=False).encode() + b"\n"
    meta = {"bytes": len(note), "sha256": hashlib.sha256(note).hexdigest(), "mode": 0o600}
    if not blob(catalog, meta["sha256"]).exists():
        import io
        put(catalog, meta, io.BytesIO(note))
    listing["files"]["host-observation.json"] = meta
    result = seal(catalog, target, listing, frozen)
    return result, {"unique_payload_bytes_transferred": transferred, "new_payload_objects": len(missing),
                    "snapshot_logical_bytes": sum(v["bytes"] for v in listing["files"].values()),
                    "manifest_sha256": sha(target / MARKER)}


def publish_latest(catalog, target, proof):
    require(proof["status"] == "ACCEPTED_CATALOG_CHECKPOINT" and proof["lease_release"]["status"] == "RELEASED",
            "LATEST_REQUIRES_ACCEPTANCE")
    require(type(proof["completed_months"]) is int and 0 <= proof["completed_months"] <= 288
            and isinstance(proof["session_id"], str) and re.fullmatch(r"[a-f0-9]{32}", proof["session_id"]),
            "LATEST_TYPED_PREFIX_AND_SESSION_REQUIRED")
    return publish_latest_locked(catalog, target, proof)


@locked
def publish_latest_locked(catalog, target, proof):
    pointer = safe(catalog / STORE / "latest.json")
    if pointer.exists():
        old = load(pointer)
        require(type(old["completed_months"]) is int and old["completed_months"] <= proof["completed_months"], "LATEST_CANNOT_REGRESS")
    value = {"version": VERSION, "observation": target.name, "manifest_sha256": sha(target / MARKER),
             "completed_months": proof["completed_months"], "session_id": proof["session_id"],
             "catalog_request_sha256": sha(catalog / "request.json"), "research_ready": False, "official_exported": False}
    temp = pointer.with_name("latest-pending-" + uuid.uuid4().hex + ".json")
    persist(temp, value); os.replace(temp, pointer); sync_directory(pointer.parent)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("verify", "compact", "restore"))
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--observation", type=Path, required=True)
    parser.add_argument("--session", required=True)
    parser.add_argument("--remove-verified-legacy-files", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    require(re.fullmatch(r"[a-f0-9]{32}", args.session), "EXACT_SESSION_REQUIRED")
    spec = importlib.util.spec_from_file_location("observation_progress", Path(__file__).with_name("research-catalog-progress.py"))
    progress = importlib.util.module_from_spec(spec); spec.loader.exec_module(progress)
    catalog, source = map(safe, (args.catalog, args.observation))
    require(catalog_of(source) == catalog and source.parent == catalog
            and re.fullmatch(r"runtime(?:-observation-[a-f0-9]{32})?", source.name), "EXACT_OBSERVATION_REQUIRED")
    validate = lambda root: progress.report(catalog, root, args.session)
    if args.mode == "verify":
        require(not args.remove_verified_legacy_files, "VERIFY_IS_READ_ONLY")
        print(json.dumps(validate(source), sort_keys=True)); return
    if args.mode == "restore":
        require(args.output is not None and not args.remove_verified_legacy_files, "EXPLICIT_NEW_RESTORE_OUTPUT_REQUIRED")
        proof = validate(source)
        target = materialize(source, args.output, progress.c.files())
        require(validate(target) == proof, "RESTORED_ACCEPTANCE_CHANGED")
        print(json.dumps({"status": "RESTORED_VERIFIED", "output": str(target)})); return
    require(args.remove_verified_legacy_files, "EXPLICIT_LEGACY_REMOVAL_REQUIRED")
    require(not (source / MARKER).exists(), "ALREADY_COMPACTED_NO_AUTOMATIC_RETRY")
    rollout = progress.c.module("research-rollout")
    rollout.services(); rollout.idle(); rollout.trino_queries_idle()
    before = shutil.disk_usage(catalog).free
    print(json.dumps(compact_legacy(catalog, source, progress.c.files(), validate), sort_keys=True))
    proof = validate(source); pointer = catalog / STORE / "latest.json"
    if not pointer.exists() or load(pointer)["completed_months"] <= proof["completed_months"]:
        publish_latest(catalog, source, proof)
    print(json.dumps({"host_free_before": before, "host_free_after": shutil.disk_usage(catalog).free,
                      "scope": "OBSERVED_HOST_DELTA_NOT_EXCLUSIVE_ATTRIBUTION"}))


if __name__ == "__main__":
    main()
