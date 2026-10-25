#!/usr/bin/env python3
"""Offline paired research collection verification; no ETL, fitting, cloud or pipeline credentials."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import re
import sys
import types

VERSION="dat03-team-research-v1"
ROSTER=("HoaiTam","ThanhTris","rosy179")
SPLITS=("REPRODUCTION","EXTENSION")
SUPPORT={"README.md","requirements.txt","verify_dataset.py","tools/research_collection.py","tools/experiment_harness.py","tools/pilot_bundle.py"}
MAX_FILE=64*1024**2
MAX_TOTAL=8*1024**3


def require(ok,reason):
    if not ok:raise ValueError(reason)


def safe(path):
    path=Path(path).absolute()
    require(not any(p.is_symlink() for p in (path,*path.parents)) and path==path.resolve(),"RESEARCH_TEAM_UNSAFE_PATH")
    return path


def sha(path):
    with safe(path).open("rb") as stream:return hashlib.file_digest(stream,"sha256").hexdigest()


def read_json(path):
    path=safe(path);require(path.is_file() and 0<path.stat().st_size<=512*1024,"RESEARCH_TEAM_METADATA_BOUND")
    def unique(items):
        result={}
        for key,value in items:require(key not in result,"RESEARCH_TEAM_DUPLICATE_KEY");result[key]=value
        return result
    def bad(_):raise ValueError("RESEARCH_TEAM_NONFINITE")
    return json.loads(path.read_bytes(),object_pairs_hook=unique,parse_constant=bad)


def valid_name(name):
    return name in SUPPORT or re.fullmatch(r"bundles/(?:REPRODUCTION|EXTENSION)/(?:dataset_manifest\.json|collection-preparation\.json|(?:export-)?checksums\.sha256|shard_[0-9]{5}/(?:features|mainshocks)\.parquet)",name) is not None


def gold_pin(manifest):
    pin={key:manifest.get(key) for key in ("physical_gold_table_name","gold_publication_id","publication_bundle_sha256")}
    value=manifest.get("gold_snapshot_id")
    require(type(value) in (str,int) and re.fullmatch(r"[1-9][0-9]{0,18}",str(value)) and int(value)<=9223372036854775807,
            "RESEARCH_TEAM_GOLD_PIN_REQUIRED")
    pin["gold_snapshot_id"]=str(value)
    require(isinstance(pin["physical_gold_table_name"],str)
            and re.fullmatch(r"iceberg\.dat03_catalog_[a-f0-9]{32}\.event_current",pin["physical_gold_table_name"])
            and isinstance(pin["gold_publication_id"],str) and re.fullmatch(r"pub_[a-f0-9]{64}",pin["gold_publication_id"])
            and isinstance(pin["publication_bundle_sha256"],str) and re.fullmatch(r"[a-f0-9]{64}",pin["publication_bundle_sha256"]),
            "RESEARCH_TEAM_GOLD_PIN_REQUIRED")
    require(pin["gold_publication_id"]=="pub_"+pin["publication_bundle_sha256"],"RESEARCH_TEAM_GOLD_PIN_CHANGED")
    return pin


def summary(collection):
    result=collection.verify_all()
    result["dataset_split"]=collection.manifest["dataset_split"]
    result["gold_pin"]=gold_pin(collection.manifest)
    return result


def pair(results):
    require(set(results)==set(SPLITS) and all(results[s]["dataset_split"]==s for s in SPLITS),"RESEARCH_TEAM_TWO_SPLITS_REQUIRED")
    require(results[SPLITS[0]]["gold_pin"]==results[SPLITS[1]]["gold_pin"],"RESEARCH_TEAM_GOLD_PIN_CHANGED")
    require(len({results[s]["dataset_id"] for s in SPLITS})==2 and len({results[s]["collection_id"] for s in SPLITS})==2,
            "RESEARCH_TEAM_SPLIT_IDENTITIES_REQUIRED")


def verify(root,expected_handoff_sha256,recipient,receipt=None):
    require(recipient in ROSTER,"RESEARCH_TEAM_RECIPIENT_REQUIRED")
    require(isinstance(expected_handoff_sha256,str) and re.fullmatch(r"[a-f0-9]{64}",expected_handoff_sha256),"RESEARCH_TEAM_EXTERNAL_PIN_REQUIRED")
    root=safe(root);require(root.is_dir(),"RESEARCH_TEAM_DIRECTORY_REQUIRED")
    metadata=safe(root/"handoff.json")
    require(metadata.is_file() and metadata.stat().st_size<=512*1024 and sha(metadata)==expected_handoff_sha256,"RESEARCH_TEAM_HANDOFF_PIN_CHANGED")
    handoff=read_json(metadata)
    require(handoff.get("version")==VERSION and handoff.get("artifact_kind")=="PAIRED_COLLECTION_EXPORT"
            and handoff.get("scientific_approval") is False and handoff.get("recipients")==list(ROSTER)
            and set(handoff.get("collections",{}))==set(SPLITS),"RESEARCH_TEAM_PAIRED_RELEASE_REQUIRED")
    files=handoff.get("files",{});require(isinstance(files,dict) and SUPPORT<=set(files) and len(files)<=270
                                         and all(valid_name(n) for n in files),"RESEARCH_TEAM_EXACT_FILE_SET_REQUIRED")
    expected=set(files)|{"handoff.json"};directories={str(parent) for name in expected for parent in Path(name).parents if str(parent)!="."}
    found=set();total=0
    for path in root.rglob("*"):
        path=safe(path);name=path.relative_to(root).as_posix()
        if path.is_dir():require(name in directories,"RESEARCH_TEAM_EXTRA_DIRECTORY")
        else:require(path.is_file(),"RESEARCH_TEAM_UNSAFE_FILE");found.add(name)
    require(found==expected,"RESEARCH_TEAM_EXACT_FILE_SET_REQUIRED")
    for name,entry in files.items():
        path=root/name
        require(type(entry["bytes"]) is int and 0<entry["bytes"]<=MAX_FILE and path.stat().st_size==entry["bytes"]
                and isinstance(entry["sha256"],str) and re.fullmatch(r"[a-f0-9]{64}",entry["sha256"]) and sha(path)==entry["sha256"],
                "RESEARCH_TEAM_FILE_CHANGED")
        total+=entry["bytes"]
    require(total<=MAX_TOTAL,"RESEARCH_TEAM_TOTAL_BOUND")
    # Execute exactly hashed source bytes, not ambient sys.modules or timestamp
    # pycs. Restore caller state when used as a library or on verification failure.
    names=("pilot_bundle","experiment_harness","verified_research_collection")
    originals={name:sys.modules.get(name) for name in names};prior_path=list(sys.path)
    prior_no_bytecode=sys.dont_write_bytecode
    results={}
    try:
        sys.dont_write_bytecode=True;sys.path.insert(0,str(root/"tools"))
        for name,file in zip(names,("pilot_bundle.py","experiment_harness.py","research_collection.py")):
            relative="tools/"+file;path=safe(root/relative);raw=path.read_bytes()
            require(hashlib.sha256(raw).hexdigest()==files[relative]["sha256"],"RESEARCH_TEAM_CODE_CHANGED_BEFORE_IMPORT")
            module=types.ModuleType(name);module.__file__=str(path);sys.modules[name]=module
            exec(compile(raw,str(path),"exec"),module.__dict__)
        for split in SPLITS:
            declared=handoff["collections"][split]
            collection=module.ResearchCollection(root/"bundles"/split,manifest_sha256=declared["manifest_sha256"],checksums_sha256=declared["checksums_sha256"])
            results[split]=summary(collection)
            require(results[split]==declared and results[split]["dataset_split"]==split,"RESEARCH_TEAM_COLLECTION_CHANGED")
    finally:
        sys.path[:]=prior_path;sys.dont_write_bytecode=prior_no_bytecode
        for name,value in originals.items():
            if value is None:sys.modules.pop(name,None)
            else:sys.modules[name]=value
    pair(results)
    result={"version":VERSION,"status":"TEAM_RESEARCH_VERIFIED","recipient":recipient,"recipient_self_reported":True,
            "verified_at_utc":datetime.now(timezone.utc).isoformat(),"handoff_sha256":expected_handoff_sha256,
            "machine_fingerprint_sha256":hashlib.sha256(platform.node().encode()).hexdigest(),"python_version":platform.python_version(),
            "collections":results,"all_shards_verified":True,"scientific_approval":False}
    if receipt is not None:
        target=safe(receipt);require(not target.is_relative_to(root),"RESEARCH_TEAM_RECEIPT_OUTSIDE_KIT_REQUIRED")
        with target.open("x",encoding="utf-8") as stream:json.dump(result,stream,sort_keys=True,indent=2,allow_nan=False);stream.write("\n")
    return result


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kit",type=Path,default=Path(__file__).resolve().parent)
    parser.add_argument("--expected-handoff-sha256",required=True)
    parser.add_argument("--recipient",choices=ROSTER,required=True)
    parser.add_argument("--receipt",type=Path,required=True)
    args=parser.parse_args()
    print(json.dumps(verify(args.kit,args.expected_handoff_sha256,args.recipient,args.receipt),sort_keys=True,indent=2))
