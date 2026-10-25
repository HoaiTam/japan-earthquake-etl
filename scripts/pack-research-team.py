#!/usr/bin/env python3
"""Stream two independently verified official split exports into one immutable offline team kit."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import zipfile

ROOT=Path(__file__).resolve().parents[1]
sys.dont_write_bytecode=True
sys.path.insert(0,str(ROOT/"notebooks"))
from research_collection import ResearchCollection

SPEC=importlib.util.spec_from_file_location("research_team_verifier",ROOT/"scripts/verify-research-team.py")
v=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(v)


def pack(inputs,output):
    v.require(set(inputs)==set(v.SPLITS),"RESEARCH_TEAM_TWO_SPLITS_REQUIRED")
    output=v.safe(output);v.require(output.suffix==".zip","RESEARCH_TEAM_ZIP_REQUIRED")
    ancestor=output.parent
    while not ancestor.exists():ancestor=ancestor.parent
    v.require(shutil.disk_usage(ancestor).free>=4*1024**3,"RESEARCH_TEAM_PHYSICAL_DISK_RESERVE")
    sources={};summaries={};expected={}
    for split in v.SPLITS:
        pin=inputs[split];folder=v.safe(pin["directory"])
        v.require(not output.is_relative_to(folder),"RESEARCH_TEAM_OUTPUT_OUTSIDE_BUNDLES_REQUIRED")
        collection=ResearchCollection(folder,manifest_sha256=pin["manifest_sha256"],checksums_sha256=pin["checksums_sha256"])
        v.require(collection.manifest["dataset_split"]==split,"RESEARCH_TEAM_SPLIT_CHANGED")
        summaries[split]=v.summary(collection)
        for name,digest in {**collection.index,"checksums.sha256":pin["checksums_sha256"]}.items():
            key="bundles/"+split+"/"+name;sources[key]=folder/name;expected[key]=digest
    v.pair(summaries)
    for name,source in {
        "README.md":ROOT/"docs/runbooks/DAT-03_RESEARCH_TEAM_README.md",
        "requirements.txt":ROOT/"config/ml/research_team_requirements.txt",
        "verify_dataset.py":ROOT/"scripts/verify-research-team.py",
        **{"tools/"+n:ROOT/"notebooks"/n for n in ("research_collection.py","experiment_harness.py","pilot_bundle.py")},
    }.items():sources[name]=v.safe(source);expected[name]=v.sha(source)
    v.require(len(sources)<=270 and all(v.valid_name(n) for n in sources),"RESEARCH_TEAM_EXACT_FILE_SET_REQUIRED")
    sizes={n:p.stat().st_size for n,p in sources.items()}
    v.require(all(0<size<=v.MAX_FILE for size in sizes.values()) and sum(sizes.values())<=v.MAX_TOTAL,"RESEARCH_TEAM_TOTAL_BOUND")
    output.parent.mkdir(parents=True,exist_ok=True)
    v.require(shutil.disk_usage(output.parent).free>=4*1024**3+2*sum(sizes.values()),"RESEARCH_TEAM_PHYSICAL_DISK_RESERVE")
    handoff={"version":v.VERSION,"artifact_kind":"PAIRED_COLLECTION_EXPORT","scientific_approval":False,
             "recipients":list(v.ROSTER),"collections":summaries,
             "files":{n:{"bytes":sizes[n],"sha256":expected[n]} for n in sorted(sources)}}
    raw=(json.dumps(handoff,sort_keys=True,indent=2,allow_nan=False)+"\n").encode()
    v.require(len(raw)<=512*1024,"RESEARCH_TEAM_METADATA_BOUND")
    with tempfile.TemporaryDirectory(prefix=".dat03-pack-",dir=output.parent) as temp:
        archive_path=Path(temp)/"team.zip"
        with zipfile.ZipFile(archive_path,"x",compression=zipfile.ZIP_STORED,allowZip64=True) as archive:
            for name in sorted(set(sources)|{"handoff.json"}):
                info=zipfile.ZipInfo(name,date_time=(2000,1,1,0,0,0));info.external_attr=0o100600<<16
                if name=="handoff.json":archive.writestr(info,raw);continue
                digest=hashlib.sha256();count=0
                with v.safe(sources[name]).open("rb") as source,archive.open(info,"w",force_zip64=True) as target:
                    while data:=source.read(1024**2):
                        count+=len(data);v.require(count<=sizes[name],"RESEARCH_TEAM_SOURCE_CHANGED_DURING_PACK")
                        digest.update(data);target.write(data)
                v.require(count==sizes[name] and digest.hexdigest()==expected[name],"RESEARCH_TEAM_SOURCE_CHANGED_DURING_PACK")
        archive_sha=v.sha(archive_path);reused=output.exists()
        if reused:v.require(output.is_file() and output.stat().st_size==archive_path.stat().st_size and v.sha(output)==archive_sha,"RESEARCH_TEAM_NO_OVERWRITE")
        else:
            with archive_path.open("rb") as source,output.open("xb") as target:shutil.copyfileobj(source,target,1024**2)
            v.require(v.sha(output)==archive_sha,"RESEARCH_TEAM_OUTPUT_READBACK_CHANGED")
    return {"status":"TEAM_RESEARCH_PACKED","version":v.VERSION,"zip_sha256":archive_sha,"handoff_sha256":hashlib.sha256(raw).hexdigest(),
            "bytes":output.stat().st_size,"collections":summaries,"files":len(sources)+1,"reused":reused,"scientific_approval":False}


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    for split in ("reproduction","extension"):
        parser.add_argument("--"+split,type=Path,required=True)
        parser.add_argument("--"+split+"-manifest-sha256",required=True)
        parser.add_argument("--"+split+"-checksums-sha256",required=True)
    parser.add_argument("--output",type=Path,required=True);args=parser.parse_args()
    inputs={s:{"directory":getattr(args,s.lower()),"manifest_sha256":getattr(args,s.lower()+"_manifest_sha256"),
               "checksums_sha256":getattr(args,s.lower()+"_checksums_sha256")} for s in v.SPLITS}
    print(json.dumps(pack(inputs,args.output),sort_keys=True,indent=2))
