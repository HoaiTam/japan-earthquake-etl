"""Offline profile/lifecycle guards. Never starts Docker or applies a profile."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("trino_research", ROOT / "scripts/research-trino-profile.py")
p = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(p)


class ResearchTrinoProfileTest(unittest.TestCase):
    def test_profile_keeps_all_memory_caps_and_default_files_unchanged(self):
        original = dict(line.split("=",1) for line in (ROOT / "trino/config.properties").read_text().splitlines())
        research = dict(line.split("=",1) for line in (ROOT / "trino/research/config.properties").read_text().splitlines())
        self.assertEqual("1", research.pop("task.concurrency")); self.assertEqual(original, research)
        a = (ROOT / "trino/jvm.config").read_text().splitlines(); b = (ROOT / "trino/research/jvm.config").read_text().splitlines()
        self.assertEqual(["-XX:G1HeapRegionSize=32M"], [flag for flag in a if flag not in b])
        self.assertEqual(["-Xlog:gc*:stdout:time,uptime,level,tags"], [flag for flag in b if flag not in a])
        for flag in ("-Xmx1536m", "-Xms256m", "-XX:MaxDirectMemorySize=512m"):
            self.assertIn(flag, b)

    def test_exact_image_and_all_environment_must_survive_not_only_new_keys(self):
        old = {"Image":"sha256:fixture", "Config":{"Image":"trino:fixture", "Env":["PATH=/bin","S3_ACCESS_KEY=private-example"]}}
        compiled = {"image":"trino:fixture", "environment":{"S3_ACCESS_KEY":"private-example"}}
        p.same_environment_and_image(compiled, old, ["PATH=/bin"])
        for change in ({"image":"trino:other","environment":compiled["environment"]}, {"image":"trino:fixture","environment":{}},
                       {"image":"trino:fixture","environment":{"S3_ACCESS_KEY":"changed"}}):
            with self.assertRaisesRegex(ValueError,"ENVIRONMENT_DRIFT"): p.same_environment_and_image(change,old,["PATH=/bin"])

    def test_snapshot_does_not_persist_credentials_or_raw_environment(self):
        row = {"Id":"one","Image":"image","RestartCount":0,"State":{"StartedAt":"now"},"Config":{"Env":["KEY=private-example"],"User":"trino"},
               "HostConfig":{"Memory":3221225472,"NanoCpus":2000000000}}
        result = p.snapshot({p.TRINO:row}); text = json.dumps(result)
        self.assertNotIn("private-example", text); self.assertNotIn('"Env"',text)
        self.assertEqual(p.env_pin(["B=2","A=1"]),p.env_pin(["A=1","B=2"]))
        self.assertNotEqual(p.env_pin(["A=1"]),p.env_pin(["A=2"]))

    def test_query_preflight_only_reads_and_never_cancels_or_retries(self):
        with patch.object(p,"command",return_value="TRINO_QUERIES_IDLE\n") as call:
            p.queries_idle(); script=call.call_args.args[0][-1]
            self.assertIn("urlopen",script); self.assertIn('(\"FINISHED\",\"FAILED\")',script)
            self.assertNotIn("DELETE",script);self.assertNotIn("POST",script)
        with patch.object(p,"command",return_value="not-idle"):
            with self.assertRaisesRegex(ValueError,"PREFLIGHT_FAILED"):p.queries_idle()

    def test_cli_apply_requires_explicit_operator_approval_before_docker(self):
        with patch.object(p,"command",side_effect=AssertionError("unexpected Docker")), patch("sys.argv",["profile","apply","--folder","/nonexistent"]):
            with self.assertRaisesRegex(ValueError,"APPROVAL_REQUIRED"):p.main()

    def test_backup_file_is_private_and_cannot_be_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            file=Path(tmp).resolve()/"backup.private";p.immutable_private(file,b"original")
            self.assertEqual(0o600,file.stat().st_mode & 0o777)
            with self.assertRaises(FileExistsError):p.immutable_private(file,b"new")
            self.assertEqual(b"original",file.read_bytes())

    def test_compose_scope_rejects_foreign_project_or_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();(root/"compose.yaml").write_text("services: {}")
            labels={"com.docker.compose.project":"japan-earthquake-etl","com.docker.compose.project.working_dir":str(root),
                    "com.docker.compose.project.config_files":str(root/"compose.yaml")}
            self.assertEqual([root/"compose.yaml"],p.config_files({"Config":{"Labels":labels}},root))
            labels["com.docker.compose.project"]="foreign"
            with self.assertRaisesRegex(ValueError,"PROJECT_MISMATCH"):p.config_files({"Config":{"Labels":labels}},root)

    def test_late_verification_cannot_apply_compose_or_accept_missing_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve(); folder = root / "staging" / ("dat03-trino-research-" + "a" * 32)
            folder.mkdir(parents=True)
            plan = {"version":"dat03-trino-research-profile-v1", "deployment":str(root), "profiles":{}, "backup_sha256":{}}
            p.r.save(folder / "plan.json", plan)
            p.r.save(folder / "execution.json", {"status":"COMPOSE_RETURNED", "exit_code":0})
            with patch.object(p, "command", side_effect=AssertionError("no Docker before pins")), \
                    patch.object(p.r, "services", side_effect=AssertionError("no Docker before pins")):
                with self.assertRaisesRegex(ValueError,"EXACT_CONFIG_SET_REQUIRED"):p.verify(folder)

    def test_reconcile_unknown_execution_must_not_retry_apply(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve(); folder = root / "staging" / ("dat03-trino-research-" + "a" * 32)
            folder.mkdir(parents=True)
            p.r.save(folder / "plan.json", {"version":"dat03-trino-research-profile-v1", "deployment":str(root)})
            p.r.save(folder / "execution.json", {"status":"APPLY_OUTCOME_UNKNOWN_REQUIRES_INSPECT", "retry":False})
            with patch.object(p, "command", side_effect=AssertionError("unexpected Docker")):
                with self.assertRaisesRegex(ValueError,"RECONCILE_EXECUTION_REQUIRED"):p.verify(folder)
                with self.assertRaisesRegex(ValueError,"EXACT_NEW_ATTEMPT_REQUIRED"):p.apply(folder)

    def test_reconcile_detects_backup_tamper_before_reading_runtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp).resolve(); backup = folder / "before-config.properties"
            p.immutable_private(backup,b"original")
            plan = {"profiles":{name:{"candidate":str(ROOT / "trino/research" / name),
                                      "candidate_sha256":p.r.profile.sha(ROOT / "trino/research" / name)} for name in p.CONFIGS},
                    "backup_sha256":{backup.name:p.r.profile.sha(backup)}}
            p.verify_backup_and_candidates(folder,plan)
            backup.write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError,"BACKUP_CHANGED"):p.verify_backup_and_candidates(folder,plan)

    def test_late_health_receipt_never_claims_initial_or_capacity_acceptance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve(); folder = root / "staging" / ("dat03-trino-research-" + "a" * 32)
            folder.mkdir(parents=True); override = folder / "compose.override.yaml"
            override.write_text("services: {}")
            files = [root / "compose.yaml"]
            profiles = {name:{"candidate":str(ROOT / "trino/research" / name),
                              "candidate_sha256":p.r.profile.sha(ROOT / "trino/research" / name)} for name in p.CONFIGS}
            row = {"Id":"new", "Image":"image", "RestartCount":0, "State":{"StartedAt":"later"},
                   "Config":{"Env":["PATH=/bin"],"User":"trino"}, "HostConfig":{"Memory":3221225472,"NanoCpus":2000000000},
                   "Mounts":[{"Destination":dest,"Source":profiles[name]["candidate"],"Type":"bind","RW":False}
                             for name,dest in p.CONFIGS.items()]}
            before = p.snapshot({p.TRINO:row}); before[p.TRINO].update(id="old",started_at="before")
            plan = {"version":"dat03-trino-research-profile-v1", "deployment":str(root), "before":before,
                    "profiles":profiles,"backup_sha256":{override.name:p.r.profile.sha(override)},"config_files":[str(f) for f in files]}
            p.r.save(folder / "plan.json",plan);p.r.save(folder / "execution.json",{"status":"COMPOSE_RETURNED","exit_code":0})
            compiled = {"services":{"trino":{"volumes":[{"target":m["Destination"],"source":m["Source"],"type":"bind","read_only":True}
                                                          for m in row["Mounts"]]}}}
            with patch.object(p.r,"services"),patch.object(p.r,"idle"),patch.object(p,"queries_idle"), \
                    patch.object(p,"inspect",return_value={p.TRINO:row}),patch.object(p,"config_files",return_value=files+[override]), \
                    patch.object(p,"same_environment_and_image"),patch.object(p,"command",return_value=json.dumps(compiled)) as command:
                receipt = p.verify(folder)
            self.assertEqual("LATE_HEALTH_VERIFIED_NOT_CAPACITY_ACCEPTED",receipt["status"])
            self.assertFalse(receipt["initial_180_second_health_acceptance_passed"])
            self.assertFalse(receipt["full_capacity_accepted"]);self.assertFalse(receipt["data_mutations"])
            self.assertEqual(1,command.call_count)
            self.assertEqual(["config","--format","json"],command.call_args.args[0][-3:])

    def test_new_apply_requires_exact_compose_content_pins_and_rejects_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve(); file = root / "compose.yaml"; file.write_text("services: {}")
            plan = {"deployment":str(root),"config_files":[str(file)]}
            with self.assertRaisesRegex(ValueError,"CONTENT_PINS_REQUIRED"):p.verify_compose_pins(plan)
            plan["compose_sha256"] = {str(file):p.r.profile.sha(file)}
            p.verify_compose_pins(plan)
            file.write_text("services: {changed: {}}")
            with self.assertRaisesRegex(ValueError,"CONTENT_CHANGED"):p.verify_compose_pins(plan)


if __name__ == "__main__":unittest.main()
