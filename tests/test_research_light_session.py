"""Mock-only four-container session guards. Never starts Docker or contacts a source."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("light_session", ROOT / "scripts/research-light-session.py")
s = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(s)


class LightSessionTest(unittest.TestCase):
    def containers(self, deployment):
        return {name:{"Name":"/japan-earthquake-etl-"+name+"-1", "Id":"id-"+name, "Image":"image-"+name,
                      "Config":{"Env":["PRIVATE_KEY=example-only"],"Labels":{"com.docker.compose.project":"japan-earthquake-etl",
                                 "com.docker.compose.project.working_dir":str(deployment)}},"HostConfig":{"Memory":100},"Mounts":[],
                      "State":{"Status":"running","Health":{"Status":"healthy"},"StartedAt":"before"},"RestartCount":0}
                for name in (*s.KEPT,*s.TARGETS)}

    def test_operator_approval_is_required_before_any_docker_access(self):
        with patch("sys.argv",["session","--deployment-root","/tmp/fixture","--runtime-root","/tmp/runtime"]), \
                patch.object(s,"command",side_effect=AssertionError("Docker forbidden")):
            with self.assertRaisesRegex(ValueError,"EXPLICIT_OPERATOR_APPROVAL_REQUIRED"):s.main()

    def test_snapshot_does_not_leak_environment_and_pins_all_config_and_mounts(self):
        rows=self.containers(Path("/fixture"));before=s.snapshot(rows)
        self.assertNotIn("example-only",json.dumps(before));self.assertNotIn('"Env"',json.dumps(before))
        rows["trino"]["Config"]["Env"]=["PRIVATE_KEY=changed"]
        with self.assertRaisesRegex(ValueError,"DRIFT"):s.unchanged(before,s.snapshot(rows))

    def test_exact_project_and_container_set_are_required(self):
        rows=self.containers(Path("/fixture"));s.project(rows,Path("/fixture"))
        rows["spark-master"]["Config"]["Labels"]["com.docker.compose.project"]="other"
        with self.assertRaisesRegex(ValueError,"FOREIGN_PROJECT"):s.project(rows,Path("/fixture"))
        rows.pop("spark-worker")
        with self.assertRaisesRegex(ValueError,"EXACT_CONTAINER_SET"):s.project(rows,Path("/fixture"))

    def trial(self, error=None, stop_timeout=False):
        with tempfile.TemporaryDirectory() as tmp:
            deployment=Path(tmp).resolve();(deployment/"compose.yaml").write_text("services: {}");(deployment/"staging").mkdir()
            current=self.containers(deployment);calls=[]
            def rows(names=(*s.KEPT,*s.TARGETS)):return {name:copy.deepcopy(current[name]) for name in names}
            def command(args, timeout=30):
                calls.append(args)
                if args[:2]==["docker","stop"]:
                    self.assertEqual(["id-"+n for n in s.TARGETS],args[4:])
                    for name in s.TARGETS:current[name]["State"]["Status"]="exited"
                    if stop_timeout:raise subprocess.TimeoutExpired(args,timeout)
                elif args[:2]==["docker","start"]:
                    self.assertEqual(["id-"+n for n in s.TARGETS],args[2:])
                    for name in s.TARGETS:current[name]["State"].update(Status="running",StartedAt="after")
                else:raise AssertionError("unexpected mutation: "+str(args))
                return ""
            result={"status":"STRESS_PROBE_PASSED","postflight_status":"PASSED_UNCHANGED"}
            with patch.object(s,"rows",side_effect=rows),patch.object(s,"command",side_effect=command), \
                    patch.object(s.r,"services"),patch.object(s.r,"idle"),patch.object(s.r,"trino_queries_idle"),patch.object(s,"spark_idle"), \
                    patch.object(s,"validate_runtime"),patch.object(s,"host_sample",return_value={"status":"fixture"}),patch.object(s.time,"sleep"), \
                    patch.object(s.r,"probe",side_effect=error,return_value=result) as probe:
                receipt=s.run(deployment,deployment/"runtime")
            self.assertEqual(1,len(list((deployment/"staging").glob("*/restored.json"))))
            self.assertTrue(receipt["four_original_containers_restored_healthy"])
            self.assertFalse(receipt["full_capacity_accepted"])
            self.assertFalse(receipt["dag_lease_or_credential_mutations"])
            self.assertEqual(2,len(calls));self.assertTrue(s.healthy(current))
            return receipt,probe.call_count

    def test_stops_and_starts_only_four_original_ids_and_keeps_other_five(self):
        receipt,count=self.trial();self.assertIsNone(receipt["failure_type"]);self.assertEqual(2,count)

    def test_probe_failure_always_restores_four_and_never_retries(self):
        receipt,count=self.trial(ValueError("private error"))
        self.assertEqual("ValueError",receipt["failure_type"]);self.assertEqual(1,count)
        self.assertNotIn("private error",json.dumps(receipt))

    def test_unknown_stop_outcome_is_inspected_and_restored_without_repeating_stop(self):
        receipt,count=self.trial(stop_timeout=True)
        self.assertEqual("TimeoutExpired",receipt["failure_type"]);self.assertEqual(0,count)

    def test_restore_refuses_replaced_target_before_starting_any_container(self):
        before=s.snapshot(self.containers(Path("/fixture")));current=self.containers(Path("/fixture"))
        current["spark-worker"]["Id"]="foreign-replacement"
        with patch.object(s,"rows",return_value={name:current[name] for name in s.TARGETS}), \
                patch.object(s,"command",side_effect=AssertionError("must not start replacement")):
            with self.assertRaisesRegex(ValueError,"DRIFT"):s.restore(before)

    def test_spark_guard_is_readonly_bounded_and_checks_apps_drivers_and_worker(self):
        with patch.object(s,"command",return_value="LIGHT_SPARK_IDLE\n") as command:
            s.spark_idle();script=command.call_args.args[0][-1]
            for key in ("activeapps","activedrivers","workers"):self.assertIn(key,script)
            self.assertNotIn("POST",script);self.assertNotIn("DELETE",script)
            self.assertEqual(20,command.call_args.args[1])

    def test_wrong_runtime_blocks_before_any_docker_or_container_stop(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();(root/"compose.yaml").write_text("services: {}")
            with patch.object(s,"command",side_effect=AssertionError("must not contact Docker")):
                with self.assertRaisesRegex(ValueError,"EXACT_RETAINED_RUNTIME_REQUIRED"):s.run(root,root/"foreign")

    def test_mount_order_is_not_drift_but_any_mount_content_change_is(self):
        rows=self.containers(Path("/fixture"))
        rows["airflow-scheduler"]["Mounts"]=[{"Destination":"/b","Source":"/two","RW":False},{"Destination":"/a","Source":"/one","RW":True}]
        before=s.snapshot(rows);rows["airflow-scheduler"]["Mounts"].reverse()
        s.unchanged(before,s.snapshot(rows))
        rows["airflow-scheduler"]["Mounts"][0]["RW"]=False
        with self.assertRaisesRegex(ValueError,"DRIFT"):s.unchanged(before,s.snapshot(rows))

    def test_pre_stop_drift_never_calls_stop_start_or_restoration(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();(root/"compose.yaml").write_text("services: {}");(root/"staging").mkdir()
            original=self.containers(root);changed=copy.deepcopy(original);changed["spark-worker"]["Config"]["Env"]=["CHANGED=true"]
            with patch.object(s,"validate_runtime"),patch.object(s,"rows",side_effect=[original,changed]), \
                    patch.object(s.r,"services"),patch.object(s.r,"idle"),patch.object(s.r,"trino_queries_idle"), \
                    patch.object(s,"spark_idle"),patch.object(s,"host_sample",return_value={"status":"fixture"}), \
                    patch.object(s,"command",side_effect=AssertionError("no stop/start")),patch.object(s,"restore") as restore:
                with self.assertRaisesRegex(ValueError,"PREFLIGHT_FAILED_NO_CONTAINER_MUTATION"):s.run(root,root/"runtime")
            restore.assert_not_called()
            folder=next((root/"staging").iterdir());self.assertFalse((folder/"stop-intent.json").exists())
            self.assertFalse(s.r.load(folder/"pre-stop-failed.json")["stop_or_start_performed"])


if __name__=="__main__":unittest.main()
