import importlib.util
from pathlib import Path
import signal
import subprocess
import unittest
from unittest.mock import Mock,patch

spec=importlib.util.spec_from_file_location("spark_capacity",Path(__file__).resolve().parents[1]/"scripts/research-spark-capacity.py")
capacity=importlib.util.module_from_spec(spec);spec.loader.exec_module(capacity)


class SparkCapacityTest(unittest.TestCase):
    def test_invalid_limits_stop_before_any_docker_or_file_write(self):
        for table,copies,partitions,seconds in (("bad",43,1,1200),("event_current",True,1,1200),("event_current",43,8,1200),("event_current",43,1,7200)):
            with patch.object(capacity,"module") as module,self.assertRaisesRegex(RuntimeError,"TYPED_LIMITS"):
                capacity.run(Path("/nonexistent"),Path("/nonexistent"),table,copies,partitions,seconds)
            module.assert_not_called()

    def test_success_never_sends_a_signal(self):
        child=Mock(pid=12345);child.wait.return_value=0
        with patch.object(capacity.subprocess,"Popen",return_value=child) as start,patch.object(capacity.os,"killpg") as kill:
            self.assertEqual((0,False),capacity.bounded_child(["owned-child"],None,None,300))
        self.assertTrue(start.call_args.kwargs["start_new_session"]);kill.assert_not_called()

    def test_timeout_only_terminates_the_owned_process_group(self):
        child=Mock(pid=12345);child.wait.side_effect=[subprocess.TimeoutExpired("owned-child",300),-15]
        with patch.object(capacity.subprocess,"Popen",return_value=child),patch.object(capacity.os,"killpg") as kill:
            self.assertEqual((-15,True),capacity.bounded_child(["owned-child"],None,None,300))
        kill.assert_called_once_with(12345,signal.SIGTERM)

    def test_forced_kill_only_after_own_child_ignores_term(self):
        child=Mock(pid=12345);child.wait.side_effect=[subprocess.TimeoutExpired("owned-child",300),subprocess.TimeoutExpired("owned-child",20),-9]
        with patch.object(capacity.subprocess,"Popen",return_value=child),patch.object(capacity.os,"killpg") as kill:
            self.assertEqual((-9,True),capacity.bounded_child(["owned-child"],None,None,300))
        self.assertEqual([(12345,signal.SIGTERM),(12345,signal.SIGKILL)],[call.args for call in kill.call_args_list])


if __name__=="__main__":unittest.main()
