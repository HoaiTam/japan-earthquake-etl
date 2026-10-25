"""Pinned single-node Trino budget guards; no Docker or network."""
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]


class TrinoMemoryTest(unittest.TestCase):
    def test_heap_direct_query_and_headroom_fit_container(self):
        jvm = (ROOT / "trino/jvm.config").read_text().splitlines()
        self.assertIn("-Xms256m", jvm)
        self.assertIn("-Xmx1536m", jvm)
        self.assertIn("-XX:MaxDirectMemorySize=512m", jvm)
        self.assertFalse(any("RAMPercentage" in flag for flag in jvm))
        properties = dict(line.split("=", 1) for line in (ROOT / "trino/config.properties").read_text().splitlines() if line and not line.startswith("#"))
        budget = lambda key: int(re.fullmatch(r"(\d+)MB", properties[key])[1])
        query, headroom = budget("query.max-memory-per-node"), budget("memory.heap-headroom-per-node")
        self.assertLess(query + headroom, 1536)
        self.assertEqual(512, query)
        self.assertLess(budget("query.max-memory"), budget("query.max-total-memory"))
        self.assertLess(1536 + 512 + 256, 3 * 1024)  # leave native/JVM non-heap reserve

    def test_both_configurations_mount_readonly_only_on_server(self):
        compose = (ROOT / "compose.yaml").read_text().split("\n  trino:\n")[1].split("\n  trino-smoke:\n")[0]
        self.assertIn("memory: 3G", compose)
        for name in ("jvm.config", "config.properties"):
            self.assertIn(f"source: ./trino/{name}\n        target: /etc/trino/{name}\n        read_only: true", compose)
        self.assertIn("483@sha256:", compose)


if __name__ == "__main__":
    unittest.main()
