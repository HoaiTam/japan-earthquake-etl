"""Exact-content triage pins: never exempt a changed file or token category."""
import importlib.util
import json
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('triage_audit',ROOT/'scripts/audit-security.py')
audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)

class ReviewedPinsTests(unittest.TestCase):
    def setUp(self):
        self.allow=json.loads((ROOT/'docs/security/SEC-01-synthetic-uris.json').read_text())
        self.paths=[ROOT/'spark/src/test/java/ie212/earthquake/spark/gold/TrinoSqlClientTest.java',
                    ROOT/'spark/src/test/java/ie212/earthquake/spark/silver/JmaFixedWidthParserTest.java']

    def test_current_reviewed_java_rejection_fixtures_match_exact_pins(self):
        for path in self.paths:
            data=path.read_bytes()
            self.assertEqual(['credential_uri'],audit.scan_text(data.decode()))
            self.assertEqual(([],['credential_uri']),audit.triage(['credential_uri'],data,self.allow))

    def test_changed_content_cannot_inherit_fixture_exemption(self):
        for path in self.paths:
            changed=path.read_bytes()+b'\n// changed content invalidates reviewed whole-file pin\n'
            self.assertEqual((['credential_uri'],[]),audit.triage(['credential_uri'],changed,self.allow))

    def test_token_key_or_configured_secret_category_cannot_be_exempted(self):
        for path in self.paths:
            categories=['github_token','private_key','configured_secret_in_content']
            self.assertEqual((categories,[]),audit.triage(categories,path.read_bytes(),self.allow))
