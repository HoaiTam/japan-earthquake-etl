# SEC-01 — Review secret và bề mặt truy cập

Owner: Trang. Status: In Progress. Reviewer: unassigned.
Hard dependency ORC-05 is Done; its runtime evidence was measured on a different
host and is not a security certification for this Windows checkout.

## Scope and evidence

`scripts/audit-security.py` is read-only and reports categories, repository-relative
paths or Git object IDs, never matched secret values. It scans current tracked /
nonignored files, reachable blobs on all local Git refs, project-local .env and
logs in airflow/logs and staging (excluding Maven wrapper cache). Blob/file guard
is 2 MiB; skipped coverage is explicit. Unreachable objects, reflogs, external
accounts/Drive, Docker volumes and logs on another host are outside this scan.
High-confidence token/key/credential-URI patterns do not detect every secret.

Compose config is captured in memory using --no-interpolate --no-env-resolution;
only port/role findings are serialized, never environment contents. This is the
declared config, not proof of deployed bindings. Four permitted host ports are
loopback: MinIO console 9001, Airflow API 8080, Spark master UI 8080, Trino 8080.
MinIO API 9000, Catalog, Spark RPC and metadata DB remain internal.

Eight initial URI findings were manually traced to deliberate URI-rejection /
redaction tests, not external credentials. The four exact source-content hashes
are recorded in SEC-01-synthetic-uris.json. Normalizing only CRLF/LF avoids host
differences. Changed content is flagged again; token/key categories cannot be
exempted by this triage. This records reviewer work on patterns, not independent
security approval. See [redacted report](../evidence/SEC-01-static.json).

## Checklist / result

| Check | Evidence / status |
|---|---|
| Current source + reachable Git blobs | No untriaged match in listed pattern scope; counts in JSON report |
| Project-local logs | 9 files scanned; not container named-volume logs |
| .env Git ignore + sample config | .env ignored/untracked, absent locally; existing CFG-01 checker validates placeholders |
| Host ports / root credential consumers | Four loopback endpoints declared; root keys only MinIO/init |
| Prefix policy source | compose/minio/init.sh limits ListBucket to configured prefixes; Bronze omits DeleteObject; Silver/warehouse mutable |
| Actual allow/deny permissions | Pending on running project stack; static policy is not deployed permission evidence |
| Runtime credentials/logs/defaults | Pending local .env strength and live redaction tests; no values in this report |
| Colab/export/import boundary | Pending actual EXP-01/MLD-05/MLI-03 artifacts; no future certification |

## Repeat checks

With Python 3 (standard library only), from repository root:

    python scripts/audit-security.py --output staging/sec-01-report.json
    python -m unittest discover -s tests -p test_security_audit.py -v

In this session Python is the bundled Codex executable (system Python stub fails):

    & 'C:\Users\ADMIN\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' scripts/audit-security.py --output staging/sec-01-report.json

Exit 0 reserved for complete audit (not implemented); 1 findings/error;
2 means static scan clean with incomplete coverage. Never translate 2 into Done.
Existing repository checks:

    & 'C:\Program Files\Git\bin\bash.exe' -lc './scripts/check-config.sh'
    & 'C:\Program Files\Git\bin\bash.exe' -lc './scripts/check-config.sh --require-local'

Only run --require-local after project .env exists. Do not source or print it.

## Remaining work / issues

- [SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md): on running
  stack, compare deployed bindings and env role boundaries, scan scoped container
  logs, check distinct nondefault credentials, deny anonymous/cross-prefix access
  and allow expected pipeline reads/writes using a disposable exact QA prefix.
  Keep test operations scoped; never delete bucket/warehouse/volumes. Bootstrap
  mc admin commands currently pass credentials as process arguments; review this
  exposure and supported stdin/config alternatives before declaring runtime gate
  passed. Do not put the values or commands containing them into evidence.
- [EXP-01 — Tạo Colab harness và khóa môi trường thí nghiệm](../task/tasks/EXP-01.md):
  notebook/handoff actual permissions/token isolation still unavailable.
- [MLD-05 — Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md): inspect actual
  export/config/lock/bundle for secret/account-path leakage when implemented.
- [MLI-03 — Validate/import kết quả và commit bảng ML Iceberg](../task/tasks/MLI-03.md):
  validate actual received bundles, no external credential, durable registry and
  quarantine/publication boundaries.

If a real secret is found in history: revoke/rotate first; coordinate approved
history cleanup. Removing only the current file is insufficient. This PR performs
no credential rotation, permission mutation, deploy or data removal.
