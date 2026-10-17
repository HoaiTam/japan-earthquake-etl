# QA-03 - JMA historical backfill

Status: **PREPARED**

Matrix SHA-256: `0d1449e05fb5cbe9a3ce664abdc8f0cc870d28b8b04f27f0f5854370f45f9c7b`
Inventory SHA-256: `61dc2c73f1bb9cee48fba851cbb941cc62c893201c36db583cdd55ae19f8199a`

Selected months are representative coverage; they do not prove the full historical period.

| Year | Segment | Month | Archive identity | Required coverage archives |
|---:|---|---|---|---|
| 1997 | jan-sep | 1997-09 | `h199701.zip` | h199701.zip, h199710.zip |
| 1997 | oct-dec | 1997-10 | `h199710.zip` | h199710.zip |
| 2000 | full-year | 2000-01 | `h2000.zip` | h2000.zip |
| 2023 | full-year | 2023-09 | `h2023.zip` | h2023.zip |

## Runtime cases

| Case | Status |
|---|---|
| resume_after_failure | PENDING_RUNTIME |
| reuse_verified_archive | PENDING_RUNTIME |
| changed_release | PENDING_RUNTIME |
| same_release_different_bytes | PENDING_RUNTIME |
| retention_outside_scope | PENDING_RUNTIME |
| resource_guard | PENDING_RUNTIME |

Capacity fields are null until measured by the runtime adapter.
ORC-07 real adapters are required before a Bronze-to-Published PASS.
