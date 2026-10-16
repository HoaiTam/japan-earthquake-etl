# QA-03 - JMA historical backfill

Status: **PREPARED**

Matrix SHA-256: `033d1d4ddc792787e7bf9d1a90593558570bcf6ac8ceab161ee65e33f2babfe6`
Inventory SHA-256: `f599b442b2b4a3e024adf2b7baa2b27f731a7638ab121af5233ec5f4572d1963`

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
