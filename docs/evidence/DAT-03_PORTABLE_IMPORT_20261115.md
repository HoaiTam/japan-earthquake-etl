# DAT-03 — Portable importer phần 1, 15/11/2026

## Phạm vi

Owner yêu cầu triển khai phần 1 và cập nhật trực tiếp PR #131 bằng commit/push/
comment. Không merge main, không ghi Gold hoặc official ML export trong phần này.
Branch triển khai `feat/dat-03-portable-import`, từ main `f09ac66` rồi fast-forward
head PR #131 `d77c1d8`. DAT-03 vẫn **In Progress**, reviewer `unassigned`.

Implementation: [contract](../specs/DAT03_PREPARED_CATALOG.md),
[runbook](../runbooks/DAT-03_PORTABLE_IMPORT.md). Đây là contract `PREPARED` mới,
không giả receipt-bound FullMonthReady/SilverReady hoặc source keys của worker.

## Input pins giữ nguyên

- Baseline accepted163 tháng, session `0a65bf45d6eb4207bf2c7a5a117bae7d`.
- Owner request SHA `ad306633ac438b33276cfa3cf5e797482cc78e139410db3385863bbd3c9501fb`.
- CAS observation `runtime-observation-8f66979a46f64e7d87671c7b47a9ed13`, SHA
  `5a77f710655d0b748b79bae63fca65657002ef2130301fb1718c6d90c427f860`.
- Baseline commit SHA `57bf616cd76cd6e9ca6f417ec1b7182910a95da34059369a93dfb28dcb4ed19b`.
- Source cutoff `2026-10-25T06:13:04.747Z`, không lấy ngày chạy import làm cutoff mới.
- ThanhTris completed62 tháng SHA
  `444d5cce731e52aa1506cbd337d9d99b408ca56dc4f8160ddc7d83093fefa1d8`.
- rosy179 completed63 tháng SHA
  `617f630c6ea14a448b469622e9cdd727cd234023f2ed0584fa17d1f09da4bb89`.
- Bronze relocation plan SHA
  `a680b27fdbf2115701e983821e2e6c0e8b63d82591d17559db814f0c8d105ebf`.

Input artifacts nằm tại ignored staging trên owner; không commit archive/Parquet/
runner/private logs hoặc credential. Original follow-up và byte readback đã kiểm
ở [evidence trước](DAT-03_PORTABLE_READBACK_20261114.md), physical mapping và
capacity ở [15/11](DAT-03_CAPACITY_PROVENANCE_20261115.md).

## Tests đã chạy

- Maven offline package/release17: 38 Java pure contract/Parquet/provenance/
  Silver readback/capacity tests PASS. Host JDK26 không được coi là Spark17 gate.
- 48 Python portable metadata/planner/controller/checkpoint tests PASS, gồm chặn
  unknown/failed session resume và đổi trusted catalog SHA sau full completion.
- 42 CI impact/suite/workflow tests PASS.
- Docs link check0 problems; task status81/81 metadata/tracking/index khớp;
  CFG-01 repository/sample secret hygiene PASS (checkout không có local `.env`,
  không suy thành security audit cho deployed credentials).
- Actual Java17/Spark3.5.9 fixture: 14/14 cases PASS, exit0, 29,937 giây,
  services unchanged, không lake writes. Operation
  `import-fixtures-cd419ba6de074205bfeac72ac51df5ef`.
- Fixture gồm boundary Jul/Aug2013, exact duplicates/counts, empty input,
  conflicting canonical/observation/current revision, null primary, missing/wrong/
  ambiguous links, wrong count, physical relocation, wrong raw SHA, Parquet readback.

Fixture attempt đầu không chạy thành công vì test classes chưa shade dùng method
signature Jackson khác runner shaded (`NoSuchMethodError`). Đã dùng unshaded
classes JAR cùng namespace cho fixture Java17; production importer vẫn dùng
runner shaded đúng pin. Giữ cả hai executions, không coi attempt lỗi là PASS.

Actual attempt đầu `f9d929aad648430a8d9e307138fcd337`, session
`b63d6e6b9cc24251998d97a94846174f`, exit1 sau16,131 giây, không timeout:
`PROVENANCE_RERUN_CHANGED`. Nguyên nhân so JsonNode integer width sau persist
(`IntNode`/`LongNode`), trước batch Parquet và trước mọi Gold write. Đã chuyển sang
so exact deterministic persisted bytes và thêm regression; không nới source SHA
hoặc thay input để pass. Attempt lỗi/own preparation lock/logs giữ nguyên.

## Actual data acceptance

Attempt mới `787ab335a1b54a83bf0d565031b0fb2a`, request SHA
`5a4d858231efbb111f2b0313e240e5f7c24b3a884912c6b3e9274dcdba070b23`, session
`0af41186f8554564be9813cfdc9e99a5`. Chỉ dùng scheduler/MinIO/REST/Trino/PostgreSQL
đang có, không build/pull/recreate/start thêm container hoặc thay deployed runner.

Trong batch25, optional Docker progress probe timeout20 giây làm host controller
cũ thoát. Actual worker Java vẫn active, services vẫn healthy, checkpoint258 tháng
không bị sửa, source lease trống. Đã gắn lại **read-only observer** vào đúng session,
không resume/launch ETL. Worker gốc sau đó chốt checkpoint268 tháng và chạy batch26.
Host swap quan sát khoảng6,98 GiB/8 GiB, worker vẫn có CPU/tiến triển; không restart
hoặc tăng heap để vượt gate. Thêm regression progress timeout non-fatal và mode
capture chỉ nhận existing terminal success, không coi monitor lỗi là worker PASS.

**Actual full PREPARED accepted:** worker gốc exit0, không timeout/stop, runtime
3715,556 giây (~62 phút), không ETL retry hoặc lake writes. Read-only observer
đợi đúng worker terminal; mode capture kiểm input/candidate/execution/session/
metadata SHA, service IDs/start/restarts và runtime idle trước release đúng own
preparation lock. Không release source global lease hoặc lock attempt lỗi cũ.

Catalog SHA: `4e9282c35af384779fc5977c43b740e81a7185c9a9755b2cb117a2fbed75f8e8`.
Runner SHA: `8444a934b52d86a6250bfabe2a8a4977553dfa1beed3b6d9058c7e47e09b7612`.
Worker controller SHA: `3b856338eb76a90d6a649414e023804561b7aa2714a9c9fb8db79112389bf462`.
Host capture controller SHA:
`1b603a33ef081590b286fcbe8044b92043539880dda4f7483f1a490ce7d5e847`.
Hai controller pins khác nhau vì bản vá host observation/capture được bổ sung
trong lúc worker gốc tiếp tục; không thay runner/request hoặc code worker đang chạy.
Service preflight fallback dùng receipt đã có trước attempt, SHA
`5225dcd928676babbb27fc40427e15c6bdf8d0abae494ece863e7f896f1e5cd8`,
không tái tạo intent hoặc DB state sau lỗi để nhận PASS.

| Output local PREPARED | Rows |
| --- | ---: |
| event_current | 4.488.650 |
| event_source_bridge | 4.489.823 |
| dim_date | 8.767 |
| dim_region | 1 |
| dim_magnitude_band | 7 |
| dim_depth_band | 5 |
| auxiliary source_link | 1.976, gồm1.173ACCEPTED |

Đủ27/27 immutable checkpoints,288 tháng0gap/0overlap,225 REPRODUCTION/63 EXTENSION.
Global/boundary/revision/source-link checks PASS trên toàn bộ rows;
`reconciliation.balanced=true`; exact duplicates event/bridge/link đều0;
`4.489.823 - 4.488.650 = 1.173 accepted_links`. Không coi toàn bộ source links là
accepted, hoặc canonical rows là số trận động đất tự nhiên sau filter nghiên cứu.

Sáu dataset refs, auxiliary links và toàn bộ checkpoint metadata/file SHA/size/
Parquet footer counts đã được independent verifier kiểm trước receipt PREPARED.
`gold_published=false`, `official_export_ready=false`, `research_ready=false`,
`ml_upstream_eligible=false`; namespace Gold mới chưa được tạo.

**Independent reuse đã đạt:** session `8a9719be50d84d4d9b6521a2c44a3e76`, exit0
sau14,026 giây, `reused=true`, catalog SHA giữ nguyên. Kiểm lại toàn bộ file refs/
footer/checkpoint chain, không transform/append lại Parquet hoặc ghi lakehouse.
Host cũng đối chiếu catalog SHA với acceptance session đầu, không chỉ tin receipt
mới tự khai. Hai host acceptances đều services unchanged/idle, own preparation lock
released. [Machine-readable pins và execution receipts](DAT-03_PORTABLE_IMPORT_20261115.json).

## Giới hạn / bước tiếp theo

[DAT-03 — Chuẩn bị catalog nghiên cứu thật và bàn giao dataset dùng chung](../task/tasks/DAT-03.md):

1. **Phần1 hoàn tất kỹ thuật:** full PREPARED + independent reuse, catalog SHA
   giữ nguyên. Không chuyển Done toàn DAT-03 vì các phần dưới chưa hoàn tất.
2. Actual six-table Gold commit/publish + Trino readbacks/rerun trong namespace
   mới được duyệt; chưa ghi Gold mới trong đợt part1.
3. Official collection adapter qua MLD-05/MLI-02, REPRODUCTION/EXTENSION EXPORTED,
   all-shard loader/rerun và immutable release; không fake legacy288 checkpoints.
4. Receipts thật ba máy, không tự điền ThanhTris/rosy179. Chưa đổi DAT-03 Done.
