# MLI-02 — Real build DAG acceptance

Ngày nghiệm thu: **24/10/2026 (Asia/Ho_Chi_Minh)**. **PASSED — technical
component**, task MLI-02 Done; không là nghiệm thu scientific/catalog thật.

## Phạm vi và provenance

- Branch `feat/mli-02-real-export-orchestration`, từ main `f3386ba` đã gồm
  PR #96/#97; worktree riêng, không commit/push/merge tự động.
- Owner task vẫn rosy179; reviewer unassigned, chưa có approval độc lập.
- Real Airflow LocalExecutor + Java17/Spark3.5.9 + MinIO/REST Catalog/Trino;
  DAG manual `mli_02_build_dataset`, không trigger daily hoặc Colab.
- Dùng lại **Published historical synthetic fixture** của
  [MLD-05](./MLD-05_RUNTIME_ACCEPTANCE.md): 10 bounded ETL batches, 225 tháng
  REPRODUCTION, 227 Gold events. Không reingest nguồn hoặc ghi Gold lần này.
- Gold namespace `iceberg.mld05_qa_ce10b138154447978d8007fa25b64ffe`;
  ML namespace mới `iceberg.mli02_ml_qa_20261024_b79d72`.
- Publication `pub_f113a14782d74d2b5c9895a45a7c24f9a1a183e84c616043cb4501c145f2a6b6`;
  exact Gold snapshot **9079127511755137392** (bigint, không qua float).
- Cutoff `2026-10-21T11:08:02.336Z`, config `mli02-synthetic-qa-v1`. Binary cuối
  có cache, đồng thời là code_version của request:
  `5bdae6a5fb8344036ac09a57cddda6fa88939267e0b2c0168b06d3a067ce755c`.
- Dataset cuối `ds_570abe6c95b4c4c7b5eeaff2a194717eef057b4a8a12d706b5832f7c5574df65`.
- Prototype trước cache dùng binary `5d90c13133bd50daa2b961d2328028f01a9ad6d5f883853f8cbb2924c6554de7`,
  dataset `ds_e28be2ef875c4aff0ae83db0d96bf6cda53c5707c2b957d26bd44faa9cb13dbe`.
  Đổi code_version tạo ID khác là đúng contract, không coi prototype/final là
  hai rerun của cùng identity. Giữ nguyên cả hai dataset QA và logs.

## Deployment an toàn

Trước triển khai: active DagRuns=0, lease absent, daily paused, MLI DAG paused;
các services healthy. Dùng base Compose của checkout deployment + override
current-month cũ + ignored override `staging/mli02-deploy-20261024/compose.override.yaml`
ở worktree MLI. Chỉ recreate ba Airflow services với `--no-deps --no-build
--pull never`, mount JAR/DAG read-only; không build/pull/init/reset/delete.
Daily tiếp tục pause; operational mode disable trong phiên MLI để không vô ý
chạy daily bằng deployment đang nghiệm thu. Sau terminal đã pause MLI và khôi
phục deployment cũ khi active=0/lease absent. [Readback restore](./MLI-02_DEPLOYMENT_RESTORED.json)
xác nhận cả daily/MLI paused, mode operational và JAR ban đầu
`cd30d98d09808d1ca617b866c57bcff00bd0aef1f70cefe3fd806146e00aa51d`.
Không xóa run, lease foreign, bundle, namespace, logs hoặc dữ liệu nguồn.

Đây là **explicit mounted-artifact acceptance**, không khẳng định default
cached-image fingerprint/preflight đã đạt cho source mới. Không đưa ignored
override, raw fixture, Parquet hoặc private logs vào Git.

## Kết quả runtime

- Prototype `mli02-qa-20261024-first`: **12/12 success**, collector PASSED,
  12 Trino readbacks, 2 mainshocks/10 features, sáu Gold pins không đổi.
  Feature thiếu cache chạy lâu; safety guard đã từ chối terminate vì phase
  vừa hoàn tất, nên **không JVM nào bị kill, không clear/retry/mark-success**.
- Final first `mli02-qa-20261024-v2-first`: **12/12 success**, collector PASSED,
  12 Trino readbacks, export phase reused=false; feature phase 154,15s, total
  1179,88s. Trino đã thấy VALIDATED 2/10 trước export, không promote sớm.
- Rerun `mli02-qa-20261024-v2-rerun`: **12/12 success**, collector comparison
  PASSED, cùng dataset ID/ba snapshot pins/ba SHA; export phase reused=true.
  Mỗi bảng mainshock/candidate chỉ một snapshot mang dataset identity, không
  duplicate. Sáu Gold pins giữ nguyên; 12 SQL readbacks đạt.
- Negative `mli02-qa-20261024-v2-rejected`: **failed đúng thiết kế** ở resolve,
  log có DS_GOLD_NOT_PUBLISHED; 11 downstream upstream_failed. Không tạo dataset
  staging/context hoặc lease; registry vẫn 2 rows (prototype + final), bundle
  đã xuất giữ checksum. Không sửa state thành success để làm xanh Graph.
- Local copy bằng EXP-01 OFFICIAL loader: **VERIFIED_EXPORTED_BUNDLE**,
  10 rows, credentials_required=false, scientific_approval=false; expected
  manifest/checksums SHA lấy từ receipt ngoài bundle, không tự tin file đi kèm.
- Sau các gate trên, chuyển task/index/kế hoạch sang Done; reviewer unassigned,
  không tự điền approval. Chưa commit/push/merge hoặc tạo PR mới.

### Docker shutdown và recovery — không là first-attempt SLA

Trong rerun, Docker Desktop không còn chạy. Chỉ mở lại ứng dụng Docker,
không reset/build/pull/init hoặc thay dữ liệu. Airflow tiếp tục validate ở
**try 2** và success; các task khác try 1. Attempt validate gốc không có
receipt/outcome hoàn chỉnh vẫn được giữ, không suy đoán exit code hoặc viết
lại logs. Source lease vẫn đúng owner rerun cho đến successful handoff tự release.
Không dùng clear/retry/mark-success/sửa DB thủ công hoặc takeover/release foreign
lease. Đây là rerun/recovery acceptance có ghi lịch sử gián đoạn, không gọi là
clean first-attempt/on-time benchmark.

### Pins của final first/rerun

| Pin | Giá trị decimal/string chính xác |
|---|---|
| Registry EXPORTED snapshot | 6757670165059348278 |
| Mainshock snapshot | 6167499692394470988 |
| Candidate snapshot | 5625340169105266542 |
| Bundle manifest SHA-256 | d6d62d45efb8a071a9eb0a15c4cf348415a6abc2dcf50b0fb83eb0e34120ab6e |
| Public checksum index SHA-256 | c5dc70d93076bda3ff77bb2933c499fa3b40fe238de5ddd127d760342c92f42c |

Metadata kiểm lại được:
[prototype](./MLI-02_RUNTIME_ACCEPTANCE.json),
[final first](./MLI-02_BUILD_FIRST.json),
[rerun/recovery comparison](./MLI-02_BUILD_RERUN.json),
[negative input](./MLI-02_REJECTED_INPUT.json),
[local loader](./MLI-02_LOCAL_VERIFY.json),
[restore](./MLI-02_DEPLOYMENT_RESTORED.json).
Snapshot bigint trong JSON không đi qua JavaScript/float; các export pins dùng
decimal strings. Collector chỉ SELECT/checksum và không tạo missing run summary.

## Test/check đã đạt

- Python/Airflow full suite cuối: **473 PASS**; scoped real-adapter/collector
  module **23 PASS**, gồm default-real/no legacy fallback, invalid/private/
  oversized config, missing/bad runner pin, timeout/foreign lease, receipts,
  checksum/release và read-only collector không tạo missing summary.
- Java build/package với JDK21/Spark3.5.9: **29 PASS**, 0 failure/error/skip:
  DatasetBuildJobTest(4), DatasetManifestTest(8), FeatureExportCoverageTest(8),
  FeatureExportBundleTest(9). Spark cần loopback socket; lần sandbox từ chối
  socket không phải test PASS, đã chạy lại có quyền và đạt.
- Legacy MLI-02 graph/mock boundary: 26 PASS, không coi là live evidence.
- CI policy: 39 PASS; full inventory có nhóm mli-02-build và Python ml-qa.
- Export CLI: 4 PASS. Task status/tuần 5, docs links, config hygiene,
  `make test-contracts` và `git diff --check` đạt tại mốc trước terminal.

## Ranh giới kết luận

Synthetic fixture chỉ chứng nhận tích hợp hệ thống, không chứng nhận catalog
nghiên cứu toàn kỳ, completeness địa chấn, huấn luyện hay scientific approval.
Giữ synthetic_component_qa=true, research_ready=false, scientific_approval=false,
Colab ready/auto_triggered=false. **Chưa có task riêng** cho backfill/nghiệm thu
catalog thật toàn kỳ nghiên cứu; MLD-05 component export không thay proof đó.
Baseline/Colab parity **EXP-02**, adaptive scientific gates **EXP-03**,
full ML E2E **MLQ-01**, report **MLI-04** còn theo task riêng.
