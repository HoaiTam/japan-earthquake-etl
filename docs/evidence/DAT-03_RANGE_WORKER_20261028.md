# DAT-03 — Range worker / frozen inputs, 28/10/2026

Branch `feat/dat-03-range-worker`, base main `327a2756671b526861844f0ad463fa18d47c4b32`
(PR #106). Bổ sung implementation, không chỉ đổi readiness flag.

## Đã kiểm

- Python17tests cho exact78payload/84runtime files, external manifest pin,
  mutation/missing h2024/symlink/duplicate JSON, disjoint assignments,
  code dependency pin, first-month gate, foreign lease, failed-session block,
  success own release và failure/pending giữ lease. Lifecycle tests là mocks,
  **không phải** receipt actual Spark/Trino.
- Java49pure tests: ResearchRangeStateTest6, ResearchRangeRecoveryTest1,
  ResearchCatalogStateTest7, ResearchCatalogBatchPlanTest5,
  ResearchUsgsCaptureCheckTest9, GoldRangeNamespaceTest2, GoldOutsideBucketTest6,
  GoldVerifiedReuseTest5, GoldFactBucketTest8. Maven package exit0; không full
  Spark fixture suite. Contract/split/checkpoint/namespace/quality guards đạt.
- Team setup11tests và task status77files đạt. Make target mới luôn có trong
  ci-docs-base; full Java vẫn theo impact policy.
- `make ci-docs-base` đạt, gồm docs/contracts, source/metadata/security/runtime
  mocks, CI42tests, static Compose/MinIO/Trino và secret hygiene; không build/start Docker.
- Real frozen pack từ catalog `b6db356fb30e46f9a26a419ebacb5393`, original
  request SHA `ad306633ac438b33276cfa3cf5e797482cc78e139410db3385863bbd3c9501fb`.
  Không fetch nguồn mới, không thay cutoff/pins/source namespaces.
- Upload immutable, `rclone check --download`: **79 matching, 0 differences**.
  Tải fresh folder khác và repo verifier: `FROZEN_37_INPUTS_VERIFIED_NOT_RUNTIME_ACCEPTANCE`.
- Prepare ThanhTris request `1b37d98cbe374214bfa091f7e8fcbd20` và rosy179
  `518e511ad1af430aa22d53db6a0da5fb` trên máy owner: Java standalone validation
  đọc actual37sources, checks USGS query/halo/HTTP/bytes/featurecount/summary
  và JMA inventory/provenance đạt. Không ghi Bronze/Silver/Gold hoặc lấy lease.
- ACL đọc cả79file +folder: user-only,4permission entries, canDownload=true
  theo tài khoản owner. Không anyone/domain, không thay ACL. Số4 là actual
  hiện tại, không giả định3 từ release trước; chưa chứng nhận đúng identity
  của hai thành viên hoặc việc họ đã tự tải được.

## Release nguồn đã có trên Drive

- [Folder](https://drive.google.com/drive/folders/1EuR7uISq_EhCkKX7NCWJPwkJtnD0PbZ6)
- Remote `dat03_team:data/dat03/shared-inputs/range-inputs-v1/e0752283a868`.
- Manifest SHA `e0752283a868c601b3f2f996ace0cb77cd2e869f9d2a4b729a76a1b2b54fcc25`.
- 78payload files/130206607bytes +manifest:25JMA ZIP,25metadata,
  12USGS GeoJSON,12metadata,2inventories,plan,summary.
- Cutoff `2026-10-25T06:13:04.747Z`; JMA2024 chỉ boundary. Đây là original
  ETL input transport, không source completeness/Gold/official ML acceptance.
- Không upload code chưa review, credentials, capacity private logs hoặc JAR.

## Runtime thật sau khi catalog cũ terminal

Catalog owner session `b9ed8dbd1fd64a97bf303348a05eff2a` đã exit0/own release,
accepted141/288 đến2011-09; host CAS capture hoàn tất
`runtime-observation-2889197b1116450eb01f4d916154d982`, manifest SHA
`27467d9b7772672c10226a9f3e3c07fb92282599a0c4fd49a4f8e49a6059fb98`.
Sau đó preflight idle mới cho phép test range, không chạy hai ETL đồng thời.
Không start/restart/build Docker, không thay cron hoặc runner catalog cũ.

### First-month failure được giữ và publication-only recovery

- Range test `0ffa89ec97af42a19031233d59553841`, session
  `800acf27697b484788b88a7cd435eb22`, tháng2013-08.
- Bronze/Silver/Gold đã commit,133,564s; Verify bị namespace policy cũ
  `DAT03_ISOLATED_CATALOG_REQUIRED`. Không gọi failure là accepted.
- Vá GoldPublisher cho exact isolated `dat03_range_<32hex>`, vẫn chặn shared/
  arbitrary namespaces. Recovery bytecode chỉ đổi GoldPublisher, thêm entry
  ResearchRangeRecoveryJob; mọi ETL bytecode gốc khớp. Test chống đổi/xóa code đạt.
- Recovery `550aebb08c664855a647b92d7dcc45b7` chỉ Verify/Publish,28,221s,
  **173SQL**,13.066canonical và13.066bridge, six current pins đạt, postflight
  unchanged/idle và own lease RELEASED. Failure files/source/context giữ nguyên.
- Recovery request SHA `8984279c5fed38efe8ff5eb50645f071175c9516a867f342d7f409120b832cc7`;
  result SHA `6504b6de0df16330e85538f2a96f2a7b0086c3818c27488ea95e74e51f05277f`.
- Đây là `PUBLICATION_RECOVERED_QUARANTINED_TEST_RUN`, không giả acceptance
  normal session cũ hoặc cho resume JAR lỗi. Không xóa dữ liệu/namespace test.

### Fresh runner và resume bình thường

Range `987bd30759ea416c8d38d348bb59cf1c`, owner label rosy179, **chạy trên máy
HoaiTam**; namespace riêng, không receipt máy rosy179 hoặc catalog đã merge.
Request SHA `c059d26d82c8a771fc36705bcf48d3cea14821b8b4488b3668862140dc1e7631`.

| Phiên | Tháng mới | Accepted | Canonical / bridge mỗi bảng | Thời gian |
|---|---|---:|---:|---:|
| `1815da8dc74c46898e4334b06d1940e9` | 2018-10 | 1/63 | 21.929 | 121,398s |
| `101e92f067954a4d8cdab2798d9f2cee` | 2018-11 | 2/63 | 45.826 | 162,950s |

Cả hai exit0/no timeout,148reported checks mỗi publication (không gọi là
148SQL executions), six snapshots, contiguous record chain, own release và
`RANGE_HOST_POSTFLIGHT_PASSED`. Resume giữ input/request/namespace, dùng baseline
của tháng trước; outside-scope checks đạt. `report` kiểm lại và lưu receipt host.

- Acceptance SHA tháng10: `bc2b70c07ae13b373d339fbcd93527a8357a1a2b1a84732c56411338a82fb8a1`.
- Acceptance SHA tháng11: `64ab8ae1e2a35d9a25075e049dd28076c52fb2720bf6fda5d9356035fe38c1fc`.
- Result SHA cuối: `61923d89a54ccb0ca2928f5022827089090dc1647b4fa5c8c57225ec210a4eb5`.
- Dừng sau test2tháng; không mở tiếp12tháng hoặc cộng test vào global141.

## Chưa được ghi là hoàn tất

**DAT-03:** range runtime chưa nghiệm thu trên máy Trí/rosy179; chưa portable
six-table export/import/merge, full288, official two-split ML export hoặc
ba máy receipts. Worker cho phép first-month opt-in với sourceguard, không
cho rollout12tháng từ trạng thái chưa có một tháng accepted. Giữ In Progress.

**SEC-01:** recipient/account acceptance vẫn phải do từng người thực hiện.
Không giả receipts của họ bằng hai prepare trên máy owner.

Runbook thực thi: [DAT-03_RANGE_WORKER](../runbooks/DAT-03_RANGE_WORKER.md).
