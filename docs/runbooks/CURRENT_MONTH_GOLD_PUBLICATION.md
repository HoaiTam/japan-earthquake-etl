# QA-01 — Gold và publication current-month

## 1. Phần này làm gì và dùng để làm gì?

Đây là **bước 2/3** của mở rộng current-month trên PR #96, tiếp nối
[Source/Silver](./CURRENT_MONTH_SOURCE_SILVER.md). Protocol riêng
`current-month-gold-v1` đưa Silver đã verify vào sáu bảng Iceberg, kiểm đúng
snapshot bằng Trino rồi mới công bố **operational provisional**.

Không đổi cron, không bật planner `DESIGN_ONLY`, không hạ gate
`COMPLETE_MONTHS` của historical v1. [QA-01](../task/tasks/QA-01.md) vẫn In Progress:
component tests không thay MinIO/Trino live acceptance hoặc scheduled E2E.

| Thành phần | Có gì / dùng để làm gì |
|---|---|
| `CurrentMonthGold` | Revalidate source/Silver SHA chain; pin request/baseline; scope Gold; commit → verify → publish → receipt/marker |
| `OperationalGoldPolicy` | Reserved namespace và policy bất biến; cấm tự promote research/ML |
| `GoldInputReader` operational overload | Đọc cumulative bundle nhưng chỉ transform observations/memberships thuộc affected months, không replacement tháng bằng delta ba ngày |
| `GoldIcebergWriter` operational overload | Dùng lại six-table snapshot/recovery; pin policy vào identity, commit, table properties và từng snapshot summary |
| `GoldPublisher` explicit operational mode | SQL blockers trên literal snapshots; status row/views/receipt có quality; legacy publisher từ chối operational |
| `CurrentMonthGoldJob` | Explicit MinIO/REST Catalog/Trino runner, không refetch source, yêu cầu shared lease đang giữ |
| Research consumers | MLD-01 identity/manifest, MLD-02/03 pinned resolver và MLD-05 exporter từ chối namespace operational; snapshot guard chặn cả pin đã bị đổi tên trong metadata |

## 2. Namespace và sản phẩm

Namespace chỉ nhận `<configured_catalog>.operational_current_month`, không nhận
`gold`, `gold_qa_*` hoặc namespace caller tùy ý. Trong đó có:

- `event_current`, `event_source_bridge`, `dim_date`, `dim_region`,
  `dim_magnitude_band`, `dim_depth_band`.
- `publication_status` riêng; versioned serving views `v_<bundle_sha_prefix>_*`
  và stable alias `earthquake_event_current` riêng.
- Các serving views có `coverage_mode=OBSERVED_AS_OF`,
  `publication_quality=PROVISIONAL`, `research_ready=false`,
  `ml_upstream_eligible=false`. Status row còn pin source policy, hai cutoff
  và toàn bộ policy JSON. Không sửa schema của historical status table.

Table property `gold.data_policy=current-month-gold-v1` không được thay bằng
policy lịch sử. Mỗi snapshot có `gold.operational_policy_sha256`; SHA này còn
được query độc lập qua Trino. Có `PASSED`/`Published` **không** làm dữ liệu đủ
điều kiện nghiên cứu, không chứng nhận JMA current-month hoặc source completeness.

State dưới `${SILVER_PREFIX}/current-month/` trong data bucket:

| Key sau prefix | Nội dung |
|---|---|
| `gold-state/<operation>/request.json` | Canonical request; retry khác request/code/pin bị chặn |
| `gold-commits/<operation>/identity.json` | Scope, sáu baselines, schema/count/content fingerprints, exact policy |
| `gold-commits/<operation>/commit.json` | Sáu snapshot IDs đã Spark readback; `published=false` |
| `gold-verification/<operation>/publication.json` | Trino SQL + actual/expected, policy và publication ID; chỉ có sau gate/readback |
| `gold-state/<operation>/receipt.json`, `_SUCCESS` | SHA chain Silver → commit → publication; marker ghi cuối và readback |

## 3. Request và baseline

Đây là **schema mẫu**, phải điền pins thật; không chạy nguyên placeholder:

```json
{
  "contract_version": "current-month-gold-v1",
  "operation_id": "cm1-<40 hex từ Silver receipt>",
  "registry_sha256": "<64 hex từ Silver receipt>",
  "namespace": "iceberg.operational_current_month",
  "baseline_publication": null,
  "code_version": "<40 hex commit của bản build>"
}
```

- Bootstrap: Silver request cũng phải `baseline=null`; sáu baselines Gold đều
  0. Nếu namespace đã có snapshot không thuộc operation này thì dừng, không
  coi đó là dữ liệu được phép overwrite.
- Lần kế tiếp: `baseline_publication` có exact `operation_id` và
  `publication_sha256` từ Gold receipt trước. Component đọc publication/commit
  đã persist, lấy đủ sáu snapshots; không nhận baselines do caller đoán/latest.
- Parent Silver operation/registry phải trùng parent Gold policy; không ghép
  Silver của một chain với Gold publication khác. Chưa hỗ trợ skip một đoạn
  unpublished chain hoặc promote một cumulative Silver bất kỳ làm bootstrap.
- Hai cutoff, observed coverage, source policy và Silver receipt SHA đều từ
  verified state; request không được truyền eligibility/completeness override.
- Gold không gọi USGS lại. Source capture/config thay đổi phải có operation
  mới theo [contract](../specs/CURRENT_MONTH_AS_OF_CUTOFF.md).

## 4. Commit, verification và retry

1. Kiểm exact registry/Silver receipt/selection/raw manifests, quality và
   reconciliation. Source state còn history nhưng Gold chỉ lấy current.
2. Lọc **trước transform** theo những tháng đã capture đầy đủ đến cutoff.
   Count Gold phải khớp số current IDs trong source index thuộc scope đó.
3. Commit sáu bảng bằng writer GLD-03. Chỉ overwrite affected month partitions;
   giữ nguyên rows/run IDs ngoài scope, dimensions chỉ append thành viên mới.
   Sáu bảng không atomic cùng nhau: caller phải giữ shared whole-run lease.
4. Trino kiểm schema/count/unique/lineage/UTC-JST/dimensions/source counts,
   snapshot operation/identity, ngoài scope không đổi; thêm policy SHA của cả
   sáu snapshots, USGS-only/ROI/event cutoff và từng month coverage boundary.
5. Chỉ sau blocker và freshness mới tạo versioned views/status row; readback
   đúng policy/count trước đổi **operational** stable alias. Historical aliases
   không được chạm vào.
6. Rerun reuse exact commit/snapshots/publication, vẫn revalidate/readback.
   Rerun publication cũ không kéo alias về sau. Partial commit resume theo
   snapshot operation marker, không recommit bảng đã thành công. SQL fail giữ
   artifacts và lease cho operator xử lý, không xuất success receipt giả.

ML guard ở `DatasetManifestWriter` kiểm reserved namespace trước mọi SQL/write,
đồng thời yêu cầu exact event snapshot **không** có operational policy summary.
`DatasetIdentityPlan`/`pinnedGoldSql` chặn reserved physical table; pinned audit,
window và official export resolve lại publication qua manifest writer. Một
caller gắn `ml_upstream_eligible=true` hoặc đổi metadata sang tên historical
không bỏ được guard snapshot persisted. Không biến pilot thành official export.
Historical snapshots cũ thiếu policy key dùng `element_at(summary,key) IS NULL`,
không dùng map subscript gây lỗi khi key vắng; xem [Trino map semantics](https://trino.io/docs/current/functions/map.html).

## 5. Cách test và chạy component

Từ gốc repo, dùng JDK17/21:

```bash
make test-current-month-gold
make test-current-month
make test-qa-01
```

Gold tests dùng source fixture → Parquet và **Iceberg local thật**; Trino adapter
ở đây là fixture protocol, không phải nghiệm thu SQL trên service thật. CI tự
discover hai test classes mới trong suite `gold`; thay shared Gold contract
vẫn full regression, không skip consumer tests theo tên task.

Đường runtime opt-in, **chỉ sau khi** caller hợp lệ đã có Source/Silver trong
MinIO và giữ exact shared source lease của cùng run:

```bash
make prepare-current-month
make current-month-gold CURRENT_MONTH_REQUEST=staging/<owned-run>/gold-request.json
```

- Không auto build/start/pull/init Docker. Chuẩn bị JAR/classpath là bước riêng;
  source/dependency thay đổi thì phải prepare lại, không sửa prepared receipt.
- `JAVA_HOME`, MinIO/data bucket/Silver prefix, catalog/warehouse, Trino và
  `SOURCE_GUARD_ROOT` lấy từ environment hợp lệ. Host runner chỉ đọc **public**
  USGS defaults từ `.env.example`, không dùng credentials mẫu hoặc tự load `.env`.
- Java dùng local Spark `local[1]`; endpoints phải truy cập được từ process đó.
  Endpoint nội bộ `minio:9000`/`iceberg-rest:8181` không tự trở thành host ports.
  Khi chạy trong container đã chuẩn bị, có thể dùng Spark/JAR entrypoint
  `ie212.earthquake.spark.CurrentMonthGoldJob <exact-request.json>`.
- Job không acquire/release lease. Không tự tạo `owner.json`, takeover owner,
  chuyển local file URIs thành s3 URIs, copy/sửa manifests để giả MinIO input.
  **Source/Silver local evidence của bước 1 không trực tiếp làm MinIO handoff.**
- Log private 0600 ở `staging/current-month-logs/`; stdout chỉ in controlled
  acknowledgement/count/URI sau Java readback. Không commit log/raw/build/secret.

Thành công: exit 0, `event=current_month_gold_component_passed`, status Published,
PROVISIONAL/OBSERVED_AS_OF và hai eligibility flags false; exact six snapshots,
publication journal và receipt marker có thật. Khi kiểm qua Trino, pin
`publication_id` lấy từ journal rồi đọc `publication_status`; so policy/count/
snapshot với receipt, không chỉ thấy một alias tồn tại là PASS.

## 6. Giới hạn có tên để theo dõi

- **QA-01 — Daily E2E:** [bước 3 Airflow wiring/collector v2](./CURRENT_MONTH_DAILY.md)
  đã triển khai/deploy; scheduled 22/10 failed ở resolve trước ghi lake, đã sửa
  SDK enum. Còn same-run JMA historical readiness/capture/Gold/Trino acceptance;
  owner giữ lịch 07:15 chờ 23/10, không ghi Done trước report PASSED.
- **QA-02 / QA-04:** regression fixture/recovery mới ở component không thay
  live replay/failure evidence cho operational scheduled chain.
- **ORC-06/ORC-07, GLD-03/GLD-04:** historical v1 giữ nguyên; Done của v1 không
  chứng minh v2 đã được deploy. Work package current-month chưa có task ID riêng.
- **Chưa có task riêng:** JMA sau baseline, automatic ID/deletion readback,
  replan ngoài hai tháng, compaction/archive vượt state budget. Không bỏ guard.

Xem [evidence triển khai](../evidence/QA-01_GOLD_PUBLICATION.md).
