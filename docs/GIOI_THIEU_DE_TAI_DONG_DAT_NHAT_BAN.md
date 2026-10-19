# Giới thiệu đề tài: Nền tảng phân tích dữ liệu động đất tại Nhật Bản

DOC-01 đồng bộ **19/10/2026** theo [baseline PLN-01](./specs/MVP_SCOPE_KPI_AND_DOD.md).
Đây là mô tả sản phẩm và cơ chế; mức đã kiểm chứng xem [known issues](./KNOWN_ISSUES.md),
không coi kiến trúc mục tiêu là MVP đã hoàn tất.

## 1. Mục tiêu

Xây dựng hệ thống Big Data end-to-end thu thập USGS/JMA, bảo toàn dữ liệu nguồn,
chuẩn hóa/revision/source linking bằng Spark Java và công bố canonical events
qua Iceberg/Trino. Từ một Gold snapshot bất biến, tạo dataset/feature để so sánh
Window, DBSCAN, HDBSCAN global/adaptive theo từng mainshock window.
Đầu ra là **sequence/aftershock candidate hồi cứu**, không dự đoán động đất
hoặc khẳng định quan hệ nhân quả vật lý.

## 2. Đầu vào và phạm vi

| Nguồn | Dữ liệu / vai trò | Cần chú ý |
|---|---|---|
| USGS API | GeoJSON cập nhật từ 2023-01-01, query UTC và overlap mặc định 3 ngày | Revision cũ hơn overlap cần scoped backfill |
| JMA bulletin | Raw ZIP theo year/segment/catalog release, inventory 1984–2023 | Không phải daily event API; lưu raw đầy đủ trước biến đổi |
| Boundary enrichment | Tùy chọn cho phân loại địa lý | Thiếu mapping vẫn giữ event UNKNOWN/OFFSHORE, không chặn Core |

Nguồn, bounding box, timestamps/JST/datum chi tiết:
[CON-01](./specs/SOURCE_COVERAGE.md), [JMA inventory](./specs/JMA_ARCHIVE_INVENTORY.md).
Không cần tải hết 40 năm trước khi làm/test. Bronze có thể giữ nhiều raw
revisions; Silver/Gold chọn current/canonical theo contract để không double count.

## 3. Nội dung thực hiện và công nghệ

1. **Bronze trên MinIO:** lưu raw USGS response/JMA ZIP bất biến cùng manifest,
   window/release, checksum, run ID và validation/readback.
2. **Silver bằng Spark Java:** parse/UTC-JST, quality/reject/lineage, dedup revision
   từng nguồn, liên kết observation xuyên nguồn và canonical membership.
3. **Full-month state và Gold Iceberg:** resolver không cho delta thay mất tháng;
   sáu bảng event/bridge/dimensions/bands, commit snapshots có scope/journal.
4. **Trino publish gate:** verify exact snapshots trước PASSED publication và
   serving views; committed chưa phải Published, nhiều bảng không atomic chung.
5. **ML dataset:** pin snapshot/dataset identity, audit/Mc, mainshock/candidate
   windows, đổi tọa độ sang km và scale vector 4-D bằng Spark Java.
6. **Experiment Jupyter trước Colab:** shared Python modules fit Window/DBSCAN/
   HDBSCAN theo từng mainshock; chuyển cùng bundle/code/config sau local gate.
7. **Official result import/report:** validator checksums/schema/grain/lineage,
   commit Iceberg ml.*, Trino/static report Core. Power BI/ODBC chỉ Stretch.

Docker Compose quản lý nền MinIO/Airflow/PostgreSQL metadata/Spark/Catalog/Trino.
Airflow điều phối run/dependency/lease/receipts, không tự biến free Colab thành
job service hoặc xử lý raw thay Java. Không dùng PostgreSQL làm business DB
bản sao; notebook không giữ credential hoặc ghi lake trực tiếp.
[Kiến trúc](./SYSTEM_ARCHITECTURE.md) có sơ đồ và trách nhiệm từng thành phần.

## 4. Quy trình nghiên cứu

Reproduction `[2000-01-01,2018-10-01)`; extension `[2018-10-01,2024-01-01)` UTC.
Mainshock depth 50–200 km, M>5.5, natural/JMA UNIFIED trong study area theo
contract; candidate không bị áp nhầm toàn bộ rule mainshock.
Vector baseline `[x_scaled,y_scaled,z_scaled,time_scaled]`, không lat/lon
degrees trực tiếp và không thêm magnitude làm feature baseline.

Chỉ chọn cluster chứa chính mainshock. Mainshock noise phải giữ outcome/reason,
không chọn cluster gần nhất để làm đẹp. So sánh cùng input/candidate population,
báo noise/probability, DBCV khi phù hợp, physical consistency/Omori, stability,
extension và resource/failure limits. Không dùng accuracy/F1 khi không có
ground truth độc lập; SQL/technical PASSED không phải scientific APPROVED.

## 5. Đầu ra và cách hiểu

| Đầu ra | Ý nghĩa |
|---|---|
| Bronze manifests/raw | Truy ngược đúng source bytes/release/window |
| Silver bundle và full-month proof | Observations/current-history/reject/link/membership đối soát |
| Gold snapshots/publication | Canonical events tránh double count, SQL kiểm chứng |
| Dataset/pilot bundle | Exact input, 4-D features, manifest/checksums, coverage/limitations |
| Membership/sequence summary | Candidate roles/noise/cluster/probability theo dataset/experiment |
| Trino/static report | So sánh có lineage, không thay bằng vài hình cluster hoặc dashboard |

DAT-02 pilot 279 candidate rows/2 mainshock có PARTIAL coverage, development-only.
Official dataset vẫn cần MLD-05 EXPORTED và result/import lifecycle. Hướng dẫn
cho thành viên mới: [onboarding](./runbooks/DOC-01_TEAM_ONBOARDING.md).
MVP chưa đủ scheduled ETL/official ML/report acceptance; không tuyên bố model
tốt hơn hoặc full 40 năm đã tải từ một pilot.

## 6. Tìm đúng tài liệu

[Đặc tả](./specs/PROJECT_SPECIFICATION.md), [daily flow](./flows/DAILY_ETL_PIPELINE.md),
[local runbook](./LOCAL_OPERATIONS_RUNBOOK.md), [Jupyter workflow](./specs/JUPYTER_PILOT_WORKFLOW.md),
[logical ML model](./specs/ML_DATA_MODEL.md), [task index](./task/tasks/README.md).
