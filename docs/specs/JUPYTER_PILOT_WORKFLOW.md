# DAT-02 — Jupyter pilot trước Google Colab

## 1. Quyết định và trạng thái

Theo yêu cầu ngày 2026-10-14, phát triển **Jupyter local trước, Colab sau**.
Tài liệu này là yêu cầu triển khai, chưa phải evidence đã có dataset/notebook.
[DAT-02](../task/tasks/DAT-02.md) mới Backlog; các implementation MLD-04/05,
EXP-01/02 vẫn chưa hoàn tất. Không có lệnh chạy mới để hướng dẫn ở mốc này.

Mục đích pilot là kiểm tra hướng xử lý/feature/grain/thuật toán và portability,
không chứng minh clustering tốt hoặc completeness toàn kỳ nghiên cứu.

## 2. Hai loại bundle — không trộn lifecycle

| Thuộc tính | Development pilot DAT-02 | Dataset nghiên cứu MLD-05 |
|---|---|---|
| Identity | pilot_id riêng, source_dataset_id chỉ lineage | dataset_id trong ml.dataset_manifest |
| Artifact kind | DEVELOPMENT_PILOT | Official dataset export |
| Readiness | allowed_usage=DEVELOPMENT_ONLY, research_ready=false | Audit/candidate/feature/coverage gates trước VALIDATED rồi EXPORTED |
| Manifest | pilot_manifest.json + coverage_report.json | dataset_manifest.json |
| Coverage | COMPLETE hoặc PARTIAL theo window và nguồn thực tế; luôn báo limitations | Phải đạt scope/audit rule nghiên cứu đã version hóa |
| Result marker | pilot_result_manifest.json, không official _SUCCESS.json | Result bundle theo MLI-01 với _SUCCESS.json |
| Import | Không eligible MLI-03/serving ml.* | MLI-03 verify EXPORTED/IDs/checksums trước CANDIDATE |
| Runtime | Jupyter; chỉ đưa sang Colab sau local smoke đạt | Cùng code đã test local; Colab sau local gate và official export gate |

Đây là artifact/test workflow mới, **không thêm trạng thái PILOT** vào
CON-03, không sửa BUILDING/VALIDATED/EXPORTED hay result lifecycle. Parent
BUILDING pilot không được đưa trực tiếp cho official experiment loader.
Exporter nội bộ tạo bản development có allowlist schema/lineage rõ;
notebook test đọc bản này, không đọc hoặc thay metadata nội bộ đang BUILDING.

## 3. Dataset nhỏ có những gì?

Mặc định chọn cố định 1–3 mainshock JMA thuộc reproduction, rule depth 50–200
km/magnitude>5.5. Candidate giữ hết trong input scope, không sample từng row.
Nếu chưa đủ pre/post window như pilot hiện tại, ghi PARTIAL, missing ranges và
selection bias. Không “cắt” công thức window cho vừa slice dữ liệu.

Feature 4-D do Java/Spark MLD-04 tạo; notebook Python chỉ validate/load/fit/plot.
Grain/field semantics theo CON-03; không latitude/longitude degrees trực tiếp
vào Euclidean model và không thêm magnitude vào vector.

Proposed artifact layout (chưa phải file có sẵn):

    ml-pilot/pilot_id=<pilot_id>/
      features-part-*.parquet
      mainshocks.parquet
      pilot_manifest.json
      coverage_report.json
      checksums.sha256

Manifest version dat-02-pilot-v1 bắt buộc có pilot_id, artifact_kind,
allowed_usage, research_ready=false, source_dataset_id, Gold publication/table/
snapshot pins, candidate/audit/feature input references + SHA, selection config/
code/feature/scaling versions, selected mainshock IDs, schema hash, row counts,
coverage status/missing ranges và limitations. File references tương đối,
không wildcard/latest/credential/signed URL/local account paths.

Pilot identity hash dựa vào canonical pins + selection/config/version; không
dựa timestamp/retry/path. Checksums tính bytes mọi file bắt buộc, không hash
chính checksums.sha256. Rerun cùng identity giữ logical contents, không trộn
parts; serialization/layout/checksum reproducibility phải có test.

Không upload full Iceberg warehouse hoặc MinIO credentials. Data lớn ở
staging/local download, Git chỉ notebook nguồn/config/evidence metadata và
small synthetic fixtures. Notebook output lớn phải clear trước commit.

## 4. Trình tự và ownership

1. MLD-04 (rosy179): khóa feature implementation/schema/fixture.
2. DAT-02 (HoaiTam): chọn pins/scope, tạo pilot feature bundle và Jupyter
   inspection evidence; không cần full research dataset EXPORTED.
3. EXP-01 (rosy179): shared Python module + thin Jupyter notebook + package
   lock; loader tách explicit development/official, schema/checksum gates.
4. EXP-02 (rosy179): local Window/DBSCAN/HDBSCAN global per-mainshock;
   output counts/noise/failure/runtime/plots và local smoke report.
5. Khi local gate đạt, chuyển cùng module/notebook/config/checksum bundle
   sang Colab; không viết lại feature hoặc có hai model implementations.
6. Official experiment chỉ dùng MLD-05 EXPORTED dataset; import MLI-03
   vẫn giữ exact candidate lineage và lifecycle gates.

Không dependency vòng: DAT-02 chỉ phụ thuộc MLD-03/04; EXP-01 local harness
phụ thuộc DAT-02/CON-03, không đợi MLD-05. EXP-02 final research acceptance
vẫn cần MLD-05, nhưng local development stage được thực hiện trước.
MLD-05 không phụ thuộc DAT-02/EXP tasks; MLI-02 build và MLI-03 fixtures không
phải chờ Jupyter pilot để phát triển.

## 5. “Test ổn” để chuyển Colab nghĩa là gì?

EXP-02 lưu local-smoke-report.json với pilot_id/checksum, code/package/config
versions, case results, counts/reconciliation, runtime, coverage/limitations,
local_smoke_status=PASSED/FAILED và colab_migration_allowed boolean.
Đây là báo cáo kỹ thuật, **không scientific APPROVED**.

PASSED chỉ khi:

- Loader reject bundle sai checksum/schema/grain/finite/self presence.
- Notebook chạy restart kernel → run all trên máy từ bundle local, không
  dựa biến/cell đã chạy tay hoặc credential/Docker ở mỗi lần fit.
- Ba baseline cùng pilot/checksum, per-mainshock, không mutate input.
- Case mainshock noise trả no productive sequence đúng reason; không chọn
  cluster khác để làm đẹp kết quả. Noise nhiều/không có POST không tự là lỗi.
- Feature units/scales/zero-self/PRE/POST đúng; membership/summary/counts
  đối soát, error/skipped windows được ghi reason, không giấu failure.
- Chạy lại cùng input/config tái lập trong tolerance đã chốt; nếu algorithm/
  package không deterministic, report phân biệt invariant vs label permutation/
  numerical tolerance và lý do. Không yêu cầu cluster labels giống máy tuyệt đối.
- Size/runtime/memory được ghi; guard/resource failure được xử lý minh bạch.
- Output chỉ development, không _SUCCESS.json/RESULT_READY/CANDIDATE.

FAILED không cho migrate bằng cách bỏ test hoặc đổi fixture expected cho xanh.
PARTIAL pilot có thể PASSED kỹ thuật, nhưng report phải ghi không đủ coverage
để kết luận khoa học; không dùng nó tune/approve cho toàn reproduction/extension.

Colab pilot smoke sau PASSED dùng **cùng pilot bytes**, code/config/package lock;
đối soát invariants/counts/roles và numerical/label tolerance, báo khác môi trường.
Research run cần thêm MLD-05 export hợp lệ. Official _SUCCESS chỉ cho official
result hoàn tất, không được lấy pilot_result_manifest đổi tên làm official.

## 6. Handoff và giới hạn

- DAT-02: dataset/inspection; EXP-01: loader/environment; EXP-02: local
  algorithms/go-no-go report và Colab smoke; không tính hai lần ownership.
- MLD-05: official export; MLI-02: scheduled build; MLI-03: strict import.
- EXP-03/04/05: adaptive, scientific evaluation và out-of-period còn riêng;
  pilot PASSED không thay các tasks này. DOC-01 cập nhật local/Colab runbook.
- Không sửa task Done upstream chỉ vì thêm development pilot workflow.
- Task files/index là nguồn trạng thái; spec này không tự chứng minh notebook chạy được.
