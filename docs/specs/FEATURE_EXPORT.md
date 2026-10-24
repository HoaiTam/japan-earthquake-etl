# MLD-05 — Validate, commit snapshot và export feature

## Phần này làm gì và dùng để làm gì

FeatureExportJob nhận đúng ml_namespace/dataset_id đã đăng ký bởi MLD-01.
Nó resolve publication, audit/Mc, window và shared MLD-04 feature engine trên
Gold snapshot đã pin; không nhận rows, Mc hoặc cờ research_ready từ request.
Kết quả là snapshot candidate có feature 4-D và bundle Parquet portable cho
EXP-01/02, cùng trusted registry handoff cho MLI-03. Không chạy clustering.

MLI-02 vẫn sở hữu build DAG/step adapter; có CLI exporter không có nghĩa
MLI-02 đã được nghiệm thu live. DAT-02 giữ development-only lifecycle riêng.

## Gate trước khi ghi dữ liệu

1. Một persisted dataset ở BUILDING/VALIDATED/EXPORTED, identity/config hợp lệ;
   Gold PASSED publication và exact snapshot, cutoff không trước publication.
2. Gold commit SHA phải khớp publication. Full-month registry/receipt/marker
   và Silver bundle của cùng operation phải khớp checksum, namespace, cutoff
   và Silver SHA trong commit. Không dùng event min/max để suy completeness.
3. Complete-month registry phải chứng minh JMA đủ cả split: REPRODUCTION
   225 tháng, EXTENSION 63 tháng. Không nhận coverage_scope hoặc
   research_ready=true thay proof. Thiếu tháng dừng trước lifecycle/data writes.
4. Audit thực sự trên pinned Gold, Mc reliable/finite; filters, window,
   selection và feature_scaling_config nằm trong identity filter_config_json.
   Hai version feature/scaling phải khớp manifest. Không đổi scale sau dataset_id.
5. Không clip window ở biên period rồi nhận COMPLETE. Không cấu hình policy
   mới thì giữ legacy: window vượt period dừng DS_EXPORT_WINDOW_COVERAGE_INCOMPLETE.
   DAT-03 có opt-in strict-split, loại window có giải trình như phần dưới;
   không cho phép đọc candidates ngoài period.
6. Shared engine kiểm ID/grain, mainshock presence/zero-self, role, raw units,
   Mc, finite feature/scale, per-window resource limits và count reconciliation.
   So feature đã tính với kết quả recompute; không silently drop invalid rows.

V1 bounded: tối đa 100.000 feature rows/dataset và 128 mainshock. Vượt giới hạn
dừng, không sample/truncate. Đây không là benchmark hay cam kết xử lý toàn catalog.

## DAT-03 — Strict split và loại window thiếu coverage

Owner chốt ngày 24/10/2026: thêm `filter_config.window_coverage_policy` bằng
`dat03-strict-split-v1`. Field này tham gia dataset identity, không tái sử dụng
dataset ID/export của legacy. Giá trị sai/null/boolean bị reject, không fallback.
Không sửa các window-model version hoặc thông số/caps đã pin của reproduction.

- Chọn mainshock theo audit/Mc/selection cũ, tính **nguyên** PRE/POST bằng shared
  resolver. Chỉ giữ mainshock có `window_start >= period_start` và
  `window_end < period_end_exclusive`. Window endpoints inclusive, nên POST
  đúng period end vẫn thiếu coverage; equality ở start và end-1µs hợp lệ.
- Loại **cửa sổ**, không xóa event khỏi Gold/audited candidate corpus. Event bị
  loại làm mainshock vẫn có thể là PRE/POST candidate của window hoàn chỉnh khác.
  Không mượn extension cho reproduction hoặc cắt ngắn duration để pass.
- `window_coverage` chứa policy/split/period, eligible/included/excluded counts,
  `reason_counts` và từng `mainshock_event_id`, event time, required start/end,
  reason `PRE_OUTSIDE_SPLIT`, `POST_OUTSIDE_SPLIT` hoặc `PRE_AND_POST_OUTSIDE_SPLIT`.
  Counts phải reconcile, IDs unique, lý do phải khớp biên thời gian thật.
  Report metadata tối đa 4096 exclusions, vượt thì fail, không truncate.
- MLD-03 report/readback, MLI-02 window/feature phases và MLD-05 recompute dùng
  cùng policy. MLI-02 receipt chỉ mang count/hash bounded; bundle/trusted manifest
  mang chi tiết. SHA report tham gia export identity; verifier kiểm semantics,
  period/count và identity nên xóa report rồi rehash files không hợp lệ.
- Tất cả window bị loại → report rõ included=0; official export vẫn bị guard
  nonempty chặn, không tạo self window hoặc EXPORTED rỗng cho qua.
- Đây không là missing-source repair: coverage đầy đủ 225/63 tháng vẫn bắt
  buộc trước build. Không biến PARTIAL pilot thành research data. DAT-03 còn
  thiết kế/triển khai collection/sharding và full-period runtime; cap 128/100k
  của mỗi export **chưa** được mở rộng.

## Snapshot và lifecycle

- Commit mainshock_candidate_snapshot và sequence_candidate_snapshot
  bằng Spark/Iceberg, partition dataset_id; bảng candidate chứa feature columns.
- Summary pin ml.dataset_id + ml.export_identity_sha256; retry tìm snapshot
  đã commit, kiểm fingerprint/count rồi reuse. Không append lại cùng dataset,
  không ghi đè dataset khác hoặc sửa Gold.
- Trino đọc exact pins/count và snapshot identity trước VALIDATED.
- Additive metadata trong dataset_manifest: mainshock_snapshot_id (bigint),
  candidate_snapshot_id (bigint), export_identity_sha256 (varchar).
  Không đổi grain, dataset_id hoặc enums CON-03; bảng cũ được ADD COLUMN IF NOT EXISTS.
- Ghi VALIDATED kèm Mc/counts/schema/pins, rồi materialize/readback bundle.
  Chỉ sau đó update EXPORTED và kiểm readback. SQL unknown outcome/partial
  writes giữ lease; không tự reset dữ liệu hay ghi CANDIDATE.
- Rerun EXPORTED verify bundle với exact pinned Iceberg contents và registry,
  giữ nguyên bytes/pins. Artifact thiếu/hỏng hoặc identity conflict thì dừng,
  không regenerate để che corruption.

Resource/coverage/quality gate thất bại trước write không promote parent
BUILDING. Infrastructure failure sau partial commit giữ state/pins để operator
reconcile. Không tự approve hay đổi terminal REJECTED thành active.

## Layout portable và checksum không vòng

    ml-export/dataset_id=<dataset_id>/
      features-part-00000.parquet
      features-part-00001.parquet       (nếu cần nhiều part)
      export-checksums.sha256
      dataset_manifest.json
      checksums.sha256

Features chỉ gồm allowlist 29 cột clustering + lineage của CON-03:
schema/version, ba IDs, candidate time/location/depth/magnitude,
mainshock magnitude, role/self, relative km/giờ, vector 4-D, scales/Mc và
created_at_utc. Không mang thêm raw payload/credential hoặc cột tùy ý.
Magnitude là lineage/filter, không phải phần tử thứ năm của vector.

- Spark materialize từ pinned logical rows, không copy physical Iceberg files.
- Với >10.000 rows tạo nhiều deterministic hash buckets/parts; sort full grain,
  Snappy/TIMESTAMP_MICROS và stable footer. Số bucket không bảo đảm đúng file
  size tuyệt đối; mỗi file vẫn có hard bound 64 MiB, vượt thì fail.
- export-checksums.sha256 chỉ index feature parts. Manifest lưu SHA của index
  này và export_manifest_ref cho trusted registry. Không hash chính manifest
  vào index mà manifest tự tham chiếu.
- checksums.sha256 index tất cả files còn lại, gồm dataset_manifest.json và
  export-checksums.sha256. Receipt bên ngoài pin thêm SHA của manifest và
  public checksums, tránh coi một bộ tự rehash là trusted origin.
- Readback exact file set, schema/hash/count/content trước atomic directory
  install; attempt riêng, không trộn part cũ hoặc overwrite target đã tồn tại.

Trusted registry ngoài bundle gồm <dataset_id>.json và
exports/<dataset_id>/export-checksums.sha256. Sau EXPORTED mới ghim
registry_snapshot_id chứa row EXPORTED và tạo immutable trusted manifest;
candidate/mainshock pins và danh sách lineage đọc từ Iceberg thật. Việc tách
manifest portable/trusted tránh vòng checksum và snapshot tự tham chiếu.
MLI-03 nhận dataset_manifest_ref/SHA từ export receipt, không tự nhận manifest
hoặc cờ EXPORTED do notebook cung cấp. Chưa có handoff hoàn chỉnh thì chưa ready.

## Safety và acceptance

Whole-run source lease bao cả Java/Trino/verify và publication. Nonzero hoặc
timeout giữ exact owner; chỉ release sau khi biết quiescent và metadata gates
đạt. Không build/pull/start/init trong smoke. Không xóa bucket, journal,
snapshot hoặc volume; attempts lỗi được giữ để audit.

Component QA có thể dùng archive tổng hợp đầy đủ temporal scope trên namespace
mld05_qa/mld05_ml_qa riêng; report bắt buộc synthetic_component_qa=true và
research_ready=false. Không gán scientific approval hoặc nâng pilot thật.
Nghiệm thu actual research data toàn kỳ và model/Colab vẫn là gates riêng.

Chi tiết lệnh, expected outputs và recovery ở
[runbook](../runbooks/MLD-05_FEATURE_EXPORT.md).
