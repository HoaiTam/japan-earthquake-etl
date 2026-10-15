# DAT-02 — tạo và kiểm tra pilot JMA trên Jupyter

## Mục đích và dữ liệu có sẵn

Pilot kiểm tra **đúng feature/lineage/loader/coverage**, chưa đánh giá model.
Đường đi: MLD-03 candidate đã pin → Java/Spark MLD-04 → pilot Parquet →
loader local → Jupyter inspection. Không đọc raw Bronze hay tự tính feature
trong Python, không tải 40 năm, không chạy lại Docker trong mỗi phép thử.

Selection `config/ml/dat02_reproduction_v1.json` khóa hai mainshock năm 2000
trong REPRODUCTION trước fit. Giữ tất cả 279 candidate theo grain
`(source dataset, mainshock, candidate)`; một window có 1 self, window còn lại
có 278 rows (22 PRE, 255 POST, 1 self). Đây không phải số event độc lập khi
cùng candidate thuộc nhiều window. Window self-only không được loại bỏ ngầm.
Mainshock phải depth 50–200 km, M>5.5; candidates không áp rule mainshock.

Source JMA input thực tế chỉ
`[2000-01-21T14:21:08.760Z, 2000-02-04T14:21:08.760Z)`, không phải toàn năm
dù Bronze ZIP có toàn năm. Vì window khoa học dài hơn nên cả hai window
PARTIAL. Exporter không thu nhỏ window để đổi thành COMPLETE. Coverage là
độ phủ thời gian input, không chứng minh catalog completeness hoặc ground truth.

## Chuẩn bị một lần

### Bộ đã nghiệm thu ngày 2026-10-15

[Evidence metadata](../evidence/DAT-02-pilot-inspection.json) ghi đúng pins/
config/JAR/checksums. ZIP bàn giao ngoài Git:
`staging/dat02-verified-pilot-handoff.zip` (45,907 bytes), SHA256
`b53cd7bfdc3768c3fd0ee0bcc5e037ef1c9c9b007d0df900ec223c8dc139bc3e`.
Bundle gốc và ZIP giải nén đường dẫn khác đều đã restart-kernel/run-all đạt.

Trên chính máy nghiệm thu, không cần export/build lại để kiểm tra:

```bash
make inspect-pilot PILOT_BUNDLE=staging/ml-pilot/pilot_id=pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c
```

Máy thành viên khác nhận ZIP, giải nén riêng và dùng đường dẫn đó, không mặc
định có staging của máy nghiệm thu. Tự prepare/export lại có thể tạo JAR SHA
và pilot_id khác; chọn ID từ receipt của chính lần export đó.

### Tạo lại từ nguồn pin

Từ root repo, chọn JDK 17/21 qua JAVA_HOME (không Java 26). Cần Python 3.13,
Internet để cài dependency lần đầu, Docker **đang chạy** với Trino và accepted
MLD-03 artifacts. Không cần đưa MinIO/Airflow credential vào notebook.

Runtime host này không cập nhật JAR bên trong container. Các image đang chạy
được giữ nguyên; nếu sau đó muốn chạy pipeline/smoke Docker với Java source
mới, chủ động `make docker-build` một lần trước `make docker-up`. Không sửa
image label hoặc fingerprint receipt để lách freshness gate.

```bash
make prepare-ml-local
make pilot-env
make stage-ml-windows
```

Nhận biết đạt:

- `event=local_ml_prepared` và JAR SHA; build chỉ Java local, không Docker image.
- Venv ở staging/venvs/dat02; direct dependencies được pin trong
  notebooks/requirements-pilot.txt. EXP-01 sẽ khóa đầy đủ transitive/model
  environment; DAT-02 chưa tuyên bố Colab portability.
- `STAGED_VERIFIED_WINDOWS`, REPRODUCTION, native receipt SHA
  `9246338b3d754ff70835a5d092efe12fbb5ce479ebe197bdb6197579e56f6353`.
  Rerun hiện `reused=true`; byte thay đổi thì fail, không overwrite.
- Thiếu accepted receipt/data thì dừng và xem runbook MLD-03; script không
  tự sinh evidence, ingest hay restart service để thay thế input thiếu.

## Export một lần, không build lại

```bash
make ml-features-local
make ml-pilot-local
```

Mặc định Trino `http://127.0.0.1:8081`; nếu máy dùng port khác, truyền
`ML_TRINO_URL=http://127.0.0.1:<port-thực-tế>` cho lệnh Make. Resolver chỉ
SELECT Published status, cả sáu snapshot và Gold values của candidate IDs;
không INSERT/CREATE/UPDATE dataset_manifest, không commit Iceberg.

Feature đạt: `feature_ready`, rows=279, invalid=0, Parquet readback verified.
Pilot đạt: `DEVELOPMENT_PILOT_READY`, feature_count=279, mainshock_count=2,
coverage_status=PARTIAL, research_ready=false, source_status_unchanged=BUILDING.
Copy `pilot_id` từ chính receipt của lần chạy. Bundle nằm tại
`staging/ml-pilot/pilot_id=<pilot_id-vừa-in>`; không dùng wildcard/latest để chọn input.

Các biến tùy chọn: ML_WINDOW_INPUT, ML_SCALING, PILOT_SELECTION, PILOT_OUTPUT.
Adaptive dùng `ML_SCALING=config/ml/feature_scaling_adaptive_dev_v1.json`.
Hai preset là development scales chưa tune/approve nghiên cứu. Thay config,
input hoặc JAR hash tạo identity mới. Build lại JAR có thể đổi artifact SHA;
không sửa receipt để giữ identity cũ.

## Năm file và cách nhận biết

| File | Có gì / dùng để làm gì |
|---|---|
| features-part-00000.parquet | Mọi candidate đã chọn, raw relative km/giờ, vector 4-D, scale thực tế/version, source lineage, pilot_id |
| mainshocks.parquet | Main metadata, formula window/radius, Mc và candidate_count; đối soát self/count/coverage |
| pilot_manifest.json | DEVELOPMENT_PILOT/DEVELOPMENT_ONLY, research_ready=false; source BUILDING chỉ lineage, Gold pins 64-bit, SHA audit/Mc/input parts, config/code/schema/count |
| coverage_report.json | Required window và available/missing ranges từng mainshock; PARTIAL công khai, không clip window |
| checksums.sha256 | SHA-256 bytes của bốn file trên; không hash chính nó, loader kiểm exact file set |

Parquet một part cố định, sort theo full grain, timestamp microsecond UTC;
footer chuẩn hóa bằng cùng rule SilverParquetSerializer: chỉ sort metadata
encoding/key-value không có ý nghĩa thứ tự, không sửa pages/schema/rows.
Rule này tránh checksum thay đổi giữa JVM dù dữ liệu giống nhau.
readback semantic checksum/count trước publication local. `pilot_id` là hash
canonical exact input/config/code, không chứa timestamp hoặc path/retry ID.
Rerun tính lại vào thư mục tạm riêng rồi so byte toàn bộ bundle, `reused=true`
khi giống hoàn toàn; không trộn attempts hoặc ghi đè bundle khác.
Export fail có thể để lại `.pilot-prepare-*` riêng để điều tra; không chọn
thư mục đó làm input hoặc xóa rộng staging/lake để chữa lỗi.

## Kiểm tra notebook — hoàn toàn local

Đặt PILOT_BUNDLE bằng **đúng đường dẫn vừa export**, rồi chạy:

```bash
make test-pilot
make inspect-pilot PILOT_BUNDLE="$PILOT_BUNDLE"
make jupyter-pilot PILOT_BUNDLE="$PILOT_BUNDLE"
# Sau khi dừng Jupyter, đóng gói để gửi team:
make pack-pilot PILOT_BUNDLE="$PILOT_BUNDLE"
```

Lệnh đầu: synthetic fixtures (không Docker/API). Lệnh hai: kernel mới,
run all ba code cells; đạt `RESTART_RUN_ALL_PASSED`, `PILOT_INSPECTION_PASSED`,
279 features, 2 mainshocks, một self/vector zero mỗi window, PARTIAL.
Report ở staging/dat02-notebook-inspection.json; source .ipynb không bị ghi output.
Lệnh ba mở server loopback; mở URL có token mà terminal in **chỉ trên máy**,
không gửi token/log credential vào nhóm hoặc commit. Trong Jupyter chọn
Restart Kernel → Run All. Ctrl+C dừng server, không dừng Docker.

Notebook chỉ gọi loader local rồi hiện schema/count/scaling/missing ranges,
không API/MinIO/Docker ở từng cell, không cần pandas. Loader chặn checksum,
schema/type/order, NaN/Infinity/null, duplicate grain, missing/nonzero self,
role/sign/scale mismatch, mất 64-bit snapshot và coverage giả. Có thể chuyển
nguyên bundle + module/notebook cho thành viên khác; không chuyển .env,
warehouse, Maven cache hay credentials. Bundle ngoài Git, chỉ metadata
evidence/notebook sạch/config và synthetic tests vào PR.

`pack-pilot` kiểm lại loader rồi tạo staging/dat02-pilot-handoff.zip, chỉ năm
file bundle, tên tương đối; in SHA256/size để người nhận đối chiếu. Không ghi
đè ZIP khác bytes; dùng PILOT_ZIP=đường-dẫn-mới khi chuyển pilot khác. Người
nhận giải nén vào thư mục local riêng, rồi chọn PILOT_BUNDLE chính thư mục đó;
chỉ cần code notebook/module và `make pilot-env`, không cần Docker/Maven/.env.
Đối chiếu ZIP SHA qua kênh handoff tin cậy: checksum chống byte bị đổi nhưng
không phải chữ ký xác thực người gửi nếu cả dữ liệu và manifest cùng bị thay.

## Bước tiếp theo / giới hạn

- [EXP-01 — Tạo Jupyter harness và chuẩn bị chuyển sang Colab](../task/tasks/EXP-01.md):
  reuse loader này hoặc adapter có tests tương đương; bổ sung official loader,
  result writer, full package lock và Colab path adapter. Chưa có approval reviewer.
- [EXP-02 — Chạy Window, DBSCAN và HDBSCAN global](../task/tasks/EXP-02.md):
  fit ba baseline, report/noise/plots; local model PASSED mới mở Colab gate.
  DAT-02 inspection PASSED **không** phải local model smoke PASSED.
- [MLD-05 — Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md):
  coverage/audit đủ điều kiện, official VALIDATED/EXPORTED và registry riêng.
- [MLI-03 — Validate/import kết quả và commit bảng ML Iceberg](../task/tasks/MLI-03.md):
  importer official phải reject pilot; task này không ghi ml.* hoặc Gold.

Không sinh official _SUCCESS.json, RESULT_READY hoặc CANDIDATE, không thay
source BUILDING, không tuyên bố chất lượng khoa học từ mẫu 14 ngày.
