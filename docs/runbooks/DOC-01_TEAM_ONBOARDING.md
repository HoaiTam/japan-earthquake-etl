# DOC-01 — Thành viên mới: từ data flow đến experiment nhỏ

## 1. Hiểu hệ thống trước khi chạy

Đọc [kiến trúc](../SYSTEM_ARCHITECTURE.md) → [daily flow](../flows/DAILY_ETL_PIPELINE.md)
→ [known issues](../KNOWN_ISSUES.md). Contract là nguồn tên field/kiểu/grain;
task/evidence là nguồn trạng thái, không lấy màu CI thay live acceptance.

| Khối | Làm phần gì / gồm gì? | Dùng để làm gì / bàn giao |
|---|---|---|
| Source/Bronze | USGS GeoJSON + JMA raw ZIP, manifest/SHA/release | Raw bất biến và exact pins cho xử lý lại |
| Silver | Parse/UTC/JST, reject/quality, revisions, links/memberships | Observations có lineage; không double count hai nguồn |
| ORC-06/Gold | Full-month proof; 6 Iceberg tables/snapshots | Không thay tháng bằng delta; Trino verify/publish mới serving |
| ML dataset | Snapshot pin, audit/Mc, windows, 4-D features, export | Cùng dataset/candidate population cho mọi thuật toán |
| Experiment | Jupyter trước; Window/DBSCAN/HDBSCAN global/adaptive per-window | Membership/summary/noise/reasons, không causal/prediction |
| ML integration | Hai DAG build/import, strict official bundle gate, ml.* | Dataset/experiment lineage, Trino/static report Core |

Có **hai đường test**: synthetic từ source code, chạy được trên clone mới;
pilot JMA thật từ ZIP bàn giao, không giả định máy mới có staging của HoaiTam.
Cả hai đều development-only, không cần Docker/Java/.env/MinIO credentials.

## 2. Chuẩn bị Python riêng một lần

Các lệnh dưới dùng shell POSIX tại root repo (macOS/Linux), Python 3.11–3.13:

```bash
python3 -m venv staging/venvs/doc01
. staging/venvs/doc01/bin/activate
python -m pip install -r notebooks/requirements-lock.txt
python -m pip check
make test-pilot PILOT_PYTHON=python
```

Expected: pip check không báo broken requirements; tests không failure/error.
Package direct versions đã pin, transitive không khóa toàn bộ. Python 3.13 có
thể build HDBSCAN wheel, cần compiler phù hợp; không bỏ HDBSCAN để làm gate xanh.
Mở terminal mới cần activate lại. Không cài global, không source .env.
Windows dùng venv Scripts/python và lệnh Python tương đương; Make/shell hướng
dẫn ở đây không tự là evidence Windows/Colab đã test.

## 3. Máy mới chưa có dữ liệu: synthetic experiment

Notebook EXP-02 đã có fixture nhỏ **2 candidate / 1 mainshock**. Đây không phải
USGS/JMA thật. Chọn output mới để không ghi đè completed results:

```bash
DOC01_SYNTHETIC_ROOT=$(mktemp -d staging/doc01-synthetic.XXXXXX)
PILOT_BUNDLE= EXP02_OUTPUT_DIR="$DOC01_SYNTHETIC_ROOT/results" \
  python scripts/inspect-exp02-notebook.py \
  --output-json "$DOC01_SYNTHETIC_ROOT/inspection.json"
```

Expected: runner_mode=nbclient, notebook_status=RESTART_RUN_ALL_PASSED,
fresh_kernel_acceptance=true; 7 code cells chạy xong, source notebook không đổi.
Đọc results/local-smoke-report.json: ba algorithms WINDOW/DBSCAN/HDBSCAN_GLOBAL,
counts đúng synthetic input và không có official _SUCCESS.json.
Synthetic PASSED chỉ test plumbing/cases, không đủ lý do chạy nghiên cứu trên
Colab hoặc tự nói dataset thật đã được nghiệm thu.

## 4. Nhận pilot JMA thật từ người bàn giao

Gói đã nghiệm thu DAT-02 ngoài Git: dat02-verified-pilot-handoff.zip,
45,907 bytes, SHA256
`b53cd7bfdc3768c3fd0ee0bcc5e037ef1c9c9b007d0df900ec223c8dc139bc3e`.
Yêu cầu người bàn giao gửi ZIP qua kênh tin cậy và đặt đúng file tại
staging/dat02-verified-pilot-handoff.zip. Nếu chưa có ZIP, dừng ở đây hoặc dùng
synthetic bên trên; Git clone không tự chứa raw/pilot runtime data.

```bash
shasum -a 256 staging/dat02-verified-pilot-handoff.zip
unzip -Z -1 staging/dat02-verified-pilot-handoff.zip
DOC01_HANDOFF_ROOT=$(mktemp -d staging/doc01-handoff.XXXXXX)
unzip -q staging/dat02-verified-pilot-handoff.zip -d "$DOC01_HANDOFF_ROOT/bundle"
export PILOT_BUNDLE="$DOC01_HANDOFF_ROOT/bundle"
test -f "$PILOT_BUNDLE/pilot_manifest.json"
python scripts/inspect-pilot-notebook.py "$PILOT_BUNDLE" \
  --report "$DOC01_HANDOFF_ROOT/dataset-inspection.json"
```

Linux có thể dùng sha256sum thay shasum. **Chỉ giải nén sau khi SHA khớp** và
inventory ZIP đúng năm file: features-part-00000.parquet, mainshocks.parquet,
pilot_manifest.json, coverage_report.json, checksums.sha256; không có path thoát
thư mục. Không chạy lệnh unzip cho ZIP không rõ nguồn hoặc SHA khác rồi tin manifest.
Checksum chống đổi bytes, không xác thực người gửi nếu cả dữ liệu và SHA cùng bị thay.

Expected dataset inspection: fresh kernel, 279 features, 2 mainshocks,
PARTIAL/research_ready=false, self/vector zero đúng mỗi window. Pilot ID:
pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c.
Một window self-only; window kia 278 rows (22 PRE, 255 POST, 1 self).
Input temporal coverage chỉ khoảng 14 ngày, không phải toàn năm 2000.
Không có network/data service access trong notebook inspection.

## 5. Chạy ba baseline trên đúng pilot đó

Trong cùng terminal/venv, sau bước 4:

```bash
test -d "$PILOT_BUNDLE"
EXP02_OUTPUT_DIR="$DOC01_HANDOFF_ROOT/exp02" \
  python scripts/inspect-exp02-notebook.py --pilot-bundle "$PILOT_BUNDLE" \
  --output-json "$DOC01_HANDOFF_ROOT/exp02-inspection.json"
```

Hai report khác nhau:

- exp02-inspection.json: xác nhận fresh kernel, 7 cells, hash source notebook.
- exp02/local-smoke-report.json: **gate thật** gắn pilot_id/fingerprint,
  package/config, grain/count reconciliation và kết quả từng thuật toán.

Expected: mỗi algorithm xử lý 279 candidates / 2 windows; pilot_id và feature
SHA phải đúng, coverage_status=PARTIAL, research_ready không được nâng.
Mỗi result folder có memberships.parquet, sequence_summary.parquet,
experiment_metrics.json, experiment_config.json, requirements-lock.txt và
pilot_result_manifest.json. Không có official _SUCCESS.json.

Mốc DOC-01: WINDOW 255 POST candidates; DBSCAN có một mainshock-noise window;
HDBSCAN_GLOBAL có hai mainshock-noise windows/no productive sequence. Không
đổi params/cluster để ép kết quả đẹp. Đọc reason/counts khi thấy failed_windows;
technical PASSED không có nghĩa mọi window có productive sequence.

Fallback isolated_exec/FALLBACK_EXEC_WARNING, fresh_kernel=false hoặc error:
**chưa đạt fresh-kernel gate**, dù script exit=0. Không tự sửa JSON report.
Nếu quên PILOT_BUNDLE/path sai, notebook có thể dùng synthetic; phải đối chiếu
pilot_id/checksum và 279 rows thay vì chỉ nhìn chữ PASSED.

Chạy lại bằng output root mới (mktemp), giữ nguyên input bytes. So count/grain/
roles/noise/probabilities theo tolerance; run IDs/timestamps mới không cần giống
byte. Không overwrite completed bundle để làm retry. Muốn tự export lại data
thật từ Published pins: [DAT-02 runbook](./DAT-02_JUPYTER_PILOT.md), có Java/Trino
và upstream prerequisites riêng, không bắt mọi người export lại để test model.

Adaptive đã có notebook EXP-03; test riêng bằng make test-exp03 trong venv.
Đọc [task EXP-03](../task/tasks/EXP-03.md) và report thực tế; không dùng historical
fallback report làm Colab acceptance hoặc tự nhận adaptive tốt hơn.

## 6. Mở Jupyter để xem tương tác

```bash
python -m jupyterlab --ip=127.0.0.1 --no-browser
```

Mở notebook exp_02_baseline_algorithms.ipynb từ repo/notebooks, chọn đúng kernel
của venv, đặt PILOT_BUNDLE và EXP02_OUTPUT_DIR từ terminal **trước khi start
Jupyter**, rồi Restart Kernel → Run All. Giữ runner headless bên trên làm
evidence gate; interactive notebook mặc định không tự chứng nhận runner_mode.
Không gửi URL token/log Jupyter vào nhóm, không commit outputs/raw data.
Ctrl+C dừng Jupyter, không cần dừng Docker.

## 7. Local → Colab: chuyển gì và chặn gì?

Chỉ tiếp tục sau actual EXP-02 local report có PASSED, fresh_kernel_acceptance=true,
colab_migration_allowed=true, đúng bundle fingerprint và đủ ba algorithms.
Đây là cho phép thử portability, **không phải bằng chứng Colab đã chạy**.

1. Ghi commit code, Python/package versions, configs và SHA bundle/report.
2. Chuyển cùng checkout/shared modules/notebooks/configs/test helpers cần cho
   notebook và cùng pilot bytes lên môi trường Colab/Drive. Đặt working directory
   ở root checkout; không viết lại feature/model theo một pipeline khác.
3. Cài requirements-lock.txt trong runtime Colab thích hợp; ghi version thực tế.
4. Verify local gate bằng ColabPathAdapter.verify_local_smoke_gate; dùng
   resolve_paths để chọn input/output thay đường dẫn tài khoản cá nhân hard-code.
   Với fixture local không có input thật, không tự coi là pilot research gate.
5. Restart/run-all và so exact pilot/checksums/grain/count/roles/noise cùng
   tolerance numerical/label permutation. Lưu report môi trường riêng.

Không chuyển .env, keys, warehouse, private logs hoặc Maven cache; không ghi
trực tiếp MinIO/Iceberg từ notebook. MLD-05/MLI-02 đã có technical component
acceptance; catalog thật toàn kỳ chưa có task nghiệm thu riêng. **EXP-02 —
Colab parity/official run** vẫn chưa hoàn tất; không bypass bằng đổi tên marker.

## 8. Official build/import và lifecycle

[MLI-02](./MLI-02_DATASET_BUILD.md) điều phối pin→audit/Mc→window→feature→export;
[MLI-03](./MLI-03_RESULT_IMPORT.md) nhập kết quả official. Cả hai là DAG độc lập
daily ETL, không tự trigger Colab. MLD-05 cần VALIDATED/EXPORTED với registry/pins
thật trước official result _SUCCESS.json. Import technical pass mới CANDIDATE;
APPROVED cần scientific review. Pilot marker không được rename thành _SUCCESS.
Static report Core còn [MLI-04](../task/tasks/MLI-04.md), toàn trình ML còn
[MLQ-01](../task/tasks/MLQ-01.md); Power BI chỉ Stretch.

Cập nhật 24/10: [MLI-02 real build DAG](../evidence/MLI-02_RUNTIME_ACCEPTANCE.md)
đã nghiệm thu technical component first/rerun/reject/local loader, không còn
chỉ mock boundary. QA là synthetic 225 tháng; catalog thật toàn kỳ chưa có
task nghiệm thu riêng, EXP-02/03 và MLQ-01 vẫn giữ gates độc lập.

## 9. Bàn giao và bằng chứng

Gửi team: commit, input bundle ID/SHA, model/config versions, inspection/local
smoke reports, counts và limitations; không gửi credential/private logs.
[Evidence DOC-01](../evidence/DOC-01.md) ghi các lệnh thực tế đã chạy. Kiểm tra docs:

```bash
make check-docs
make test-docs
make check-task-status
```

check-docs chỉ kiểm Markdown link/heading nội bộ offline, không xác nhận external
URL hoặc service runtime. DOC-01 Done không làm QA-01/MLD-05/MLI/EXP/SEC Done.
