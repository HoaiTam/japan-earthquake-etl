# DAT-03 — Chạy portable importer phần 1

Đọc [contract](../specs/DAT03_PREPARED_CATALOG.md) trước. Tool chỉ tạo local
PREPARED input; không publish Gold hoặc official ML export. Actual status theo
[evidence](../evidence/DAT-03_PORTABLE_IMPORT_20261115.md), không theo tên file.

## Điều kiện

- Original metadata/follow-up, byte inventories, worker completed/readback SHA
  chains đã xác minh; baseline CAS accepted163 tháng và snapshots còn đọc được.
- Bronze relocation plan đã pin, physical sources vẫn khớp; giữ cutoff gốc.
- Java17/Spark runtime/dependencies đúng pins, các service cần thiết healthy;
  không có ETL/Java/DagRun/Trino query active hoặc source lease.
- Host physical free ít nhất10 GiB khi mở phiên; giữ reserve4 GiB trong phiên.
  Không dùng free space logic trong Docker VM thay host reserve.
- Không tự build/pull/init/start/recreate/reset hoặc xóa warehouse/source/evidence.

## Unit checks và build

```bash
make test-dat03-portable
make test-dat03-import-spark
./mvnw --batch-mode --no-transfer-progress -pl spark -am -DskipTests package
```

Spark fixture target cần JDK17 và không cần Docker/MinIO. Host JDK mới hơn có
thể compile release17 và chạy pure metadata tests nhưng không thay acceptance
Spark17. CI tự đưa Java tests package gold vào suite Gold và Python tests vào
docs/contracts. Không chạy Spark tests đồng thời actual importer.

## Config và lệnh thực tế

Config môi trường `dat03-portable-import-preparation-v1` cần:
`catalog`, `observation`, `observation_sha256`, `baseline_session_id`, `bronze_plan`,
`bronze_plan_sha256`, prebuilt `runner` và hai `workers` theo thứ tự ThanhTris /
rosy179. Mỗi worker chứa `owner`, `readback_root`, trusted `completed_sha256`,
`start_month`, `end_month`. Config không cần `.env`/token; runtime dùng credential
đã có, không in ra terminal hoặc đưa vào bundle.

CLI hiện có:

```bash
python3 -B scripts/dat03-portable-import.py --help
```

Các mode `prepare`, `run`, `resume`, `capture` đều yêu cầu `--folder` exact operation root;
`prepare` thêm `--config` và trusted `--config-sha256`. Folder mới có tên32hex,
parent staging đã tồn tại. Không trỏ vào warehouse/bucket hoặc thư mục khác.
`run`/`resume` dùng `--seconds` từ600 đến6000. Lệnh đã chạy trên owner nằm trong
evidence; máy khác phải có đúng input pins, không copy đường dẫn local rồi coi
là đã setup. Không đặt một default job tự chạy khi gọi test hoặc Docker startup.

## Checkpoint và lỗi

1. `prepare` kiểm CAS/worker chains và original archive pins; tạo request + exact
   assets inventory. Không đọc source API, không write lakehouse.
2. `run` preflight idle/healthy, stage exact files, tạo **own preparation lock**
   trong operation root; không chiếm/release source global lease.
3. Java ghi từng checkpoint sau Parquet SHA/size/footer/count đạt. Metadata liên
   kết request SHA/previous checkpoint SHA và exact affected months.
4. Gần deadline dừng ở `CHECKPOINTED` sạch nếu có thể; host chỉ release own
   preparation lock sau success + services/idle unchanged + metadata capture.
5. `resume` chỉ nhận các host session đã có acceptance pin khớp. Nó không tự retry
   failed/timeout/unknown outcome hoặc overwrite partial output.
6. Đủ batch: global checks, final catalog và independent verification. Rerun
   catalog hoàn tất kiểm lại immutable files, không transform/append lại dữ liệu.

Khi error/unknown: giữ execution/private logs/checkpoint/own lock, chỉ chẩn đoán
read-only. Không unlink lock bằng tay để vượt gate. Nếu lỗi đã xác định cần code
mới, dùng attempt mới với request/runner pin mới, giữ attempt cũ làm evidence;
không dùng code mới để tự nhận attempt cũ đã pass. Không tự recover source lease.

Host thiếu reserve sẽ yêu cầu dừng đúng process group do phiên này tạo. Không
restart/stop service Docker, đổi cron hay sửa Airflow DB. Log terminal chỉ lọc
`event=portable_import_*`; không in raw SQL/env/payload/private SDK logs.

### Mất bộ theo dõi host không đồng nghĩa worker failed

Lệnh đọc progress là optional, timeout20 giây không được kill/retry worker hoặc
release lock. Chờ worker gốc terminal; không gọi `resume` trong khi còn Java.
Host lưu `intent.json` trước launch với input pins và service IDs/start/restarts.

Mode `capture` chỉ thu/verify **existing successful session**, không launch ETL.
Nó yêu cầu exact `--session`, staged input SHA, runtime idle/healthy, worker exit0
không timeout/stop, candidate/report/execution SHA, metadata capture và services
không đổi trước release own preparation lock. Partial capture/failed worker cần
review, không tự ghi đè. Không coi observation timeout là quyền ETL retry.

Attempt cũ chưa có host intent chỉ được capture với `--preflight` và externally
trusted `--preflight-sha256` của receipt service có trước attempt. Không tự tạo
preflight giả sau lỗi; ID/start/restarts phải khớp nguyên vẹn. Capture không thay
receipt/marker nguồn và không release source global lease.

## Nhận biết thành công

- `portable_import_session_accepted`, report `PREPARED` đủ27/27 batch và file
  catalog SHA đã pin; `CHECKPOINTED` chỉ là prefix, không phải full acceptance.
- `reconciliation.balanced=true`, global/boundary checks thật đạt trên toàn rows.
- Own preparation lock được release sau postflight; không còn job/query active.
- Rerun `reused=true`, catalog SHA không đổi. Metadata host acceptance/execution
  giữ nguyên để audit. Không nhận `Published`/`EXPORTED` từ bước này.

Phần tiếp theo thuộc [DAT-03](../task/tasks/DAT-03.md): six-table Gold/Trino
publication và hai official exports; các gate MLD-05/MLI-02 không bị bỏ qua.
