# QA-01 — Nghiệm thu daily đa nguồn

## 1. Làm gì và dùng để làm gì?

QA-01 kiểm chứng **một** run `orc_02_daily_sources` do scheduler tạo, từ
USGS/JMA → Bronze → Silver full-month → Gold Iceberg → Trino → Published.
Không nối kết quả của các smoke run hoặc namespace khác nhau để tạo một lần
nghiệm thu giả. Output là report JSON/Markdown để team kiểm tra input, counts,
run identity, sáu snapshots và upstream evidence cho MLD-01.

Các lệnh/report trong runbook này là collector historical **`qa-01-v1`**.
Thiết kế operational v2 bên dưới chưa thay behavior hoặc eligibility của nó.

Trạng thái triển khai: collector và test offline đã có. **Chưa nghiệm thu E2E
deployment thành công**; xem [evidence ngày 19/10/2026](../evidence/QA-01.md).
Không thay đổi acceptance criteria để tính ORC-07 `DAG.test` thành scheduled run.

Ngày 22/10/2026, owner chọn bổ sung [thiết kế current-month/as-of-cutoff](../specs/CURRENT_MONTH_AS_OF_CUTOFF.md)
cho daily 2026. Policy/planner mới chỉ **DESIGN_ONLY**, chưa thay collector v1
hoặc cho phép publish current-month. Đã bổ sung [Source/Silver standalone component](./CURRENT_MONTH_SOURCE_SILVER.md)
theo yêu cầu tiếp theo trên PR #96; không phải daily wiring/scheduled acceptance.
Xem [evidence thiết kế](../evidence/QA-01_CURRENT_MONTH_DESIGN.md) và [component](../evidence/QA-01_SOURCE_SILVER.md).

## 2. Các thành phần

| Thành phần | Chức năng |
|---|---|
| `airflow/dags/qa_01_daily_e2e.py` | Module CLI, không tạo DAG mới; đọc metadata Airflow, staging exact run, MinIO và Trino |
| `scripts/qa-01.py` | Preflight service healthy; `compose exec` vào scheduler hiện có; lưu report riêng mỗi lần |
| `make test-qa-01` | Test synthetic identity/receipt/count/SQL/redaction và fail-closed từng phase của ORC-07; không Docker/nguồn thật |
| `make smoke-qa-01 QA_01_RUN_ID=...` | Read-only collector cho exact scheduled run, **không chạy pipeline** |
| `current_month_contract.py` + policy `current_month_asof_v1.json` | Planner metadata offline/design-only; không capture, ETL hay publish; tests nằm trong `make test-qa-01` |

Java/Spark vẫn sở hữu parse/transform/dedup/link/commit/publish. Python ở đây
chỉ đối chiếu metadata và đọc lại KPI qua SQL, không triển khai ETL thay Java.
Không cần build image mới để nạp collector vì `airflow/dags` được bind mount.
Điều này **không** có nghĩa image ETL đang chạy đã chứa Spark/runner mới.

## 3. Điều kiện trước live acceptance

1. Các dependency USG-05, JMA-05, SLV-09, GLD-04, ORC-01, ORC-07 đã Done.
2. Foundation đã được chuẩn bị/khởi động riêng. Collector chỉ cần scheduler,
   metadata DB, MinIO, Catalog và Trino healthy; không tự build/pull/start/init.
3. Deployment daily có đủ **8 task**, image chứa Spark Java/runner ORC-07,
   mode `real` và request key/SHA đã pin đúng phạm vi. Xem
   [runbook ORC-07](./ORC-07_REAL_ETL.md).
4. Có một scheduled run thực thi thành công với exact source readiness summary,
   context, sáu phase receipts/artifacts và publication của cùng operation.
   `manual`, backfill và `DAG.test` không thay bằng chứng scheduler deployment.

### Lưu ý về thời gian và dữ liệu

USGS query của daily là overlap UTC, không phải Airflow scheduling interval.
Ví dụ interval Airflow kết thúc `00:15Z` ngày 01/10 thì query USGS kết thúc
`00:00Z` ngày 01/10; report giữ riêng hai cửa sổ, không ép chúng bằng nhau.

ORC-06/07 v1 chỉ chứng nhận **tháng đã đóng và đủ coverage**. JMA inventory hiện
đến 2023. Không bật `ORC07_DAILY_MODE=real` với request tháng 09/2023 rồi giả định
delta USGS tháng 10/2026 đủ điều kiện Published. Để nghiệm thu cần deployment
bounded có lịch/coverage tương thích được owner chuẩn bị và review trước.
Collector không tự sửa timetable, unpause, thay config hoặc tạo historical
scheduled DagRun bằng SQL.

**QA-01 — Daily E2E** đã bổ sung thiết kế current-month theo lựa chọn của owner,
Source capture/resolver standalone đã triển khai; còn thiếu Gold adapters/publication,
consumer guards và Airflow/collector v2. Chưa có task ID/effort riêng cho work package còn lại hoặc mở rộng JMA
sau baseline. Không bật mode `real` với current-month request theo contract v1;
không dùng lệnh smoke bên dưới để tuyên bố daily 2026 operational đã nghiệm thu.

## 4. Chạy và nhận biết kết quả

Từ thư mục gốc repo, test offline:

```bash
make test-qa-01
```

Chọn exact run trong Airflow UI → `orc_02_daily_sources` → Runs. Kiểm tra
`run_type=scheduled`, interval, graph và 8 task; không chỉ nhìn màu xanh của
DagRun. Không chọn run manual hoặc tạo/sửa task state để làm test xanh.

Lệnh dưới đây chọn một run **đã có** trên máy ngày 19/10. Make chạy preflight
chuẩn về image/config/init trước collector, nên sẽ dừng sớm nếu deployment cũ:

```bash
make smoke-qa-01 QA_01_RUN_ID='scheduled__2026-10-15T00:15:00+00:00'
```

Nếu đang chẩn đoán deployment cũ, có thể đọc evidence hiện có bằng CLI trực tiếp
không đòi image mới (vẫn kiểm tra service healthy). Đây là **diagnostic readback**,
không thay preflight nghiệm thu deployment:

```bash
python3 scripts/qa-01.py --run-id 'scheduled__2026-10-15T00:15:00+00:00'
```

Run lịch sử này dự kiến `NOT_READY` vì có 7 task và chỉ đạt SourcesReady.
`IMAGE_UNPREPARED_OR_STALE_RUN_MAKE_DOCKER_BUILD` từ Make nghĩa là cần chuẩn bị
image riêng trước nghiệm thu; smoke **không tự thực hiện** việc build đó.

Để nghiệm thu run khác, thay giá trị `QA_01_RUN_ID` bằng ID thực tế từ UI.
Không dùng một ID ví dụ chưa tồn tại. Mỗi lần collector tạo thư mục riêng
`staging/qa-01/<UTC>-<token>/`, in đường dẫn `report.json` và lưu `report.md`.
`collector.private.log` quyền 0600 chỉ phục vụ chẩn đoán local; không commit.

| Kết quả | Exit | Ý nghĩa |
|---|---|---|
| `PASSED` | 0 | Scheduled run đủ 8 task, chain/pins/counts/publication và 17 SQL readback đều đạt |
| `NOT_READY` | khác 0 | Run cũ trước ORC-07 hoặc chỉ có SourcesReady, không có real ETL evidence; **không** đủ ML upstream |
| `FAILED` | khác 0 | Run/task/identity/SHA/reconciliation/SQL không hợp lệ, thiếu evidence hoặc lỗi đọc backend |

`QA01_DAILY_DEPLOYMENT_PRE_ORC07`: giữ nguyên task set thực tế của run cũ,
không tự thêm một TaskInstance `real_etl=success`.
`QA01_REAL_ETL_EVIDENCE_MISSING`: run có task set mới nhưng thiếu summary
Published. `make` trả Error 1 trong các trường hợp này là gate hoạt động đúng,
không phải lý do xóa dữ liệu hoặc chạy build lại vô điều kiện.

## 5. Report kiểm tra những gì?

1. Airflow exact `dag_id/run_id`, `scheduled`, interval đóng 24h, 8 task success.
2. Source summary đúng owner/interval, raw+manifest SHA của hai nguồn nằm trong
   resolver request; summary SHA gắn vào ORC-07 context.
3. Operation dẫn xuất từ owner; đọc lại context, từng receipt và artifact đúng
   MinIO bucket/prefix/operation, validate upstream SHA và artifact SHA.
4. BronzeReady exact pins/count; Silver `FullMonthReady/COMPLETE_MONTHS`, request
   SHA và các phương trình đối soát, không chỉ tin `balanced=true`:
   - full records = selected + outside temporal selection, parser rejects = 0;
   - selected = unique revisions + repeated observation IDs;
   - unique revisions = output revisions + outside affected months;
   - output revisions = current + history; memberships = current.
5. Gold commit journal và publication pin cùng sáu snapshots/identity/namespace.
   Verification/publication phải có đầy đủ blocker checks, expected/actual hợp lệ.
6. **17 SELECT tự sinh**, không thực thi SQL lấy tùy ý từ artifact:
   - 6 table counts + 6 snapshot owner/identity checks;
   - affected-month canonical và membership count đối chiếu Silver;
   - natural/ROI count từ pinned event snapshot và versioned serving view;
   - một publication_status row đúng bundle/run/snapshot/count và PASSED.

Canonical toàn namespace, canonical affected months và natural/ROI serving
count là **ba metric khác nhau**. Không so sánh Silver một tháng trực tiếp với
tổng Gold nhiều tháng, hoặc natural/ROI với toàn canonical.
Không đọc alias `earthquake_event_current` mutable để thay pinned snapshot.
HTTP Trino từ chối redirect/cross-origin nextUri, giới hạn size/page/timeout.

MinIO có `silver/real-etl/<operation>/`, `full-month/<operation>/`,
`bundles/<operation>/`, `gold-commits/<operation>/`,
`gold-verification/<operation>/`. Các phase artifacts là proof từ Java, không
phải dữ liệu Parquet mới được QA sinh thêm. Collector không list bucket rộng,
không tải lại USGS/JMA và không viết vào lake.

## 6. Blocker và bàn giao

`make test-qa-01` chạy lại test ORC-07 fail tại từng phase: không gọi downstream,
giữ lease khi timeout/fail và không publish/release theo summary cũ. Các test
collector thêm chống ghép run, sửa SHA/snapshot, thiếu full month, count âm/bool,
parser reject, blocker SQL và KPI drift. Đây là **synthetic regression**, không
ghi là fault injection live.

Để QA-01 Done còn cần scheduled deployment acceptance PASSED, evidence đúng
tầng khi fail và KPI SQL thật. QA-02 — rerun/duplicate/revision, QA-03 — historical
backfill/coverage, QA-04 — failure/recovery live là các phạm vi riêng, không
tuyên bố đã hoàn tất cùng collector này. MLD-05 — export official dataset vẫn
cần lifecycle EXPORTED; `ml_upstream_eligible` không nâng lifecycle dataset.

Phần mở rộng current-month phải có collector/report v2 riêng. Operational
PASSED không được tự cấp `ml_upstream_eligible=true` như historical v1; giữ
`OBSERVED_AS_OF/PROVISIONAL`, ML-ineligible và aliases tách biệt. 17 SQL hiện
hành là check v1, chưa phải evidence/protocol v2.

Khi gặp lỗi: giữ report/run/log/lease hiện trạng, kiểm tra đúng phase với runbook
ORC-07. Không clear/mark-success, xóa warehouse, foreign lease hoặc `down -v`.
