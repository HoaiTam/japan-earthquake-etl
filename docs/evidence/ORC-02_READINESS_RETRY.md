# ORC-02 — bản vá JMA HEAD retry và readiness diagnostics

## Phạm vi và trạng thái

- Ngày 2026-10-13; branch `fix/orc-02-jma-readiness-retry`, base latest
  `origin/main` `f832a70`. Assignee HoaiTam, reviewer unassigned.
- Bổ sung resilience/diagnostics cho source gate; không thay Bronze contract,
  không ghi Gold Published hoặc tự hồi phục run của người dùng.
- ORC-02 chuyển Review vì **còn chờ kiểm thử live bản vá**, không phải chờ
  approval độc lập. Evidence thành công 2026-10-08 được giữ nguyên làm lịch sử.
- Runbook: [SOURCE_SCHEDULE_AND_READINESS](../specs/SOURCE_SCHEDULE_AND_READINESS.md).

## Lỗi được đối chiếu

Evidence người dùng: `20261013T065118Z-a7a2718dec45`. Metadata read-only:

- DAG `orc_02_daily_sources`, run `scheduled__2026-10-13T00:15:00+00:00`,
  `data_interval_end=2026-10-13T00:15:00+00:00`, state FAILED.
- Resolve/acquisition thành công; `jma_readiness` FAILED; USGS và source
  summary chưa chạy; release thành công. Không còn `owner.json` lúc kiểm tra.
- JMA 1997 hai segments đã UNCHANGED/verified; năm 2000 full-year trả
  `PUBLICATION_OR_STATE_FAILED`; 2023 chưa được xử lý trong run này.
- Cache ZIP năm 2000 còn đủ 2,892,423 bytes, SHA-256 đọc lại khớp
  `268441420d9c90b1cb96f42934aefdb107a4d12c5947adafbfeb24af597d15c2`.
  Publication pointer tồn tại. Không có bằng chứng phải xóa/tải lại mọi dữ liệu.
- Read-only source HEAD: curl có lần HTTP200, Java17 có lần connect timeout;
  một phép thử IPv4 có HTTP200 nhưng phép thử tiếp theo cũng timeout. Đây là
  bằng chứng lỗi kết nối tái hiện được, **không chứng minh nguyên nhân exception
  gốc**, không đủ căn cứ đổi IPv4/IPv6 hoặc vô hiệu hóa TLS toàn project.

## Bản vá làm gì và vì sao

1. `JmaHeadRetryTransport`: retry HEAD transient theo tổng attempts/backoff
   được validate; exception reason dựa vào class/status, không raw message.
   HTTP permanent/TLS/unknown I/O fail ngay; interruption dừng và giữ flag.
2. Runner trả safe reason, phase và số HEAD attempts. GET và publication không
   được replay; HEAD hết retry vẫn FAILED. Reuse đòi cache SHA + MinIO readback.
3. Python chỉ giữ telemetry whitelist có type/range hợp lệ; task error chỉ rõ
   year/segment/reason. Không thêm credential/raw payload vào XCom/report.
4. QA dùng metadata `data_interval_end` để nhận failed scheduled interval hiện
   tại; dừng ngay yêu cầu operator recovery, không đợi lịch ngày mai hoặc clear.
   Failure report/terminal có scoped task/archive details; unknown error redacted.
5. Cleanup lỗi vẫn cố restore cả hai DAG; giữ reason gốc, không ghi VERIFIED.

## Kiểm tra đã thực hiện

- `make ci-airflow`: **216 tests đạt**, không cài/chạy Airflow runtime cho unit.
- `make ci-docs`: đạt contracts, task status, secret/config hygiene, Compose
  static, security/Docker-lifecycle/CI tests và shell syntax; không đọc `.env`
  riêng hay tải nguồn. Chạy lại sau cập nhật docs/task trước bàn giao.
- Java17 kiểm tra trong container tạm, bản sao source/fixture/cache Maven,
  `--network none`, 1 CPU/2 GiB; không chạm `spark/target` hoặc JAR trong
  containers dự án. Maven offline `-pl spark -am test`: **347 tests đạt,
  0 failures/errors/skipped**, compile 119 source files với `release 17`.
  Host Java26 không được dùng để build/test project.
- Sau tests, Maven offline `-pl spark -am -DskipTests verify`: đạt bước
  package/dependency-copy/shade; JAR chỉ tạo trong thư mục tạm. Đây là hai
  bước test + package, không tuyên bố đã chạy full `clean verify`/image build.
- Regression mới: transient HEAD rồi success vẫn reuse exact manifest, không
  thêm GET/write; hết retry không mở store/GET; GET failure không replay;
  TLS/permanent HTTP/interrupt/bounded backoff; local HTTP server 503→200;
  failed current interval không trigger/unpause; valid scheduled report reuse;
  invalid report, error redaction và cleanup failure không giả VERIFIED.
- Đã import **bản harness mới** trong Airflow container và gọi chỉ các hàm
  SELECT/scoped result read, **không gọi `main()`**. Kết quả thực tế:

```json
{
  "reason": "SCHEDULED_RUN_FAILED_RECOVER_REQUIRED",
  "dag_id": "orc_02_daily_sources",
  "run_id": "scheduled__2026-10-13T00:15:00+00:00",
  "failed_tasks": ["jma_readiness"],
  "archive_year": 2000,
  "archive_segment": "full-year",
  "runner_reason": "PUBLICATION_OR_STATE_FAILED"
}
```

Metadata cũ không có `http_head_attempts`; không tự điền số attempts hay đổi
reason lịch sử thành timeout. Không cộng các lần chạy test lặp thành số tests.

## Giới hạn và nghiệm thu còn lại

- **ORC-02 - Cấu hình lịch và readiness cho hai nguồn:** chưa build/deploy
  image mới, chưa operator recovery hoặc chạy lại positive live workflow
  scheduled/first/rerun/contention. Cần đủ 7 daily tasks success,
  SourcesReady/verified=true/published=false và lease được release trước Done.
- Retry xử lý lỗi transient, không bảo đảm nguồn mạng luôn reachable. Hết
  budget vẫn fail closed; outer runner/DAG/QA timeout còn hiệu lực.
- Lần offline đầu dùng cache builder thiếu Iceberg/Scala; lần tải Maven Central
  bị chậm và container test riêng đã dừng. Chuyển sang bản sao cache host để
  chạy offline. Lần test đầu bắt assertion legacy `DOWNLOAD_FAILED` cho HEAD;
  sửa expectation sang `JMA_HEAD_IO_FAILED` và thêm test GET riêng. Không gọi
  các lần thử đó là passed/full Docker image build.
- Không trigger/clear/unpause/restart production DAG/service, xóa lease/data,
  sửa `.env`, commit/push/merge hoặc mở PR trong quá trình này. `.metals/`
  của người dùng được giữ nguyên và không thuộc bản vá.
