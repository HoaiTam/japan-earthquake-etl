---
task_id: "USG-06"
status: "Ready"
week: 2
block: "B - USGS Bronze"
workstream: "Live Bronze integration"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["USG-05"]
---

# USG-06 - Kết nối live runner và ghi USGS thật vào Bronze

## Mục đích

Khép khoảng trống giữa Airflow DAG đã kiểm thử bằng fixture với luồng chạy thật:
Airflow gọi Java HTTP client/Bronze writer, ghi raw GeoJSON cùng manifest vào
MinIO và trả metadata nhỏ để downstream resolve được input.

## Phạm vi công việc

- Tạo executable runner tuân thủ `USGS_INGEST_RUNNER_COMMAND --phase --context-file`.
- Nối `UsgsRequestBuilder`, `UsgsHttpClient` và `UsgsBronzeWriter` theo cùng run context.
- Implement `BronzeObjectStore` cho MinIO bằng pipeline credential có quyền giới hạn.
- Chạy một fixed integration window nhỏ và kiểm tra raw object, manifest, SHA-256, count và readback.
- Giữ unit test không phụ thuộc mạng; live smoke chỉ chạy khi operator chủ động cung cấp `.env` hợp lệ.

Fixed smoke window mặc định:

```text
[2023-01-01T00:00:00Z, 2023-01-04T00:00:00Z)
```

Window này phục vụ integration evidence, không thay daily three-day revision
window tại runtime.

## Thành phần cần có

- **Đầu vào và contract:** [USG-05](./USG-05.md), USGS Airflow/Bronze contracts và cấu hình MinIO hiện hành.
- **Phần triển khai:** Runner CLI, MinIO object-store adapter, phase summary protocol và live smoke có phạm vi.
- **Kết quả bàn giao:** Một run USGS thật đạt `BronzeReady` cùng command/evidence có thể lặp lại mà không commit credential hoặc raw payload.
- **Kiểm thử và evidence:** Unit test runner/adapter bằng fake service; live smoke ghi `run_id`, window, manifest URI, checksum và record count.

## Deliverable

- Live runner và MinIO `BronzeObjectStore` implementation.
- Static/unit tests cho phase protocol, retry/rerun và error mapping.
- Evidence một fixed USGS window đã ghi/đọc lại thành công từ MinIO.
- Runbook cấu hình `USGS_INGEST_RUNNER_COMMAND` và trigger có phạm vi.

## Tiêu chí hoàn thành

- [ ] DAG chạy real mode khi `USGS_INGEST_DRY_RUN=false`, không còn fail vì thiếu runner command.
- [ ] Fixed window tạo raw GeoJSON và manifest `BronzeReady` trên MinIO; URI/checksum/count đối soát được.
- [ ] Retry cùng logical run không ghi đè khác nội dung hoặc tạo duplicate logic.
- [ ] Lỗi HTTP/payload/checksum không vượt qua `bronze_ready_gate`.
- [ ] Command, log, manifest và test fixture không chứa credential hoặc toàn bộ payload nguồn.

## Hard dependency

- [USG-05](./USG-05.md)

## Cách triển khai và phối hợp

Task có thể bắt đầu ngay vì hard dependency đã `Done`. Dùng fake object store và
mock HTTP cho phần lớn test; chỉ chạy đúng một live window sau khi runner và
MinIO adapter đã đạt unit test.

1. Tạo branch mới từ baseline mới nhất: `feat/usg-06-live-bronze-runner`.
2. Khóa input/output JSON của từng phase trước khi nối network/storage thật.
3. Giữ raw bytes ngoài XCom/stdout; runner chỉ trả whitelist metadata.
4. Chạy fixed window một lần, sau đó rerun cùng logical identity để kiểm tra idempotency.
5. Ghi evidence vào task và bàn giao manifest cho `DAT-01`.

## Ranh giới

- Không mở rộng sang parser Silver, daily schedule hoặc JMA ingestion.
- Không dùng `curl`/script thủ công để ghi thẳng object rồi giả lập manifest.
- Không public MinIO API, hard-code credential hoặc in authorization header.
- Không tải toàn bộ lịch sử USGS trong live smoke.

## Theo dõi

- **Trạng thái:** Ready
- **Assignee:** Chưa nhận
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Java CLI, Airflow runner protocol, MinIO SDK, integration testing

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository hoặc môi trường demo.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Live window và rerun có manifest/checksum/count evidence.
- [ ] Runbook/config contract đã cập nhật.
- [ ] Không chứa secret, raw data dump hoặc build artifact không cần thiết.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [USGS Airflow ingest contract](../../specs/USGS_AIRFLOW_INGEST_CONTRACT.md)
- [USGS Bronze writer contract](../../specs/USGS_BRONZE_WRITER_CONTRACT.md)
- [Bronze storage contract](../../specs/BRONZE_STORAGE_CONTRACT.md)
- [Configuration and secrets](../../specs/CONFIGURATION_AND_SECRETS.md)
- [Kế hoạch tuần 3 song song](../WEEK_3_PARALLEL_PLAN.md)

