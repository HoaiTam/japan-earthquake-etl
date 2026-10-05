---
task_id: "DAT-01"
status: "Done"
week: 3
block: "A - Hợp đồng dữ liệu"
workstream: "Shared real samples"
scope: "Core"
priority: "P0"
effort_hours: 3
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["USG-06", "JMA-01"]
---

# DAT-01 - Chuẩn bị bộ dữ liệu mẫu thật từ USGS và JMA

## Mục đích

Cung cấp một bộ input thật nhỏ, cố định và có checksum để ba luồng JMA Bronze,
USGS/JMA parser và Silver storage integration-test vào cuối tuần mà không phụ
thuộc network hoặc phải tải toàn bộ 40 năm trong lúc phát triển.

## Phạm vi công việc

- Đăng ký fixed USGS sample từ run `USG-06` với window, run ID, manifest URI, checksum và count.
- Chọn đúng một JMA archive đại diện từ inventory `JMA-01`; mặc định ưu tiên năm 2023, nếu đổi phải ghi lý do/catalog release.
- Tải/stage JMA archive nguyên bản, ghi source URL, size, SHA-256, file/record count sơ bộ và format metadata.
- Tạo catalog metadata máy đọc được và hướng dẫn consumer resolve sample; không commit raw payload/archive lớn.
- Phân biệt fixture synthetic, raw sample staged và Bronze `BronzeReady` để downstream không dùng nhầm quality gate.

## Thành phần cần có

- **Đầu vào và contract:** [USG-06](./USG-06.md), [JMA-01](./JMA-01.md), Bronze contract và fixture contract.
- **Phần triển khai:** Chốt sample selection, tạo metadata catalog/checksum và verify commands không phụ thuộc đường dẫn cá nhân.
- **Kết quả bàn giao:** Shared real-sample catalog cho một USGS window và một JMA archive, cùng evidence tải/đọc/checksum.
- **Kiểm thử và evidence:** Catalog schema check, SHA-256 readback, URL/release/window validation và secret/raw-data scan.

## Deliverable

- `docs/specs/SHARED_REAL_SAMPLE_DATA.md` mô tả cách lấy, verify và dùng sample.
- `tests/fixtures/real-samples/catalog.json` chỉ chứa metadata, URI logic, checksum và expected counts; không chứa raw data.
- `scripts/check-real-sample-catalog.sh` kiểm tra catalog offline và
  `scripts/verify-real-samples.sh` đọc lại hai object từ MinIO.
- USGS entry ở trạng thái `BRONZE_READY` và trỏ tới manifest do `USG-06` tạo.
- JMA entry ở trạng thái `STAGED_SOURCE`; chỉ `JMA-03` mới được chuyển thành Bronze manifest `BronzeReady`.

## Tiêu chí hoàn thành

- [x] Catalog có đúng một fixed USGS window và một JMA archive/release đại diện.
- [x] Cả hai entry có source identity, time/year, size, SHA-256 và expected count có thể đối soát.
- [x] USGS manifest đọc lại được; JMA staged archive mở được và record structure phù hợp inventory.
- [x] Consumer có thể dùng sample/fixture mà không cần chờ downloader hoặc gọi network trong unit test.
- [x] Git không chứa raw GeoJSON/ZIP lớn, credential, signed URL hoặc absolute path cá nhân.

## Hard dependency

- [USG-06](./USG-06.md)
- [JMA-01](./JMA-01.md)

## Cách triển khai và phối hợp

Thực hiện sau khi `USG-06` và `JMA-01` đạt `Done`. Đây là pre-week gate cho
[kế hoạch tuần 3](../WEEK_3_PARALLEL_PLAN.md); khi catalog đã khóa, mọi người
dùng cùng sample identity nhưng vẫn phát triển hằng ngày bằng fixture local.

1. Tạo branch mới: `test/dat-01-shared-real-samples`.
2. Chọn sample nhỏ, ghi metadata/checksum trước khi chia sẻ.
3. Lưu raw object trong MinIO/staging được cấu hình; chỉ commit catalog metadata.
4. Chạy checksum/readback và ghi evidence command không lộ credential.
5. Bàn giao catalog; không tự tuyên bố JMA `BronzeReady` trước `JMA-03`.

## Ranh giới

- Không tải full JMA 40 năm hoặc lịch sử USGS.
- Không thay fixture synthetic bằng network call trong unit test.
- Không normalize/filter business rows hoặc tạo Silver output trong task này.
- Không dùng raw sample làm ground truth khoa học hoặc benchmark hiệu năng.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** Chưa nhận
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** `./scripts/check-real-sample-catalog.sh` và
  `./scripts/verify-real-samples.sh` đạt; live readback ngày `2026-10-04` xác
  nhận USGS `11,771` byte/`16` event từ manifest
  `BronzeReady`, và JMA `h2023.zip` `6,977,812` byte/`257,020` record 96 byte
  từ object `STAGED_SOURCE`. Cả hai checksum khớp catalog; raw không được commit.
- **Kỹ năng phù hợp:** Data sampling, checksums, source metadata, test data management

## Checklist bàn giao

- [x] Deliverable và catalog metadata tồn tại trong repository.
- [x] Acceptance criteria và checksum/readback đã được kiểm tra.
- [x] Trạng thái `BRONZE_READY`/`STAGED_SOURCE` được dùng đúng.
- [x] Consumer docs và kế hoạch tuần 3 đã liên kết catalog.
- [x] Không chứa secret, signed URL, raw dump hoặc build artifact.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Source coverage contract](../../specs/SOURCE_COVERAGE.md)
- [Bronze storage contract](../../specs/BRONZE_STORAGE_CONTRACT.md)
- [Shared real-sample catalog](../../specs/SHARED_REAL_SAMPLE_DATA.md)
- [Shared synthetic fixtures](../../../tests/fixtures/README.md)
- [Kế hoạch tuần 3 song song](../WEEK_3_PARALLEL_PLAN.md)
