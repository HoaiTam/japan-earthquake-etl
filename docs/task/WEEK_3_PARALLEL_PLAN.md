# Kế hoạch tuần 3 — ba luồng không chờ nhau

Kế hoạch này áp dụng sau khi pre-week data readiness gate hoàn tất. Mục tiêu là
ba người làm độc lập bằng contract/fixture/sample đã khóa, rồi chỉ ghép output
trong phiên integration cuối tuần.

## 1. Pre-week gate do người điều phối hoàn thành

| Task | Effort | Output bắt buộc trước kickoff |
|---|---:|---|
| [`USG-06`](./tasks/USG-06.md) | 4h | Live runner, MinIO adapter và một USGS manifest `BronzeReady` |
| [`JMA-01`](./tasks/JMA-01.md) | 5h | Inventory/source-format contract và một archive đại diện đã xác định |
| [`DAT-01`](./tasks/DAT-01.md) | 3h | Catalog metadata cho một USGS window và một JMA staged archive |

Tổng pre-week: **12 giờ**. Chỉ bắt đầu lịch bên dưới khi ba task đạt `Done`.
Nếu gate chưa đạt, thành viên vẫn có thể làm unit/interface bằng fixture nhưng
không được nhận là đã integration-test với sample thật.

## 2. Hai loại input dùng trong tuần

| Loại | Dùng cho | Quy tắc |
|---|---|---|
| Synthetic fixture trong `tests/fixtures` | Unit test hằng ngày | Nhỏ, deterministic, không network; là input chính khi phát triển |
| Real sample catalog từ `DAT-01` | Integration cuối tuần | Chỉ metadata được commit; raw ở MinIO/staging; mọi người dùng cùng checksum/identity |

JMA sample ban đầu có trạng thái `STAGED_SOURCE`, không phải `BronzeReady`.
`JMA-03` chịu trách nhiệm validate/write và tạo Bronze manifest. Không ai phải
chờ downloader `JMA-02` vì archive đại diện đã được `DAT-01` stage trước tuần.

## 3. Phân công ba luồng

| Luồng | Task | Task effort | Review/integration buffer | Mục tiêu độc lập |
|---|---|---:|---:|---|
| Người 1 — Input & quality | `JMA-02` (6h), `SLV-01` (3h), `SLV-05` (5h) | 14h | 1h | Downloader/change detection, manifest resolver và validation/reject rules |
| Người 2 — JMA vertical slice | `JMA-03` (7h), `SLV-03` (6h) | 13h | 2h | JMA staged archive → Bronze writer và fixed-width observation parser |
| Người 3 — USGS & Silver publish | `SLV-02` (5h), `SLV-04` (4h), `SLV-08` (5h) | 14h | 1h | USGS parser, common source lineage và deterministic Silver writer |

Tổng implementation task: **41 giờ**. Bốn giờ còn lại trong capacity tuần được
dành cho cross-review và phiên integration, không tạo thêm task scope ngầm.

## 4. Vì sao các luồng không phải chờ nhau

### Người 1

- `JMA-02` dùng inventory `JMA-01` đã khóa trước tuần.
- `SLV-01` và `SLV-05` chỉ phụ thuộc contract đã `Done`, phát triển bằng fixture.
- Không cần chờ `JMA-03` hoặc parser để hoàn thành unit test của resolver/quality rules.

### Người 2

- `JMA-03` đọc staged archive từ `DAT-01`, không chờ downloader `JMA-02`.
- `SLV-03` đọc JMA ZIP/fixed-width fixture của `CON-04`.
- Integration với downloader chỉ diễn ra cuối tuần; không chặn writer/parser PR.

### Người 3

- `SLV-02` và `SLV-04` dùng USGS fixture/contract đã có.
- `SLV-08` phát triển bằng observation fixture theo `CON-03`, không chờ parser merge.
- Real USGS manifest từ `DAT-01` chỉ dùng cho integration cuối tuần.

## 5. Handoff contract giữa các luồng

| Producer | Consumer | Chỉ handoff | Không handoff |
|---|---|---|---|
| `JMA-02` | `JMA-03` | Resolved archive metadata/bytes, ETag/Last-Modified/size/SHA-256 | HTML scrape result không version, local absolute path |
| `JMA-03` | `SLV-01`/`SLV-03` | `BronzeReady` manifest, raw ZIP URI và catalog release | Extracted row bị normalize sẵn |
| `SLV-01` | Parser tasks | Exact resolved manifest/object, run/release context | Wildcard prefix hoặc “latest” mơ hồ |
| `SLV-02`/`SLV-03` | `SLV-04`/`SLV-05` | Observation rows theo `CON-03`, raw lineage và parse counts | Source-specific aliases ngoài contract |
| `SLV-04`/`SLV-05` | `SLV-08` | Valid/rejected rows, reason/count summary | Im lặng drop invalid/duplicate rows |

## 6. Cách làm việc trong tuần

1. Mỗi task có branch/PR riêng; “luồng” là ownership, không phải một branch lớn.
2. Người nhận task đánh dấu `In Progress` trong file task và task index.
3. Không branch từ PR của người khác nếu task chỉ cần fixture/contract đã merge.
4. Mọi task phải đạt unit/static acceptance độc lập trước phiên integration.
5. Nếu interface cần đổi, sửa contract/fixture qua PR riêng trước khi consumer đổi theo.

## 7. Phiên integration cuối tuần

Chạy hai đường nhỏ, không chạy full 40 năm:

```text
USGS DAT-01 manifest
  -> SLV-01 resolve
  -> SLV-02 parse
  -> SLV-04 lineage
  -> SLV-05 quality
  -> SLV-08 Silver staging/publish check

JMA DAT-01 staged archive
  -> JMA-03 BronzeReady
  -> SLV-01 resolve
  -> SLV-03 parse
  -> SLV-04 lineage
  -> SLV-05 quality
  -> SLV-08 Silver staging/publish check
```

`JMA-04`, `JMA-05` và `SLV-09` chưa thuộc tuần này. Chúng là orchestration/E2E
gate sau khi các component riêng đã ổn định; không được đánh dấu `Done` trong
phiên ghép thủ công.

## 8. Checklist để người điều phối kiểm tra cuối tuần

- [ ] 8 task trong ba luồng có evidence riêng; task chưa đạt giữ đúng status.
- [ ] USGS và JMA đều truy vết được source/sample → manifest → observation.
- [ ] Input/parsed/valid/rejected/output counts đối soát được theo source.
- [ ] Rerun fixture không đổi logical output hoặc append duplicate.
- [ ] JMA downloader và Bronze writer đồng ý cùng checksum/catalog release.
- [ ] Silver writer không coi staging/output dở là Published.
- [ ] Không commit raw sample lớn, `.env`, credential, signed URL hoặc build artifact.
- [ ] Các PR có reviewer chéo giữa ba luồng khi có thể.

## 9. Task sau tuần 3

Ưu tiên tiếp theo là `JMA-04`, `JMA-05`, `SLV-06`, `SLV-07` và `SLV-09`.
Chỉ chạy full JMA backfill sau khi downloader/writer/parser và sample integration
đã đạt; không dùng full dataset để thay thế unit test.
