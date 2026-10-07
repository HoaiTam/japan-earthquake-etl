# AGENTS.md

Hướng dẫn này áp dụng cho toàn bộ repository `japan-earthquake-etl`.

## 1. Nguồn yêu cầu

Trước khi bắt đầu một task, đọc theo thứ tự:

1. File `docs/task/tasks/<TASK-ID>.md` tương ứng.
2. Chỉ mục `docs/task/tasks/README.md` để xem trạng thái và các task cùng khối.
3. `docs/task/README.md` và `docs/task/WORK_BLOCKS.md`.
4. Tài liệu kỹ thuật được task dẫn chiếu trong `docs/`.
5. `docs/conventions_and_workflow/GIT_WORKFLOW.md` và `COMMIT_CONVENTION.md`.

Nếu các tài liệu mâu thuẫn, ưu tiên acceptance criteria trong file task, sau đó là đặc tả dự án và tài liệu flow/quality liên quan. Hành vi đã được kiểm thử là bằng chứng để cập nhật lại tài liệu, không phải lý do giữ hai mô tả khác nhau.

## 2. Quy trình bắt buộc cho mỗi task

- Kiểm tra mục `Hard dependency` trong file task trước khi sửa file. Không triển khai task khi dependency chưa hoàn tất, trừ phần fixture, mock, interface hoặc test plan mà file task cho phép.
- Tạo branch mới từ `main` trước thay đổi đầu tiên. Dùng dạng `<type>/<task-id-lowercase>-<short-description>`, ví dụ `docs/pln-01-scope-kpi-dod`.
- Chỉ thay đổi phạm vi cần thiết cho deliverable và acceptance criteria của task.
- Cập nhật metadata và mục `Theo dõi` trong file task sang `In Progress` khi bắt đầu; đồng bộ trạng thái ở `docs/task/tasks/README.md`. `Needs Update` dùng cho task từng hoàn tất nhưng baseline/contract mới làm output cũ chưa còn đủ. Khi deliverable, acceptance criteria, test/check, evidence và tài liệu liên quan đã hoàn tất, tự động chuyển task sang `Done` trước khi bàn giao. `Review` chỉ dùng khi công việc thực sự còn chờ kiểm tra để hoàn tất.
- Reviewer độc lập vẫn được khuyến nghị cho P0/P1 nhưng không chặn trạng thái `Done`. Không tự điền tên, approval hoặc evidence chưa tồn tại; nếu chưa có reviewer thì giữ `unassigned` và ghi rõ trong bàn giao.
- Trước khi bàn giao, chạy kiểm tra phù hợp, xem `git diff`, kiểm tra secret và cập nhật docs nếu contract, flow, schema logic hoặc cấu hình thay đổi.
- Không tự commit, push, merge hoặc mở PR nếu người dùng chưa yêu cầu. Khi bàn giao, cung cấp lệnh Git dùng đường dẫn cụ thể; không mặc định dùng `git add .`.

## 3. Baseline sản phẩm

Baseline phạm vi, KPI và Definition of Done nằm tại `docs/specs/MVP_SCOPE_KPI_AND_DOD.md`.

Các nguyên tắc không được phá vỡ:

- Pipeline batch hằng ngày theo cửa sổ dữ liệu UTC; timezone điều phối là `Asia/Ho_Chi_Minh`.
- Bronze lưu response nguồn nguyên bản và metadata theo run, không bị sửa sau khi ghi thành công.
- Silver dùng Spark Java để parse và chuẩn hóa USGS/JMA về observation contract chung, giữ lineage, validate, deduplicate/revision trong từng nguồn rồi liên kết observation giữa hai nguồn.
- Gold chọn canonical event để tránh double count và lưu bằng bảng Iceberg trên MinIO. Chỉ snapshot đã commit và verify qua Trino mới được coi là `Published`.
- Core ML pin Gold snapshot, tạo candidate/feature bằng Spark, chạy Window/DBSCAN/HDBSCAN theo từng mainshock qua bundle Colab có checksum, rồi validate/import vào Iceberg `ml.*`; không ghi nhãn thử nghiệm vào Gold core.
- Static report/Trino verification là output Core. Power BI đọc qua Trino/ODBC ở chế độ Import nhưng là Stretch, không chặn MVP HDBSCAN và không đọc từng object Parquet.
- Kết quả là sequence/aftershock candidate hồi cứu; không tuyên bố dự đoán động đất hoặc quan hệ nhân quả vật lý.
- Retry/rerun/backfill không được tạo duplicate logic hoặc sửa partition ngoài phạm vi.
- Không làm mất event hợp lệ chỉ vì thiếu spatial enrichment; giữ `Unknown`/`Offshore` theo contract được chốt.

## 4. Chất lượng và kiểm thử

- Mọi thay đổi phải có test tự động phù hợp hoặc evidence thủ công có thể lặp lại.
- Fixture dữ liệu phải nhỏ, xác định được và không phụ thuộc mạng cho unit test.
- Kiểm tra các trường hợp tối thiểu liên quan: response rỗng hợp lệ, null/sai kiểu, duplicate, late update, retry và rerun.
- Không công bố Gold khi blocker quality gate thất bại.
- Đối soát count phải giải thích được input, parsed, valid, rejected, duplicate/superseded và output.
- Nếu thực hiện Power BI Stretch, KPI phải khớp truy vấn Trino độc lập với cùng filter context.

Chạy các lệnh build/test được repository cung cấp. Không phát minh lệnh placeholder trong runbook. Nếu module chưa tồn tại, ghi rõ kiểm tra nào chưa thể chạy.

## 5. Bảo mật và an toàn dữ liệu

- Không commit `.env`, credential, token, private key, DSN riêng tư, payload nhạy cảm hoặc data dump lớn.
- Không ghi secret vào log, command line, tài liệu hoặc fixture.
- Không xóa bucket, warehouse, volume hoặc Iceberg metadata để xử lý lỗi.
- Không dùng `docker compose down -v`, wildcard xóa rộng, `git reset --hard` hoặc force push thông thường.
- Chỉ expose port cần cho vận hành local và Power BI; giữ MinIO API, Catalog, Spark và metadata database trong Compose network nếu không có nhu cầu đã được duyệt.

## 6. Quy ước triển khai

- Xử lý dữ liệu ETL và tạo ML dataset dùng Java/Spark SQL/DataFrame API. Python chỉ dùng cho notebook/module experiment Window/DBSCAN/HDBSCAN và report khoa học theo contract đã version hóa.
- Cấu hình thay đổi theo môi trường phải nằm ngoài mã nguồn và có validation rõ ràng.
- Mọi job dùng run context thống nhất: `run_id`, `[window_start_utc, window_end_utc)`, `processing_date`, input đã resolve, output logic, `is_backfill` và phiên bản cấu hình nếu có.
- Input/output của run hoặc backfill phải được resolve rõ; không dùng wildcard rộng.
- Log dùng key-value/JSON, có run context và count, không log toàn payload mặc định.
- Tài liệu dùng đường dẫn tương đối trong repository và Mermaid cho sơ đồ có thể review cùng mã nguồn.

## 7. Git và bàn giao

Commit dùng Conventional Commits:

```text
<type>(<scope>): <description>
```

Description viết tiếng Anh, dạng mệnh lệnh, chữ thường ở đầu và không có dấu chấm cuối. Ví dụ cho task này:

```text
docs: define MVP scope KPIs and definition of done
```

Khi hoàn thành, báo cáo tối thiểu:

- Branch hiện tại.
- File đã thay đổi.
- Test/check đã chạy và kết quả.
- Giới hạn hoặc bước review còn lại.
- Trong phần **Giới hạn / Rủi ro / Bước tiếp theo** của bàn giao, mô tả PR và docs, nếu phần còn thiếu thuộc task chưa thực hiện hoặc chưa hoàn tất, phải ghi **mã task, tên task và phần việc còn thiếu**; kèm link file task khi phù hợp. Ví dụ: chưa kiểm thử JMA → MinIO live — [JMA-05 - Kiểm thử JMA đến Bronze](docs/task/tasks/JMA-05.md). Không chỉ ghi chung chung “làm sau” hoặc “chưa kiểm thử”; nếu chưa có task phụ trách thì ghi rõ “chưa có task”, không tự đoán mã task.
- Các lệnh `git diff`, `git add <paths>`, `git commit` và `git push -u origin <branch>` phù hợp.
- Tự động gửi mô tả PR bằng tiếng Việt để người dùng copy, không chờ yêu cầu riêng. Đặt toàn bộ mô tả trong một khối code `markdown`, không lồng code fence bên trong; gồm mục tiêu/task, thay đổi chính, docs, test/check thực tế và giới hạn hoặc review còn lại. Không tự đăng mô tả lên GitHub khi chưa được yêu cầu.
