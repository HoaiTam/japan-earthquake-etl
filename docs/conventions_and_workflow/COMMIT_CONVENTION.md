# Git commit convention

Project sử dụng Conventional Commits ở mức đơn giản để lịch sử thay đổi dễ đọc và dễ tìm theo thành phần pipeline.

## 1. Cú pháp

```text
<type>(<scope>): <description>
```

`scope` có thể bỏ qua khi thay đổi ảnh hưởng toàn project:

```text
docs: add local operations runbook
```

## 2. Type

| Type | Dùng khi |
|---|---|
| `feat` | Thêm chức năng có hành vi mới |
| `fix` | Sửa lỗi |
| `docs` | Chỉ thay đổi tài liệu |
| `refactor` | Đổi cấu trúc mà không đổi hành vi mong đợi |
| `test` | Thêm/sửa test hoặc fixture kiểm thử |
| `perf` | Cải thiện hiệu năng |
| `build` | Thay đổi Maven, Docker image hoặc dependency build |
| `ci` | Thay đổi CI/CD |
| `chore` | Bảo trì không thuộc nhóm trên |
| `revert` | Hoàn tác một commit |

## 3. Scope đề xuất

| Scope | Phạm vi |
|---|---|
| `extract` | USGS/boundary ingestion |
| `airflow` | DAG, schedule, orchestration |
| `silver` | Spark Silver transform |
| `gold` | Spark Gold/Iceberg publish |
| `quality` | Validation, reconciliation, metrics |
| `minio` | Object storage/bucket config |
| `iceberg` | Table format/catalog config |
| `trino` | Query engine/catalog/SQL |
| `powerbi` | Semantic model/dashboard assets |
| `compose` | Docker Compose/local environment |
| `docs` | Tài liệu khi cần scope rõ |

Không tạo scope theo tên thành viên.

## 4. Quy tắc description

- Viết tiếng Anh ngắn gọn để thống nhất với tên kỹ thuật và Git history.
- Dùng động từ ở dạng mệnh lệnh: `add`, `fix`, `validate`, `remove`, `document`.
- Viết chữ thường ở đầu, không thêm dấu chấm cuối.
- Mô tả thay đổi thực tế, không dùng `update`, `fix bug`, `done`, `final`.
- Một commit nên đại diện một thay đổi logic có thể review/revert độc lập.

Ví dụ tốt:

```text
feat(extract): store USGS response metadata in bronze
fix(silver): keep latest event by updated timestamp
test(quality): cover duplicate events across partitions
build(compose): add Trino health check
docs: document backfill recovery flow
```

Ví dụ cần tránh:

```text
update
fix: fix bug
chore: final version
feat: add pipeline and dashboard and rewrite docs
```

## 5. Body và footer

Dùng body khi tiêu đề chưa giải thích được lý do hoặc ảnh hưởng:

```text
fix(gold): prevent stale partitions after backfill

Replace only partitions touched by the UTC data interval so a rerun does
not append duplicate events or overwrite unrelated dates.
```

Breaking change phải được nêu rõ:

```text
feat(silver)!: rename event timestamp field

BREAKING CHANGE: event_time is replaced by event_time_utc in Silver output.
```

Nếu có issue:

```text
Refs: #<issue-number>
Closes: #<issue-number>
```

## 6. Commit không nên chứa

- Secret, `.env`, access key, password hoặc DSN riêng tư.
- Data dump lớn hoặc file phát sinh không cần version control.
- Code và format hàng loạt không liên quan trong cùng commit.
- Artifact build như `target/` nếu không có quyết định lưu artifact.
- Message chứa thông tin nhạy cảm trong command line/history.

## 7. Checklist trước commit

```text
[ ] git diff chỉ chứa thay đổi mong muốn
[ ] Không có secret/data nhạy cảm
[ ] Test/lint liên quan đã chạy
[ ] Docs được cập nhật nếu contract/flow/config thay đổi
[ ] Message đúng type và scope
```

