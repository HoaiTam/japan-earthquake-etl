# Git workflow

Workflow này áp dụng cho repository hiện tại dùng branch mặc định `main`. Nếu nhóm chọn fork workflow, thay `upstream` bằng repository chung và `origin` bằng fork cá nhân; không trộn hai mô hình trong cùng hướng dẫn.

## 1. Nguyên tắc

- `main` luôn ở trạng thái có thể review/demo.
- Không commit trực tiếp vào `main` sau khi repository bật branch protection.
- Mỗi branch giải quyết một mục tiêu nhỏ, không phải toàn bộ project.
- Rebase/cập nhật branch trước khi merge để giảm conflict.
- Mọi thay đổi contract, flow hoặc cấu hình phải cập nhật docs liên quan.

## 2. Đặt tên branch

```text
<type>/<short-description>
```

Ví dụ:

```text
feat/usgs-extractor
feat/silver-deduplication
fix/trino-iceberg-catalog
docs/pipeline-flows
chore/compose-healthchecks
```

Type branch thường dùng: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`.

## 3. Bắt đầu công việc

Đồng bộ `main`:

```bash
git switch main
git pull --ff-only origin main
```

Tạo branch:

```bash
git switch -c feat/<short-description>
```

Không dùng branch tên `new_branch`, `tam`, `test1` hoặc tên thành viên.

## 4. Trong quá trình làm

Kiểm tra thay đổi thường xuyên:

```bash
git status
git diff
```

Stage có chọn lọc và commit theo [commit convention](./COMMIT_CONVENTION.md):

```bash
git add <paths>
git commit -m "feat(<scope>): <description>"
```

Không dùng `git add .` theo thói quen khi working tree có file dữ liệu, `.env` hoặc thay đổi không liên quan.

## 5. Đồng bộ trước khi mở PR

```bash
git fetch origin
git rebase origin/main
```

Nếu có conflict:

1. Mở từng file conflict và giữ nội dung đúng.
2. Chạy `git diff --check`.
3. Stage file đã giải quyết.
4. Tiếp tục rebase.

```bash
git add <resolved-paths>
git rebase --continue
```

Nếu cần hủy rebase:

```bash
git rebase --abort
```

Sau khi rebase branch đã từng push, dùng:

```bash
git push --force-with-lease origin <branch-name>
```

Không dùng `--force` vì có thể ghi đè commit người khác.

## 6. Push và tạo pull request

```bash
git push -u origin <branch-name>
```

PR cần có:

- Mục tiêu và bối cảnh.
- Thay đổi chính.
- Cách kiểm thử và kết quả.
- Ảnh/log/snapshot/query evidence nếu liên quan pipeline/dashboard.
- Rủi ro, giới hạn và rollback/recovery note.
- Danh sách docs đã cập nhật.

Template mô tả ngắn:

```markdown
## Mục tiêu

## Thay đổi

## Cách kiểm thử

## Bằng chứng

## Rủi ro / giới hạn

## Checklist
- [ ] Test liên quan đạt
- [ ] Không commit secret hoặc data nhạy cảm
- [ ] Docs/contract được cập nhật
- [ ] Rerun/idempotency đã xem xét
```

## 7. Quy tắc review theo loại thay đổi

### Pipeline/Airflow

- Data interval và timezone đúng.
- Retry không tạo duplicate.
- Dependency dừng downstream khi quality gate fail.
- Log có run context, không có secret.

### Spark/Silver/Gold

- Có fixture cho null, invalid, duplicate, late update.
- Transformation có tính xác định.
- Phạm vi partition write rõ ràng.
- Có đối soát input/output.

### Iceberg/Trino

- Spark và Trino dùng cùng catalog/warehouse.
- Migration/schema change tương thích consumer hoặc được đánh dấu breaking.
- Có query verification sau commit.

### Docker/cấu hình

- Không hard-code credential.
- Chỉ expose port cần thiết.
- Có health check/dependency hợp lý.
- Volume bền vững không bị thay đổi/xóa ngoài ý muốn.

### Power BI/docs

- KPI khớp định nghĩa và SQL kiểm chứng.
- Source/freshness/đơn vị rõ ràng.
- Link nội bộ docs còn hợp lệ.

## 8. Merge

Ưu tiên squash merge cho branch nhỏ để `main` có lịch sử gọn. Tiêu đề squash phải theo commit convention.

Không merge khi:

- Test/check bắt buộc thất bại.
- Có unresolved comment quan trọng.
- PR làm thay đổi contract nhưng docs chưa cập nhật.
- Có secret hoặc artifact dữ liệu không phù hợp.
- Chưa chứng minh idempotency cho thay đổi cách ghi dữ liệu.

Sau merge:

```bash
git switch main
git pull --ff-only origin main
git branch -d <branch-name>
```

Xóa remote branch qua giao diện PR hoặc:

```bash
git push origin --delete <branch-name>
```

## 9. Hotfix

Với lỗi chặn pipeline/demo:

1. Tạo `fix/<short-description>` từ `main` mới nhất.
2. Chỉ sửa phạm vi tối thiểu.
3. Thêm regression test hoặc bằng chứng xác nhận.
4. Review nhanh nhưng không bỏ qua secret/data-safety checks.
5. Ghi rõ có cần rerun/backfill hay không.

## 10. Thay đổi ảnh hưởng dữ liệu

PR thay đổi schema, dedup, partition, Gold write hoặc KPI phải nêu:

- Phạm vi dữ liệu bị ảnh hưởng.
- Tương thích với dữ liệu hiện có.
- Có cần rebuild/backfill không.
- Cách verify trước/sau.
- Kế hoạch phục hồi nếu output sai.

Không kèm script xóa dữ liệu rộng như một bước “setup” mặc định.

## 11. Bảo vệ repository

Khi repository bắt đầu có code, nên cấu hình:

- Bảo vệ branch `main`.
- Yêu cầu ít nhất một review trước merge nếu số thành viên cho phép.
- Yêu cầu status checks cho build/test/docs link check.
- Chặn force push và xóa `main`.
- Secret scanning/dependency alerts nếu nền tảng hỗ trợ.

