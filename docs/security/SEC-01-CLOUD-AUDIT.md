# SEC-01 — audit Drive/Colab pilot chỉ đọc

Phần này bổ sung cloud acceptance của [SEC-01](../task/tasks/SEC-01.md), không
thay [nghiệm thu local 23/10](../evidence/SEC-01_LOCAL_ACCEPTANCE_20261023.md).
Không cần Docker hoặc chờ COL-01 merge. Code/artifacts PR chưa merge được pin
bằng full commit và SHA; audit không checkout/merge/thực thi code của PR đó.

## Kiểm gì và để làm gì

- `scripts/security_cloud.py` dùng shared bounded reader của security handoff:
  quét source/output/metadata notebook, ZIP lồng nhau, JSON đã decode, giá trị
  và footer Parquet, text metadata PNG (không OCR ảnh). Không chạy notebook.
- Chặn checksum sai, path traversal, symlink, duplicate/encrypted ZIP entries,
  JSON duplicate keys, unsupported formats và resource budget. Cùng giới hạn
  32 MiB/file, 128 MiB tổng, 256 items; ZIP tối đa ba tầng kể cả archive ngoài.
- Quét OAuth fields kể cả key JSON escape, Bearer tokens, secret patterns,
  personal account paths và exact configured credentials trong linked deployment.
  Không phải scanner nhận diện được mọi secret hoặc dữ liệu nhạy cảm.
- `scripts/audit-security-cloud.py` đọc explicit local paths/Git blobs đã pin;
  optional Drive GET metadata, permissions **đủ pagination**, actual notebook
  media. Không upload, đổi ACL, mount Drive, chạy ETL hoặc ghi MinIO probes.
- Không in filenames/paths/token/email/raw API exception trong report; leaf
  locations được hash. Report staging có principal hashes để đối chiếu riêng,
  không coi hashes là vô danh tuyệt đối; không commit private scope/roster.

API theo tài liệu chính thức: [files.get](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/get),
[permissions.list](https://developers.google.com/workspace/drive/api/reference/rest/v3/permissions/list).
Snapshot ACL của exact objects không chứng nhận mọi descendant, tương lai,
toàn tài khoản hoặc tài nguyên ngoài project. Personal notebook owner-only có
thể là chủ đích hợp lệ; không tự share cho cả nhóm để làm xanh audit.

## Setup và chạy lại

Dùng Python/model venv đã chuẩn bị theo [EXP-01](../runbooks/EXP-01_COLAB_BOOTSTRAP.md),
có PyArrow/Pillow. Thiếu decoder trả coverage pending, không skip để PASS.
Unit tests offline: `make test-security-audit` với model venv đã activate.

Tạo scope JSON **ignored/private** tại `staging/`, không dùng report PASSED
cũ thay artifact thật. Schema tối thiểu:

```json
{
  "version": "sec-01-cloud-scope-v1",
  "artifacts": [
    {"path": "exact-artifact.zip", "sha256": "<trusted-full-sha256>"}
  ],
  "drive_objects": [
    {"id": "<exact-project-object-id>", "notebook": true}
  ]
}
```

Đây là schema mẫu, không phải evidence; thay bằng paths/pins/IDs thật trước
khi chạy. Có thể dùng `git_revision` (40 hex), `git_path`, `sha256` thay `path`;
commit phải có trong repo. Artifact mang embedded receipt dùng
`embedded_receipt: ["cloud_execution", "pilot_receipt"]`; receipt có provenance
containing artifact SHA và canonical-fields SHA, **không** giả là SHA bytes
của receipt gốc trong ZIP. Receipt tự khai không xác thực Google identity,
không tự chứng minh đã chạy trên local hay Colab VM.

Phiên 02/11 có scope thật ở `staging/sec01-cloud-20261102/scope.json` trên máy
owner; scope không được commit và không mặc định có trên máy khác. Dùng tên
report mới mỗi lần, không ghi đè hoặc sửa evidence cũ:

```bash
make audit-security-cloud \
  PILOT_PYTHON=staging/venvs/exp01-colab/bin/python \
  SECURITY_CLOUD_MANIFEST=staging/sec01-cloud-20261102/scope.json \
  SECURITY_REPORT=staging/sec01-cloud-20261102/operator-readonly-new.json \
  SECURITY_DEPLOYMENT_ROOT=../japan-earthquake-etl
```

Mặc định offline. Thêm `SECURITY_CLOUD_ONLINE=1` mới đọc API. Reader chỉ dùng
existing rclone remote `dat03_team` với exact folder nhóm đã được owner duyệt;
không đăng nhập lại, xin scope rộng hơn hoặc serialize token. Access token hết
hạn: có thể chạy scoped `rclone lsjson dat03_team: --max-depth 1` để rclone tự
refresh theo kết nối cũ, rồi chạy lại bằng report mới. API error/403/timeout là
pending, không là bằng chứng private ACL an toàn. HTTP redirects bị từ chối.

Google Drive mount có quyền rộng theo OAuth consent, không phải folder-scoped
read-only security boundary. Hai phiên owner được audit ngày02/11 dùng Files
upload/download, không mount Drive; không suy ra cách dùng của thành viên khác.

## Đọc report và chốt acceptance

- Exit **1**: có finding; cần review/fix đúng scope, không log raw secret.
- Exit **2**: chưa đủ human/identity/session/permission coverage hoặc input lỗi.
  `scoped_bytes_passed=true` chỉ là artifact scan; không đồng nghĩa SEC-01 Done.
- CLI luôn giữ `can_mark_done=false`: nó không thay người review quyền nhận và
  actual sessions ba người. Sau khi đủ human evidence, owner ghi quyết định
  nghiệm thu riêng; không sửa scanner flag/report cũ để pass.
- Xác nhận riêng roster và các tài khoản phụ được duyệt trên từng object.
  User-only ACL vẫn có thể chia sẻ nhầm người. Giữ nguyên ACL khi cần xác nhận.
- Thu actual notebook/output ZIP cùng trusted SHA từ Trang/Trí; đối chiếu bytes,
  decoded scan và provenance session. Evidence JSON trên PR là nguồn tham khảo,
  không thay việc tải artifact. Không tạo receipt hộ hoặc suy identity từ tên.
- [DAT-03](../task/tasks/DAT-03.md) còn gate nhận dataset trên ba máy; receipt
  Colab không thay receipt máy riêng. [EXP-02](../task/tasks/EXP-02.md) và
  [EXP-03](../task/tasks/EXP-03.md) vẫn có experiment/official gates riêng.

Kết quả hiện tại và phần còn thiếu: [evidence 02/11](../evidence/SEC-01_CLOUD_20261102.md).
