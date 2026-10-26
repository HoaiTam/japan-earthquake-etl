# DAT-03 — Kết nối Google Drive nhóm

## Folder và phạm vi

- [Folder nhóm do owner cung cấp](https://drive.google.com/drive/u/1/folders/17Wntw5LIRvFdzaU0RpnBRL60gzsRdFTB).
- Folder ID: `17Wntw5LIRvFdzaU0RpnBRL60gzsRdFTB`.
- Remote local riêng: `dat03_team:`. Không ghi token/rclone config vào repo.
- Setup kết nối không là quyền upload dataset, xóa local, đổi ACL hoặc chạy
  distributed ETL. Giữ các gate [team worker setup](./DAT-03_TEAM_WORKER_SETUP.md).
- `/u/1` chỉ là vị trí account trong browser, không pin danh tính Google.

## Tình trạng setup trên máy HoaiTam — 26/10/2026

- Rclone v1.75.2 đã cài sẵn; không cài lại.
- **Mới nhất — SETUP_READ_WRITE_LAYOUT_VERIFIED:** owner cung
  cấp Desktop OAuth JSON thuộc project `ie212-dat03-drive`. Đã kiểm đúng
  client/project/token endpoint, đổi riêng client của `dat03_team`, giữ
  scope/root và toàn bộ remote khác nguyên trạng. Token shared-client cũ
  của remote đích đã được gỡ trước re-auth, không tái dùng cho client mới.
  JSON và config đều mode0600; không vào repo, không log secret hoặc truyền
  secret trong argv. Private Unix control socket đã đóng sau cập nhật, không
  có control listener TCP để lại. Owner đã hoàn tất browser re-auth bằng
  client riêng; stat/list/about exit0, không còn shared-client warning/403.
- Metadata API xác nhận exact folder ID, tên **IE212**, không phải Shared
  Drive; `canAddChildren`, `canListChildren`, `canShare` true. Root metadata
  trả3permissions dạng user, không có anyone/domain. Chưa map3Google emails
  với roster hoặc audit đầy đủ inherited/descendant ACL; không coi là SEC-01
  cloud acceptance. Không lưu email/permission IDs/token vào evidence.
- Đã tạo và readback đủ6required directories dưới
  **`dat03_team:data/dat03/`**; giữ nguyên `data/` và dữ liệu cũ. Không upload
  dataset, delete hoặc mutate ACL. [Evidence metadata](../evidence/DAT-03_DRIVE_SETUP_20261026.json).
- `about` đọc được quota account hiện đăng nhập; đây không phải dung lượng
  được reserve cho folder nhóm hay bảo đảm quota của mọi file owner. Không
  dùng quota account làm lý do bỏ physical4GiB reserve trên máy ETL.
### Lịch sử lỗi trước khi chuyển sang client riêng

Các mô tả sau không phải trạng thái kết nối hiện tại; custom client đã kiểm
read/write thư mục ở trên. Chưa nghiệm thu file transfer hoặc research data.

- Remote cũ `gdrive` dùng `scope=drive.appfolder`, root `appDataFolder`;
  đây là vùng ẩn của ứng dụng, không dùng truy cập folder share thông thường.
- Đã tạo remote `dat03_team` riêng với backend `drive`, `scope=drive`,
  `root_folder_id` đúng folder ID phía trên. Giữ nguyên remote cũ và token cũ.
- Owner đã đồng ý xác nhận read/write OAuth trong browser. Root folder chỉ
  chọn điểm bắt đầu thao tác, **không thu hẹp quyền OAuth về đúng một folder**.
- OAuth browser đã hoàn tất, token mới lưu ở rclone config local với mode0600,
  không vào repo. Không dump token, không sao chép token cũ sang scope mới.
- **Lỗi cũ — BLOCKED_SHARED_CLIENT_QUOTA:** stat/list/about từng nhận Google403
  `rateLimitExceeded`, quota metric Queries/requests per minute của shared
  OAuth project. Không phải lỗi password, không chứng minh thiếu quyền folder
  và **không phải thông báo hết storage Drive**. Tại mốc đó chưa xác nhận
  read/write, tên folder, quota storage hoặc effective ACL.
- Phiên shared client đã dừng sau ba read-only checks, không retry vòng lặp
  hoặc xin tăng quota project không thuộc nhóm; chưa tạo folder hoặc upload.
- Shared client mặc định bị rclone cảnh báo loại bỏ trong2026. Đã chuyển sang
  [OAuth client riêng](https://rclone.org/drive/#making-your-own-client-id)
  rồi re-authorize chính remote này và kiểm lại thành công. Không chuyển chỉ
  Client ID mà giữ refresh token cấp cho shared client; không fake acceptance.

## Máy thành viên

Đọc release pin và offline checks trong team worker guide trước. Không copy
token hoặc `.env` của HoaiTam. Tạo remote riêng bằng `rclone config`, chọn
`drive`, scope read/write `drive`, folder ID phía trên; OAuth bằng tài khoản
được share. Không chọn `drive.appfolder`/`drive.metadata.readonly` cho upload.
Nếu máy đã có `dat03_team`, kiểm root/scope mà không dump token; không ghi đè.
Không dùng account khác chỉ vì browser tự chọn mặc định.

## Kiểm tra sau xác thực

Các lệnh chỉ đọc dưới đây không upload/delete/đổi ACL; OAuth có thể refresh
token local. Nếu403/404, kiểm account/quyền share, không tự mở public folder.

```bash
rclone listremotes --long
rclone lsd dat03_team: --contimeout 10s --timeout 20s --retries 1 --low-level-retries 1
rclone lsd dat03_team:data/dat03 --contimeout 10s --timeout 20s --retries 1 --low-level-retries 1
rclone about dat03_team: --contimeout 10s --timeout 20s --retries 1 --low-level-retries 1
```

Expected: remote tồn tại, root/scope đúng; listing exit0. Folder trống có thể
không in dòng nào, không tự hiểu là lỗi. `about` có thể thiếu thông tin quota;
ghi UNKNOWN và kiểm quota tài khoản/chủ sở hữu trên web, không gọi unlimited.
Listing thành công chỉ chứng minh read, không chứng minh write/ACL private
hoặc đã nghiệm thu SEC-01 cloud. Không in `rclone config show/dump` vào log.
Lần chốt setup đã kiểm lại `lsd dat03_team:data/dat03`: exit0, thấy đủ
`accepted`, `receipts`, `shared-inputs`, `workers`. Sáu required directories
(gồm ba worker subfolders) đã có mkdir + exact-path stat receipts trước đó.

## Tạo OAuth client riêng — owner thao tác trên Google Cloud

Owner đã tự tạo client và cung cấp JSON local để cấu hình. Các bước dưới giữ
lại để hai máy thành viên setup; agent không tạo/bật billing hoặc yêu cầu nâng
quota project của rclone. OAuth client và quyền share folder là hai điều riêng biệt.

1. Mở [Google Cloud Console](https://console.cloud.google.com/), chọn **New
   project**, đặt tên `IE212-DAT03-Drive`; chọn đúng project sau khi tạo.
2. **APIs & Services → Library → Google Drive API → Enable**.
3. **Google Auth platform → Get started** (nếu chưa cấu hình): Branding đặt
   tên `IE212 DAT03`, support/developer contact là email owner. Audience dùng
   **External** nếu có account ngoài một Workspace organization; chỉ dùng
   Internal khi cả nhóm thuộc organization được hỗ trợ. Giữ **Testing** để
   nghiệm thu ban đầu; thêm đúng ba Google emails vào **Audience → Test users**.
4. **Data Access → Add or remove scopes**: thêm
   `https://www.googleapis.com/auth/drive` cho read/write đã được owner duyệt,
   lưu cấu hình. Không thêm Gmail/Sheets/scopes khác cho việc archive ZIP.
5. **Clients → Create client → Desktop app**, tên `DAT03 rclone desktop`,
   Create. Không chọn API key, Service account hoặc Web application. Desktop
   client dùng local browser callback; không đưa secret vào redirect URI.
6. Tải credential JSON, lưu **ngoài repo/staging và folder Drive chia sẻ**,
   ví dụ `/Users/hoaitam/Downloads/ie212-drive-oauth.json` trên máy owner.
   Không gửi nội dung JSON/Client Secret/token qua chat hoặc commit. Có thể
   báo agent đường dẫn local để cấu hình bằng cách không log giá trị secret;
   file nhạy cảm này không phải gói dữ liệu bàn giao nhóm.
7. Cập nhật **đúng `dat03_team`**, giữ scope/root folder và remote `gdrive`
   cũ. Dùng Client ID/Secret mới, bắt buộc re-authorize để lấy refresh token
   thuộc client mới; không tái dùng token của shared client. Owner tự xác nhận
   đúng Google account trong browser; sau đó chạy lại read-only checks.

Theo [Google OAuth](https://developers.google.com/identity/protocols/oauth2),
refresh token cho External/Testing với Drive scope hết hạn sau7ngày. Đây là
giới hạn của setup thử; chưa nghiệm thu unattended dài hạn. Sau kiểm thử mới
review Production/publishing và yêu cầu verification của Google, không publish
app hoặc khai privacy/domain giả chỉ để né expiry. Không hứa client riêng
loại bỏ mọi quota: nó tách khỏi shared-client quota, vẫn có limits riêng.

Nguồn tạo client: [Google credentials](https://developers.google.com/workspace/guides/create-credentials),
[consent/scopes](https://developers.google.com/workspace/guides/configure-oauth-consent).

## Layout đã tạo và gate còn lại

Theo yêu cầu setup của owner, sau xác thực/metadata readback đã tạo các folder
riêng bên trong `data/dat03/`, không rải vào root project hoặc di chuyển
`data/` đang có. Remote root vẫn là folder nhóm **IE212**:

```text
dat03_team:data/dat03/
├── shared-inputs/
├── workers/
│   ├── HoaiTam/
│   ├── ThanhTris/
│   └── rosy179/
├── accepted/
└── receipts/
```

Các folder đã được tạo bằng mkdir và đọc lại từng path thành công, không có
dataset bên trong do phiên setup này upload. Shared folder My Drive không tự
có quota riêng; quyền thư mục cha có thể kế thừa. Worker paths là quy ước
ownership, **chưa được ACL-isolate** theo mỗi người. Người dùng cần xác nhận
effective permissions và chỉ cấp quyền ghi cần thiết trước handoff thật;
không tự sửa quyền đã có hoặc bật public trong lúc setup.

DAT-03 vẫn cần range worker/portable ETL export/import/merge trước chia187tháng,
và full288/source/publication/official ML export/ba máy receipts trước Done.
SEC-01 local Done không chứng nhận Drive; phải mở lại Needs Update và nghiệm
thu cloud trước upload dataset. Phiên này chỉ setup và root permission read;
không scan toàn Drive, upload/readback payload hoặc dùng Drive làm live warehouse.

Nguồn: [rclone Drive](https://rclone.org/drive/),
[quota/about](https://rclone.org/commands/rclone_about/),
[Google sharing](https://support.google.com/drive/answer/2494822?hl=en).
