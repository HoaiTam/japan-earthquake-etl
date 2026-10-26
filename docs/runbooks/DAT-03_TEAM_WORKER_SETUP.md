# DAT-03 — Agent setup/check trên máy ThanhTris và rosy179

Cập nhật 26/10/2026. Phạm vi: chuẩn bị hai máy, kiểm tra code/input, cấu hình
kênh bàn giao và báo cáo readiness. **Không phải lệnh khởi chạy distributed
rollout.** Không có quyền mặc định upload, đổi ACL hoặc xóa dữ liệu từ tài liệu này.

## 1. Đọc trước và giới hạn hiện tại

Agent đọc `AGENTS.md`, [DAT-03](../task/tasks/DAT-03.md),
[progress](../evidence/DAT-03_PROGRESS.md),
[batch execution](./DAT-03_BATCH_EXECUTION.md). Phần storage/CAS được giải thích
trong guide này; owner phải cung cấp cùng phiên bản tooling/contract đã push
trước khi cho máy mới đọc observation theo layout mới.
Task/index/evidence mới hơn ưu tiên số liệu lịch sử trong hướng dẫn này.

- Checkpoint đã nghiệm thu: **101/288 tháng**, 2000-01 đến 2008-05 inclusive;
  [receipt101](../evidence/DAT-03_CATALOG_CHECKPOINT_101.json).
  Còn187tháng; không chạy lại101tháng chỉ vì clone máy mới không có staging.
- Controller hiện kiểm **contiguous catalog prefix**, không nhận một worker
  tự nhảy đến2013hoặc2018. Chưa có portable24-month ETL bundle/importer hay
  phép ghép ba Gold catalogs thành một publication đã nghiệm thu.
- Host CAS chỉ lưu bản quan sát/artifacts, **không thay backup warehouse**.
  Descriptor `observation.json` không chứa toàn bộ bytes: còn cần frozen inputs
  và referenced blobs. Không gửi descriptor một mình như dataset.
- Official ML export vẫn cần full288tháng, publication/pins/quality/lifecycle
  gates; xem [ML export](./DAT-03_RESEARCH_ML_EXPORT.md). Không sửa flags để pass.
- SEC-01 hiện Done **local Docker/Jupyter**. Drive ACL/token/sharing cần
  cloud acceptance bổ sung theo [SEC-01](../task/tasks/SEC-01.md), không tự ghi Done.

## 2. Các mức readiness

| Mức | Làm gì | Điều kiện đạt / giới hạn |
|---|---|---|
| CODE_READY | Clone đúng release, chạy offline checks | Không cần Docker, nguồn thật hoặc pipeline credentials |
| DRIVE_READY | Người dùng đăng nhập, chọn đúng folder, kiểm truy cập/quota | Không đồng nghĩa cloud security đã nghiệm thu hoặc đã upload |
| RUNTIME_READY | Kiểm stack local riêng sau setup được duyệt | Healthy/freshness, resource, isolated namespace; chưa được chạy rollout |
| RANGE_READY | Worker/export/import contract và tests đã có | **Chưa triển khai/ nghiệm thu**; dừng trước ETL chia khoảng tháng |
| BATCH_ACCEPTED | ETL, reconciliation, publication và portable export đạt | Chưa thể phát hành chỉ từ guide này |
| REMOTE_VERIFIED | Remote bytes + independent restore/semantic verifier đạt | Không tự cho phép xóa warehouse/CAS/source/checkpoint |

Chỉ nhận trạng thái của mức đã có bằng chứng. Unit tests xanh không là
RUNTIME_READY, BATCH_ACCEPTED, official EXPORTED hoặc scientific approval.

## 3. Owner phải gửi trước khi hai máy nhận việc

- Repository: `https://github.com/HoaiTam/japan-earthquake-etl`.
- **Exact full commit SHA đã push** chứa hướng dẫn/tooling cần dùng; branch/PR
  chỉ để tìm, không dùng `latest` làm identity. Owner chưa công bố SHA thì dừng.
  Khi viết guide, storage tooling còn thay đổi local; clone GitHub không tự có nó.
- Tên người nhận (`ThanhTris` hoặc `rosy179`), khoảng tháng được duyệt và role.
- Release nguồn/config: exact inventories, source URLs/releases/bytes/SHA,
  parser/code/config versions, cutoff, policies và USGS overlap. Không chỉ gửi
  URL rồi để mỗi máy tải version mới khác nhau. Nguồn tải lại phải khớp SHA.
- Folder Drive được chia sẻ riêng theo email, folder ID, quy tắc quyền/quota;
  không gửi OAuth token hoặc `.env` của HoaiTam.
- Với một gói dữ liệu: tên/kích thước ZIP, trusted SHA256 ngoài ZIP,
  manifest SHA và loại gói: pilot, intermediate ETL hoặc official paired research.
- Release packet/worker contract còn thiếu thì báo rõ; không tự tạo metadata
  hoặc copy namespace/lease/catalog ID của máy HoaiTam để chạy trên máy khác.

## 4. Setup code và kiểm tra offline — làm ngay sau khi nhận SHA

Các command block dùng **macOS/Linux shell**, ở root clone riêng. Windows nên
dùng WSL2/Linux cho Make/shell; không coi guide là acceptance native Windows.
Input/output runtime không đặt trong thư mục đang được Drive/OneDrive sync.

### 4.1 Clone mới và pin commit

Chỉ chạy clone nếu chưa có thư mục đích. Nếu repo đã tồn tại: đọc `git status`,
giữ nguyên thay đổi, không reset/stash/switch để ép về sạch; báo owner nếu cần.

```bash
git clone https://github.com/HoaiTam/japan-earthquake-etl.git
cd japan-earthquake-etl
git status --short
read -r -p 'Full commit SHA owner đã duyệt: ' DAT03_APPROVED_COMMIT
```

`read -p` ở trên dùng Bash; trên zsh dùng `read -r 'DAT03_APPROVED_COMMIT?Full commit SHA owner đã duyệt: '`.
Sau khi nhận đủ40ký tự hex, chạy các lệnh sau; nếu kiểm SHA lỗi thì dừng,
không tiếp tục checkout. Đây là clone mới sạch, không dùng cho checkout đang chạy.

```bash
python3 -c 'import re,sys; sys.exit(0 if re.fullmatch(r"[0-9a-f]{40}",sys.argv[1]) else 1)' "$DAT03_APPROVED_COMMIT"
git fetch origin
git show --no-patch --format='%H %s' "$DAT03_APPROVED_COMMIT"
git switch --detach "$DAT03_APPROVED_COMMIT"
git rev-parse HEAD
```

Expected: HEAD đúng SHA owner gửi. Detached HEAD để kiểm release; nếu được giao
**sửa code**, tạo branch task theo AGENTS trước thay đổi, không tự push main.
Nếu SHA không có trên remote, báo `CODE_BLOCKED_RELEASE_NOT_PUBLISHED`.

### 4.2 Công cụ và offline checks

Cần Git, GNU Make, Python3.11–3.13. Các lệnh bên dưới không cài package global,
không cần `.env`, Java, Docker hoặc lấy dữ liệu nguồn:

```bash
git --version
make --version
python3 --version
test -f scripts/research-observation.py
make check-docs
make test-docs
make check-task-status
make test-research-observation
python3 -B -m unittest discover -s tests -p 'test_research_catalog.py'
python3 -B -m unittest discover -s tests -p 'test_research_catalog_progress.py'
python3 -B -m unittest discover -s tests -p 'test_research_ml_export.py'
git diff --check
git status --short
```

Expected: từng command exit0; unittest `OK`, không FAIL/ERROR; docs không broken
links,77task statuses khớp tại baseline này. Git vẫn sạch. Thiếu target/file
là sai release hoặc owner chưa push, **không bỏ test để báo CODE_READY**.
Không dùng `make check`/full Spark tests thay nhóm nhẹ này: chúng có dependency
khác và tốn tài nguyên, không cần cho việc kiểm guide/storage control plane.

## 5. Setup Drive — từng người tự xác thực

### 5.1 Folder và quyền

Owner tạo folder riêng `IE212-DAT03`, share đúng email ba người, không bật
“Anyone with the link”. Tách `shared-inputs`, `workers/HoaiTam`,
`workers/ThanhTris`, `workers/rosy179`, `accepted`, `receipts`.
Chỉ owner/người tích hợp cập nhật accepted sau review; worker chỉ ghi phần
được giao. Quyền cha được kế thừa: không cấp Editor ở root nếu cần giới hạn
worker chỉ sửa subfolder riêng. Xác nhận effective permissions bằng tài khoản
từng người, không chỉ nhìn tên folder. Kiểm storage quota của chủ sở hữu file;
đừng coi My Drive shared folder tự có quota độc lập của nhóm.

Không upload repo snapshot, `.env`, credential/IAM files, OAuth config,
private logs, JAR không được duyệt hoặc notebook output chứa token.
Drive là archive/transport, không mount làm live MinIO/Iceberg/CAS warehouse.

### 5.2 Rclone

Mac có Homebrew có thể cài `brew install rclone`; hệ khác theo
[official install](https://rclone.org/install/). Việc cài/xác thực do người dùng
cho phép trên máy mình; agent không tự xin quyền OAuth hoặc gửi credential.

```bash
rclone version
rclone config
```

Trong wizard: new remote `gdrive`, backend `drive`, dùng OAuth client riêng
theo [official client-id setup](https://rclone.org/drive/#making-your-own-client-id).
Tài liệu rclone hiện khuyến nghị client riêng vì shared client đang được loại bỏ.
Đăng nhập đúng Google account trong browser, không copy token giữa ba máy.
Advanced `root_folder_id` là ID folder owner gửi, lấy sau `/folders/` trong URL.
Folder share thông thường **không phải Shared Drive**; chỉ chọn Shared Drive
nếu trường/tổ chức đã cấp loại đó. Tránh chọn theo số thứ tự wizard vì có thể đổi.

Scope upload cần read/write. Scope `drive` có quyền rộng; root_folder_id chỉ
định điểm bắt đầu, **không thu hẹp OAuth permissions**. Nếu người dùng không
chấp nhận quyền đó, dừng OAuth và dùng Drive web thủ công; không tự nâng scope.
Giữ rclone config/token ngoài repo và gói upload, không dán config vào báo cáo.

Kiểm read-only sau khi người dùng hoàn tất wizard:

```bash
rclone lsd gdrive:
rclone about gdrive:
```

Expected: thấy các folder đã chia sẻ dưới đúng root; quota đủ cho dự kiến
gói và phần dự phòng. Shared Drive/tài khoản có thể không trả quota: khi đó
owner phải kiểm trên web/admin, ghi UNKNOWN, không coi là unlimited.
Sai folder/quyền/401/403 thì dừng, không tự đổi ACL hoặc service account.

## 6. Runtime local — chỉ khi người dùng yêu cầu chuẩn bị ETL

Nhận pilot/export để đọc notebook **không cần** dựng full stack. Nếu được giao
chuẩn bị worker ETL, đọc [local runbook](../LOCAL_OPERATIONS_RUNBOOK.md) và
[config contract](../specs/CONFIGURATION_AND_SECRETS.md). Mỗi máy dùng Docker
Desktop/Engine, stack và credential local riêng; không nối vào MinIO/DB HoaiTam.

Preflight chỉ đọc: `docker version`, `docker compose version`, `docker info`,
`df -h .`; ghi total RAM/Docker RAM/disk thực tế. `docker info` không phản hồi
thì ngắt lệnh xem sau khoảng20s và báo `RUNTIME_BLOCKED_DOCKER_UNRESPONSIVE`,
không prune/reset hoặc build thử. Host và Docker volume đều cần reserve4GiB,
**không coi đúng4GiB là đủ toàn job**; phải còn budget input/intermediate/export.
Không suy quota Drive hoặc logical free Docker thành physical host free.

Chỉ sau khi người dùng duyệt build/start trên **máy mới**:

```bash
make env-init
```

Người dùng thay toàn bộ `change-me-*` bằng secret local theo config contract;
Fernet phải đúng format. Không source/print `.env` hoặc gửi lên nhóm. Sau đó:

```bash
make check-config-local
make config
make docker-build
make docker-up WAIT_TIMEOUT=600
make status
make docker-check
```

Build chỉ một lần khi thiếu/stale image/JAR, không đặt lại vào mọi test.
Thành công: init exit0, long-running services healthy, freshness check exit0;
xem service-specific readiness theo local runbook, không chỉ “Running”.
Image/network/OOM lỗi thì giữ log đã redaction và báo; không down-v/rebuild
vòng lặp. Không trigger/unpause daily hoặc ML DAG để thử worker.

**Dừng ở RUNTIME_READY:** chưa có range-worker contract/entrypoint nghiệm thu.
Không gọi run/resume-research-catalog, đổi tháng trong request đã pin, clear
Airflow, release lease, hoặc copy warehouse để tự chạy phần được phân.

## 7. Phân phần dự kiến — chờ owner xác nhận RANGE_READY

Các mốc inclusive theo tháng UTC; khi tạo request dùng end exclusive đầu
tháng kế. Chia tháng không phải chia đều workload thực tế.

| Người | Phần còn lại dự kiến | Tháng | Gói bàn giao tối đa24tháng |
|---|---|---:|---|
| HoaiTam | 2008-06–2013-07 |62|2008-06–2010-05;2010-06–2012-05;2012-06–2013-07|
| ThanhTris | 2013-08–2018-09 |62|2013-08–2015-07;2015-08–2017-07;2017-08–2018-09|
| rosy179 | 2018-10–2023-12 |63|2018-10–2020-09;2020-10–2022-09;2022-10–2023-12|

Đây là **proposal, không phải thực thi hoặc đổi assignee task**.101tháng cũ
giữ làm input nghiệm thu; không tự thêm worker checkpoint vào chain hiện tại.
Gói24tháng gồm nhiều batch nhỏ: giữ <=12tháng/75.000profile rows/batch,
actual resolver100kselected rows và budget6000s Java/7200s host hiện có.
Hai tháng đông sự kiện2011không ép gộp; không truncate để vừa giới hạn.

Mỗi máy phải dùng namespace/run_id/lease riêng, cùng exact source/code/config
pins/cutoff; không dùng chung mutable registry. Ranh giới phân công không là
ranh giới khoa học: xử lý halo/JST/UTC/source-link closure và cross-worker
dedup khi tích hợp. PRE/POST strict split chỉ theo REPRODUCTION/EXTENSION,
không loại window chỉ vì vắt qua gói24tháng. Mc/audit/ML build theo whole split
sau tích hợp, không tự fit/tune hoặc tính Mc khác nhau từng worker.

## 8. Upload/verify/dọn local — chỉ cho gói đã được xuất và duyệt

**Chưa có lệnh export portable ETL24tháng trong release hiện tại.** Không dùng
`pack-research-team` cho gói ETL: công cụ đó dành cho hai official ML exports
đã đủ lifecycle, không export warehouse/checkpoint. Không chạy raw ZIP staging.

Sau khi owner cung cấp gói tự chứa + verifier/expected SHA đã nghiệm thu,
agent chọn exact folder chỉ chứa release files; gói phải gồm đủ dữ liệu,
manifest/file hashes, code/config/source pins, count reconciliation,
publication receipts và import/restore instructions. Snapshot IDs một mình
không portable sang catalog máy khác; importer phải bảo toàn lineage và
verify publication mới, không tự coi concatenate Parquet là merge Iceberg.

Ví dụ thao tác **rclone có thật**, không phải command export của repo. Hai biến
dưới phải được người dùng đặt thành đường dẫn upload local và remote riêng
của gói; không dùng staging root hoặc folder chứa secret. `test` thất bại thì dừng.

```bash
test -n "$DAT03_UPLOAD_DIR" && test -d "$DAT03_UPLOAD_DIR"
test -n "$DAT03_REMOTE_BATCH"
rclone copy "$DAT03_UPLOAD_DIR" "$DAT03_REMOTE_BATCH" --immutable --progress --dry-run
```

Người dùng duyệt exact file list/remote và SEC-01 cloud checklist trước actual
upload; dry-run không là bằng chứng truyền thành công. Không dùng sync/move.

```bash
rclone copy "$DAT03_UPLOAD_DIR" "$DAT03_REMOTE_BATCH" --immutable --progress
rclone check "$DAT03_UPLOAD_DIR" "$DAT03_REMOTE_BATCH" --download
```

Expected: copy/check exit0, không missing/different/error. `check --download`
đọc content remote để đối chiếu, tốn thêm băng thông nhưng không cần lưu một
full downloaded copy trên đĩa. SHA ngoài bundle vẫn giữ qua kênh tin cậy.
Người thứ hai phải tải vào folder mới, kiểm trusted ZIP SHA trước giải nén,
exact member set/path safety, chạy verifier **đúng loại artifact** và đọc dữ
liệu thật/đối soát count. Không chạy code từ ZIP không rõ SHA/người gửi.

Chỉ sau khi cả remote check và independent restore/semantic verifier đạt,
owner duyệt danh sách exact local files không còn consumer mới dọn:

- Có thể dọn: bản ZIP/upload spool hoặc export copy đã bảo đảm khôi phục.
- Không dọn theo guide này: frozen sources, active checkpoints, historical CAS
  referenced blobs, private failure evidence, Docker volume, warehouse hoặc
  Iceberg data/metadata. Xóa file đang thuộc snapshot sẽ phá time travel/readback.
- Không có GC/CAS eviction/lake retention được nghiệm thu: báo
  `CLEANUP_BLOCKED_REFERENCED_DATA`, không tự rm/prune để đủ đĩa.
- Đo physical free trước/sau; APFS COW/logical size hoặc kích thước ZIP không
  chứng minh số bytes đã giải phóng. Upload không tự giảm tăng trưởng warehouse.

Pilot hiện có đã dùng được theo [shared dataset](./DAT-03_SHARED_DATASET.md):
verifier hiện trả `TEAM_PILOT_VERIFIED`,279features/2mainshock,PARTIAL,
DEVELOPMENT_ONLY/research_ready=false. Receipt phải từ từng máy thật, không
dùng pilot để báo batch24tháng hoặc full research đạt.

## 9. Báo cáo agent phải gửi về

Lưu báo cáo ngoài input kit, dùng folder mới; gửi metadata đã redaction, không
private paths/hostname/token/credential hoặc full Docker inspect/config dump.

```text
Người nhận: ThanhTris hoặc rosy179 (self-reported)
OS/architecture; Python/Make/Git/rclone versions:
Approved commit SHA / actual HEAD / working tree clean:
CODE_READY: PASS/BLOCKED + từng lệnh, exit code, số test/lỗi
Drive: đúng root/quyền read-write/quota PASS/UNKNOWN/BLOCKED
Cloud ACL/security acceptance: PENDING/PASSED + reference
Runtime: NOT_REQUESTED hoặc PASS/BLOCKED + RAM/disk/freshness/healthy
Range: BLOCKED_RANGE_WORKER_NOT_IMPLEMENTED (cho tới release được duyệt)
Input package: NOT_RECEIVED hoặc tên/version/bytes/trusted SHA/local SHA
Assigned UTC interval: PROPOSED hoặc APPROVED + owner reference
Remote verification / independent restore: NOT_RUN hoặc evidence reference
Local deletion: NONE hoặc exact approved files + proof + physical delta
Blockers / safe next step:
```

Không báo “đã hoàn thành1/3” từ setup/tests/upload. Cần ETL acceptance thật
theo từng tháng, merge/import verification và full release gates.

## 10. Phần kỹ thuật còn thiếu và điều kiện bàn giao thật

- **DAT-03 — catalog/handoff:** range-aware worker plan/entrypoint, portable
  intermediate batch exporter/verifier/importer và coverage/lineage merge;
  chưa có task ID riêng cho ba phần này. Giữ DAT-03 In Progress.
- **DAT-03 — official research:** đủ288tháng source/publication, audit/Mc,
  strict windows, two-split official export/loader/rerun và receipts ba máy.
- **SEC-01 — cloud migration:** xác nhận OAuth scope/config storage, Drive
  effective permissions/quota, payload allowlist và quyền nhận/tải/khôi phục;
  chưa được chứng nhận bởi nghiệm thu local cũ. Hướng dẫn không đổi task Done
  local thành acceptance cloud mới.
- **EXP-02/EXP-03 — experiment:** local/official/scientific gates giữ nguyên;
  không tự chấp thuận model hoặc Colab từ một gói dữ liệu được upload.

## 11. Prompt ngắn gửi cho agent trên máy thành viên

```text
Đọc AGENTS.md và docs/runbooks/DAT-03_TEAM_WORKER_SETUP.md trong đúng release
commit do HoaiTam cung cấp. Kiểm tra máy tôi ở vai trò ThanhTris/rosy179 (tôi
sẽ xác nhận đúng tên). Trước hết chỉ chạy setup/check offline và chẩn đoán
read-only, giữ nguyên file/thay đổi có sẵn. Không tự cài/xác thực OAuth, upload,
đổi ACL, build/start/restart Docker hoặc chạy ETL khi chưa được tôi duyệt.
Không có approved release/input pins thì báo thiếu, không chọn main/latest
thay thế. Hoàn thành báo cáo mục9, phân biệt PASS/BLOCKED/NOT_RUN. Dừng trước
range ETL/export/delete vì contract chia máy chưa nghiệm thu; không tạo code
để vượt gate hoặc báo task Done từ setup. Không gửi secret/log riêng tư.
```

## Nguồn hướng dẫn công cụ

- [Google Drive sharing](https://support.google.com/drive/answer/2494822?hl=en).
- [Rclone Drive/OAuth/root folder](https://rclone.org/drive/).
- [Rclone copy/immutable](https://rclone.org/commands/rclone_copy/).
- [Rclone check/download](https://rclone.org/commands/rclone_check/).
- [Rclone quota/about](https://rclone.org/commands/rclone_about/).

Các nguồn công cụ được đối chiếu khi viết guide; actual OAuth/upload/restore
và môi trường hai thành viên **chưa được chạy/kiểm chứng trong lần viết docs**.

Kiểm tra trên máy soạn guide: docs links256files/0problems,10doc-checker tests,
77task status consistency;27observation +19catalog +4progress +16ML-export
unit tests đều PASS (offline fixtures/mocks). Không suy kết quả này thành
receipt hoặc acceptance runtime/Drive của ThanhTris/rosy179.
