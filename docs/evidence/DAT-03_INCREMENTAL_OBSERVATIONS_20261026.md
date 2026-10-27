# DAT-03 — Incremental observations acceptance 26/10

Branch `fix/dat-03-incremental-observations`, dựa trên DAT-03 rollout
`3625f9a`; local main còn cũ, không dùng làm baseline thiếu code rollout.
Giữ thay đổi tài liệu ZIP trial chưa commit của phiên trước, không overwrite.
Assignee HoaiTam/reviewer unassigned; **DAT-03 vẫn In Progress**.

## Thay đổi được nghiệm thu

Theo [contract](../specs/RESEARCH_HOST_OBSERVATIONS.md), host collector không
`docker cp` toàn runtime nữa.119frozen inputs giữ ở catalog gốc; artifact
được lưu một lần theo SHA, observation đầy đủ bằng manifest. Latest đọc đủ
nội dung qua view; có explicit restore để lấy lại cây thư mục thông thường.
Không symlink/hardlink, không nới acceptance, không thay business ETL hoặc
source cutoff/namespace/pins. Không build/pull/restart hoặc tạo scheduled run.

## Thực nghiệm trên dữ liệu thật đã có

- Migration10host observations: prefixes1/20/27/45/63/72/79/87/93/101.
  Chạy reporter trước/import/CAS-view/cutover/sau prune, output acceptance
  nguyên vẹn. Recovery descriptor/receipt và file data được fsync trước unlink
  bản copy cũ. Original paths giữ descriptor; không còn physical full tree.
- CAS sau migration/read-only probe:469unique blobs,507.853.559bytes. Mọi
  private-log/artifact bytes của10targets được giữ trong CAS;119frozen files
  chỉ reference exact catalog gốc. ZIP của observation13 giữ nguyên như trial.
- Collector đọc runtime thật trong Docker, **không ETL mới**: nhận logical
  snapshot675.534.344bytes, chuyển **0payload bytes/0new payload objects** vì
  nội dung đã có. Chỉ thêm manifest/note. Reporter khớp exact checkpoint101,
  latest pointer không đổi. Điều này không dự báo batch mới cũng bằng0bytes.
- Restore toàn bộ checkpoint101 vào thư mục mới:808files/675.534.672bytes,
  full file-set/size/SHA/mode và reporter khớp trước/sau. Persist restore proof
  rồi chỉ xóa exact temporary restore folder do thử nghiệm tạo; CAS/manifest
  và original source không mất. Không sửa cached runtime/container artifacts.
- [Metadata/hash anchors](./DAT-03_INCREMENTAL_OBSERVATIONS_20261026.json).
  Full private migration receipts giữ ở catalog `host-observations-v1/`,
  không commit blobs, source, payload hoặc private logs vào Git.

## Dung lượng và giới hạn số đo

`du -h` catalog giảm khoảng4,6GiB→805MiB, **không phải thu hồi3,8GiB vật lý**.
Host free tại đầu migration101 là6.479.974.400bytes, sau migration10targets
là6.486.114.304bytes: delta quan sát+6.139.904bytes (~5,86MiB), còn có OS/
Docker hoạt động. Trước đó host free đã tăng so với mốc3,5GiB của ZIP trial;
không gán mức tăng ấy cho migration này. Các bản cũ đã COW nên chủ yếu giảm
logical duplication; lợi ích cho phiên sau là không copy/ghi lại full history,
giảm temporary transfer peak. Không hứa đủ disk cho187tháng/official export.

Không garbage-collect blob theo latest: lịch sử có thể còn reference. Không
chạy helper `overnight_catalog.py`/ZIP/COW cũ trên layout mới; chưa tự cập nhật
hoặc hồi sinh automation. Dùng CLI/version mới trên nhánh này. Không downgrade
host tooling khi historical paths đã trở thành descriptor.

## Kiểm tra và phần còn lại

- Storage fixtures kiểm dedup/source reuse, hash/missing blob, tamper/traversal,
  symlink/hardlink, readonly view, full restore/isolation, foreign lock,
  interrupted prune, remote drift, transfer framing/timeout/disk và typed prefix.
- Catalog/progress và ML export consumer regression chạy lại; full288/EXPORTED
  gate giữ nguyên, session từ catalog khác bị chặn. CI docs-base dùng mocks;
  log test stop/restore/failure không phải hành động thật trên deployment.
- Docs/task/config/whitespace/secret checks được chạy trước bàn giao.
- Chưa chạy một ETL batch mới với host wrapper cuối phiên này. Core storage
  đã có real read-only capture và full restore acceptance; không gọi đó là
  scheduled ETL hoặc official ML acceptance.

[DAT-03](../task/tasks/DAT-03.md) còn187tháng catalog, full source/publication,
official REPRODUCTION/EXTENSION collection export/loader/rerun và ba máy team
receipts. Task không chuyển Done. Không tự commit/push/merge nhánh vá này.
