# DAT-03 — Incremental host observations

Version `dat03-host-observation-store-v1`. Đây là storage/control plane trên
host, không thay Java/Spark ETL, source cutoff, namespace, Gold pins, lease,
publication hoặc official ML gates. Pinned container controller/JAR giữ nguyên.

## Layout và ý nghĩa

```text
catalog/<119 frozen input files>          # một bộ source/config/code đã pin
catalog/host-observations-v1/
  blobs/<sha-prefix>/<sha256>             # một file cho mỗi nội dung khác nhau
  snapshots/<uuid>/observation.json       # recovery descriptor của migration
  migration-<uuid>.json                   # proof trước/sau, manifest SHA
  latest.json                            # latest accepted; không research approval
  writer.lock                            # exclusive host operation, không source lease
catalog/runtime[-observation-<uuid>]/
  observation.json                       # complete file manifest, không symlink/hardlink
```

Manifest của **mỗi** observation liệt kê đầy đủ file paths/bytes/SHA/mode và
directory names, khóa original catalog request SHA.119frozen files trỏ đúng
allowlist trong catalog root; artifact còn lại trỏ blob có tên SHA. Không có
absolute path, parent traversal, URL hoặc executable loader trong manifest.
Snapshot không phụ thuộc descriptor của snapshot trước, nên không có chuỗi
delta phải replay: lịch sử chỉ thêm manifest và nội dung SHA chưa có trong kho.

**Latest đầy đủ về nội dung**, đọc được tất cả artifacts qua virtual read-only
view, nhưng không là một bản physical full copy thứ hai. File explorer chỉ
thấy descriptor; cần materialize nếu công cụ khác yêu cầu cây thư mục cũ.
Không gọi manifest-only tự thân là backup: phải giữ cả frozen inputs và blobs
được tham chiếu. Không xóa blob chỉ vì latest không dùng: historical manifests
cũng có thể dùng nó. Chưa có garbage collector tự động.

## Capture cuối phiên

Host lấy exact read-only inventory từ container, validate names/bounds và
SHA frozen inputs, sau đó chỉ stream **missing unique payloads**. Kiểm SHA
trong lúc nhận, chống truncated/extra bytes, kiểm lại inventory container
không đổi, rồi fsync blobs/manifest trước publish descriptor mới. Không còn
`docker cp` full runtime sau mỗi phiên catalog. Transfer có deadline và chỉ
kill subprocess vận chuyển do host tạo nếu lỗi; không kill service/query/ETL.

Failed/unknown capture không được nâng thành latest accepted. File pending,
descriptor đã tạo và writer lock giữ lại để chẩn đoán; không tự retry hoặc
take over lock. Capture thành công vẫn phải qua reporter: exact acceptance,
execution/result SHA, contiguous checkpoint/record chain và own release.
Chỉ một session bao trọn prefix mới được cập nhật atomic `latest.json`.

## Đọc, khôi phục và consumer

Recovery27/10 thêm đúng một loại control artifact vào remote capture allowlist:
`session-resolutions/<32hex>.json`, pin riêng failed evidence và acceptance của
session recovery, không là data payload hay source lease. File khác dưới
prefix này (`.env`, nested path, invalid ID, latest.json) vẫn bị từ chối.
Không tự archive/unlock khi capture lỗi: case own dead writer lock của27/10
đã được chẩn đoán/lưu riêng rồi recapture chỉ đọc sau runtime acceptance;
xem [evidence](../evidence/DAT-03_CATALOG_RECOVERY_20261027.md). Generic capturer
không có auto takeover/retry, không release source lease hoặc replay ETL.

- `research-catalog-progress.py` đọc cả legacy directory và descriptor v1,
  output metadata giữ nguyên; full manifest integrity được kiểm, không chỉ
  những JSON reporter tình cờ đọc. Missing/corrupt blob hoặc source bị đổi
  làm fail, không tự tạo SHA mới để hợp thức hóa.
- Host `research-ml-export.py` dùng cùng read-only view. Gate full288 vẫn
  bắt buộc; worker trong Docker tiếp tục đọc ordinary runtime từ volume,
  không đọc manifest host hoặc dùng host JSON thay source acceptance.
- `research-observation.py restore` tạo **thư mục mới**, kiểm từng member và
  chạy reporter so trước/sau. Không overlay. APFS dùng independent COW clones
  khi có thể, không hardlink; thay file khôi phục không làm đổi blob/input.
- Script vận hành cũ tự `Path.rglob`, `sessions.iterdir` hoặc `docker cp` sẽ
  không tự hiểu layout mới. Không chạy lại ignored `overnight_catalog.py`/
  COW/ZIP helpers cũ trên descriptor. Dùng CLI/version này hoặc cập nhật
  consumer có test trước. Không hồi sinh automation đã xóa.

## Migration lịch sử

Explicit `compact --remove-verified-legacy-files` chỉ nhận exact host
observation và session có acceptance. Fresh services/idle/query/lease preflight
chỉ đọc trước thao tác. Kho host dùng writer lock riêng, không acquire/release
business lease. Không sửa volume Docker, MinIO/warehouse hoặc ZIP13 đã giữ.

1. Kiểm reporter/source inventory đầy đủ trước migration.
2. Import artifact vào CAS, reuse SHA đã có; APFS clone độc lập để tránh
   temporary full copy. Verify toàn file-set/SHA và acceptance lại.
3. Persist recovery descriptor và external receipt, fsync trước khi thay đổi.
4. Ghi descriptor vào original observation path **trước** unlink từng exact
   legacy file đã đối soát. Gián đoạn để lại view đọc đủ bytes và phần copy dư,
   không làm mất recovery manifest. Không tự retry phần prune dở.
5. Reporter sau migration phải khớp nguyên output trước. Không rewrite
   original acceptance/result/execution/checkpoint bytes hoặc scientific flags.

Metadata cũng được ghi trước/sau, nhưng `du`/logical size không là physical
space reclaimed trên APFS. Migration từ các bản đã COW có thể chỉ giảm rất
ít physical space. Lợi ích chính là không chuyển/ghi lại full history trong
các phiên tiếp theo, giảm temporary peak và tăng trưởng bản sao. Host4GiB
reserve trước ETL giữ nguyên; storage optimization không tự chứng nhận đủ disk.

## Lệnh và gate còn lại

`make test-research-observation` chạy fixture offline, không Spark/Docker.
`python3 -B scripts/research-observation.py --help` có verify/compact/restore.
Trước compact phải truyền exact catalog/observation/session và flag explicit;
verify luôn read-only. Restore phải truyền output mới và giữ đủ disk budget.

[DAT-03](../task/tasks/DAT-03.md) vẫn cần full288 source/publication, hai split
official export/loader/rerun và ba máy verifier receipts. Layout/test này không
là research-ready hoặc official EXPORTED. [Batch runbook](../runbooks/DAT-03_BATCH_EXECUTION.md).
