# DAT-03 — Tiếp tục đêm 26/10/2026 và lưu trữ bản sao host

Owner cho phép bật lại các container cũ, chạy tiếp theo checkpoint và không
hỏi lại các bước vận hành trong phạm vi đã duyệt. Không mở quyền build/pull,
recreate/reset, đổi DAG/cron/credential hoặc xóa lakehouse để vượt lỗi.

## Khôi phục dịch vụ và mở phiên

- Bật đúng9containers cũ: PostgreSQL, MinIO, Iceberg REST, Trino, Spark master/
  worker, Airflow API/scheduler/DAG processor. Không gọi Compose hoặc hai init
  containers. Cả9healthy sau khởi động; inspect trước bật có OOMKilled=false,
  không suy exit137 trước đó là bằng chứng OOM.
- Preflight read-only đạt:4required services healthy,0active DagRun/Java/Trino
  query, source lease trống. Không trigger/clear hoặc sửa DB state.
- Dùng lại catalog `b6db356fb30e46f9a26a419ebacb5393`, continuation
  `bc29aca1e816437884a34eb17de19d29`, nguyên namespace/cutoff/source/code pins.
- Phiên `9d0e74bca4a548458d4e2b0e4a723def`: seconds6000/max-months7,
  target2006-01–07. Host dùng `caffeinate -i` trong vòng đời controller để
  tránh idle sleep; không đổi system power settings. Không bảo đảm chống
  mất điện, đóng máy hoặc Docker daemon bị tắt.
- Phiên đã terminal exit0/no timeout/own release, thêm7tháng đạt79/288 đến
  2006-07. Child wall1562,109s, peakRSS1.406.868KiB; host wall1593,629s.
  [Metadata079](./DAT-03_CATALOG_CHECKPOINT_079.json) giữ acceptance/result/
  execution SHA và six pins. Có843.404 canonical/843.404bridge, dim_date2.405
  gồm Unknown. Catalog27,431%, chưa mốc50% hoặc full DAT-03.
- Postflight4required services healthy, no active DagRun/Java/Trino query và
  lease trống. Đọc DB read-only xác nhận daily/MLI-02 DAG đều vẫn paused;
  không sửa pause/cron. Không tính clean restart là cold-start acceptance mới.

## Dọn bản sao đã nén theo yêu cầu owner

Chỉ năm **host observation copies** cũ13/20/27/45/63months, không phải active
runtime trong container. Trước nén: chạy reporter offline, verify original
source/code/request SHA, accepted result/execution/checkpoint/record chain và
own-release receipt. Bản72months mới nhất và thư mục `runtime` gốc giữ nguyên.

Mỗi ZIP tạo mới không overwrite, mode0600 trong `observation-archives/`
mode0700 của catalog host. Lưu external `.receipt.json` với ZIP SHA, exact file
set, từng file bytes/SHA/mode, directory list và original path. Reopen ZIP,
stream-hash **mọi member**, đối soát source không đổi; fsync/reload receipt và
verify lại ZIP trước unlink từng exact file/thư mục. Reject symlink, hardlink,
special file, file-set/hash drift và target ngoài allowlist. Không `rm -rf`.

| Host observation suffix | Tháng accepted | Files | ZIP SHA-256 |
|---|---:|---:|---|
|fa268b89bed244a298a4efe394c5590b|13|289|`7ef8cb128ba38174e03b7c4c9b5f8be771705f3f9b0e3721987b8e3164b5d390`|
|e47edaf60572488b9e0b2dd238d3dcb6|20|331|`f33bd75268378ba06d10246701f74de24af7068d0ab2f763ec6daafb654dda67`|
|070e681ac90a4899a3395cc8c833aac0|27|373|`90496719caeb1105e3abaeb12e5b470c1f419e1a80260d66fc4471f85fff031d`|
|f40296b56ddd4427b28fd18cb489df8b|45|475|`43f337111c4675e9917048440faaeb7b9268cbf4883832b5173f2597cd5923b5`|
|622db2c891ea4f7cbc0c9462d77ce064|63|578|`94fd6052b112eb2d6aca5746ef2a26b048ef47cb36268e7a816e829fa516eb73`|

Archive names là `runtime-observation-<suffix>.zip`; receipts cùng basename
với đuôi `.receipt.json`. Năm thư mục host gốc đã được xóa **sau verification**;
bytes vẫn khôi phục được từ ZIP. Không xóa source ZIP gốc, Parquet/Iceberg trên
MinIO, private logs bên trong archive hoặc bất kỳ runtime/lease nào.

- Original logical bytes1.614.956.451 → ZIP1.124.515.360; giảm490.441.091bytes
  (~468MiB). Không coi đây là số đo physical space recovered của APFS/Docker.
- Operational helper riêng máy nằm ở ignored
  `staging/dat03-cleanup-20261026/archive_observation.py` trong checkout code;
  không là shared CLI hoặc thay đổi pinned runtime controller.
-4offline safety tests PASS: roundtrip/unchanged source, changed member
  rejected, symlink rejected, broad/active/latest target rejected.

## Đọc lại evidence cũ sau lưu trữ

Các đường dẫn observation trong evidence lịch sử chỉ vị trí **trước lưu trữ**.
Để chạy lại reporter trên observation cũ, kiểm ZIP SHA theo bảng và receipt,
giải nén vào thư mục **mới, trống**, kiểm file-set/bytes/SHA/mode theo receipt;
chỉ khôi phục original path nếu path đó hoàn toàn chưa tồn tại. Không overlay
runtime hoặc chỉnh JSON cũ. Có thể dùng observation72 còn giữ nguyên để kiểm
checkpoint mới nhất mà không giải nén các bản trước. Không thay evidence cũ
thành acceptance của tháng mới.

## Ràng buộc dung lượng và phần còn lại

Host trước khởi động còn~6,9GiB; sau bật stack~4,5GiB, có lúc còn~3,8GiB
ngay cả sau lưu trữ. Đây là quan sát, chưa xác định nguyên nhân gốc. Không dùng
logical free trong Docker VM thay số đo này, không nới guard4GiB. Để phiên
đang ghi kết thúc tại checkpoint an toàn; không mở phiên mới dưới reserve.

### APFS copy-on-write cho bản sao host

Sau checkpoint79 terminal và postflight idle, tối ưu437exact duplicate files
giữa inputs/code gốc và các host runtime/observations72/79. Native `/bin/cp -c -p`
tạo file độc lập copy-on-write; không symlink/hardlink, không change file bytes.
Chỉ cùng filesystem/owner/group/xattrs và SHA/size trùng; tạo UUID temp, giữ
permission/mtime/flags/xattrs của target, rehash target/source/temp trước atomic
replace và kiểm lại sau. Không sửa container, MinIO, namespace hoặc snapshot.

Duplicate logical bytes854.262.782; host free quan sát4.012.044.288 →
4.870.823.936bytes. Không khẳng định toàn delta physical do COW vì OS còn hoạt
động. Receipt local ignored `cow-receipt-e507d464aced4e499a2c8b4b003c0c56.json`
cùng helper folder, SHA
`7af40de7805d6adf7a1de808bcff03e7766fe722fb2412db2e99b30099d5aa38`.
Reporter079 sau tối ưu vẫn khớp **toàn bộ output** trước; đường dẫn/file/SHA
giữ nguyên nên không cần restore để đọc những bản COW này. Hai safety tests
PASS: clone thật giữ metadata/bytes và independent-write isolation, different
bytes bị chặn trước replace. Lần fixture đầu bị Python macOS thiếu os.listxattr;
đã sửa helper dùng native xattr trước khi chạy trên catalog, không nới xattr guard.

Fresh preflight healthy/idle/lease-empty đạt, physical free~4,4GiB. Mở phiên
tiếp theo cùng pins/cutoff, max-months8 từ frozen plan:2006-08–2007-03,
74.764profile rows (planning only), budget6000s. Chưa tính thêm tháng trước
terminal acceptance. Nếu reserve thiếu lại thì dừng tại checkpoint sạch.

**DAT-03 — Catalog nghiên cứu/handoff** vẫn In Progress cho full288months,
official two-split collection export/loader/rerun và receipts ba máy. Xem
[progress](./DAT-03_PROGRESS.md), [batch policy](../runbooks/DAT-03_BATCH_EXECUTION.md)
và [task](../task/tasks/DAT-03.md). Chưa mở scientific/Colab gates.
