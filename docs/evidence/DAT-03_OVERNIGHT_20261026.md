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

Archive names giai đoạn này là `runtime-observation-<suffix>.zip`; receipts
cùng basename với đuôi `.receipt.json`. Năm thư mục host gốc đã được xóa
**sau verification** rồi được khôi phục đủ bằng COW ở bước dưới. Bảng ZIP SHA
ghi lịch sử, không là danh sách ZIP còn tồn tại sau bước compaction. Không
xóa source ZIP gốc, Parquet/Iceberg trên MinIO, log bytes hoặc runtime/lease.

- Original logical bytes1.614.956.451 → ZIP1.124.515.360; giảm490.441.091bytes
  (~468MiB). Không coi đây là số đo physical space recovered của APFS/Docker.
- Operational helper riêng máy nằm ở ignored
  `staging/dat03-cleanup-20261026/archive_observation.py` trong checkout code;
  không là shared CLI hoặc thay đổi pinned runtime controller.
-4offline safety tests PASS: roundtrip/unchanged source, changed member
  rejected, symlink rejected, broad/active/latest target rejected.

## Đọc lại evidence cũ sau lưu trữ

Sau bước COW bên dưới, cả năm đường dẫn observation lịch sử đã có lại đủ
file-set/bytes/SHA/mode và acceptance như trước; chạy reporter trực tiếp với
exact session ID đã pin, không cần giải nén. Không chọn latest observation
bằng filesystem mtime vì restore có thể đổi directory mtime; dùng accepted
prefix/session/proof. Các receipt ZIP vẫn lưu lịch sử nhưng ZIP trung gian
đã bỏ sau khi restore/report đạt. Không overlay runtime hoặc sửa JSON cũ.

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

### Hoàn tất compaction các bản quan sát13/20/27/45/63

Các ZIP tạo ở bước đầu chứa phần lớn bytes đã có nguyên vẹn trong observation79.
Sau verify từng ZIP/SHA/member và receipt, khôi phục original paths **chỉ khi
chưa tồn tại**.2.045files tạo bằng independent COW từ byte-identical files
trong observation79; một file host-observation.json riêng481bytes giải nén từ
ZIP. Đủ2.046files, file-set/size/SHA/mode và toàn bộ progress output của từng
prefix khớp trước lưu trữ. Không tự gán mtime/xattrs lịch sử vốn ZIP không pin.

Persist/fsync/reload restoration receipt và verify source ZIP/root lần cuối
rồi chỉ unlink **năm ZIP trung gian do chính phiên này tạo**:1.124.515.360bytes.
Không xóa các receipt hoặc original source archives. Tất cả log/payload bytes
vẫn tồn tại tại original paths dưới dạng file độc lập, không hardlink/symlink.
Nếu restore fail trước acceptance thì giữ cả ZIP và partial folder, không retry
mù. [Metadata/receipt SHA](./DAT-03_HOST_COMPACTION_20261026.json) ghi đủ năm proof.
Host physical free sau đó quan sát~5GiB; không cộng logical bytes thành physical
reclaimed guarantee. Không coi compaction là acceptance ML/research.

Fresh preflight healthy/idle/lease-empty đạt, physical free~4,4GiB. Mở phiên
tiếp theo cùng pins/cutoff, max-months8 từ frozen plan:2006-08–2007-03,
74.764profile rows (planning only), budget6000s. Chưa tính thêm tháng trước
terminal acceptance. Nếu reserve thiếu lại thì dừng tại checkpoint sạch.

### Vòng nối tiếp vận hành

Owner yêu cầu chạy tiếp khi treo máy, không chờ nhắn lại giữa các batch sạch.
Operational helper ignored `overnight_catalog.py` chờ exact session thứ hai
`4ebb7eb671e048f4a746bfe90ee8124e` có host copy terminal/accepted; không launch
ETL khác trong lúc chờ. Có coordinator lock riêng (không là source lease),
metadata ở `overnight-0bff0c9a92be4312a01a9850821c01be` cùng helper folder.

Mỗi vòng: verify accepted prefix/result/execution/own release → fresh health/
idle checks → COW duy nhất new completed host observation và đối soát progress
trước/sau → physical reserve4GiB → gọi **existing** catalog controller cùng
original pins/cutoff/continuation. Free>=5GiB cho tối đa18tháng, nếu thấp hơn
thì đúng một next-fit batch<=12tháng/75kprofile; dừng đúng mốc144/216/288 khi
đến gần. Java vẫn tự kiểm100kactual bound và budgets6000s/phase reserves.

Nonzero/failure/timeout/unknown host outcome dừng vòng, giữ lock/evidence và
business lease; không retry/clear/cancel/takeover/restart. Thiếu physical
reserve thì kết thúc sạch, không nới guard. Không dùng directory mtime làm
proof sau restore; chọn exact accepted session/prefix. Giữ máy thức bằng
caffeinate trong vòng đời process, không đổi power setting. Năm guard tests
PASS: exact batch/split, reject boolean/final/negative prefixes, active session
không được nhận, accepted79 kiểm được, arbitrary session bị từ chối. Đây là
operational checks, không acceptance của288tháng hoặc official ML export.

Vòng chỉ xử lý catalog; khi đủ288 phải tiếp tục actual official two-split ML
export/loader/rerun và team handoff của DAT-03, không tự chuyển Done từ loop.

### Phiên thứ hai đã nghiệm thu

`4ebb7eb671e048f4a746bfe90ee8124e` terminal exit0/no timeout/own release:
thêm8tháng2006-08–2007-03, đạt87/288 (30,208%). [Metadata087](./DAT-03_CATALOG_CHECKPOINT_087.json)
có918.168canonical/bridge, child2173,924s/peakRSS1.382.972KiB; host2216,371s.
Postflight healthy/idle/lease-empty, COW exact new observation kiểm lại reporter
không đổi. Free sau COW5.574.135.808bytes ở event checkpoint của coordinator;
không dùng phần trăm này làm Done. Vòng đã mở phiên kế từ87/max18tháng/
6000s. Current lease có thể đã acquire lại bởi phiên kế, không gọi observation
release cũ là trạng thái lease hiện tại khi job mới đang chạy.

`make ci-docs-base` chạy sau phần cập nhật compaction đã PASS; các log mock
failure/stop/restore trong test không phải runtime actions. Docs lúc kiểm253
Markdown/2.281links/0problems, status77task khớp, git diff --check đạt. Metadata
087/link mới được kiểm lại riêng trước commit tiếp. Không build Java hoặc chạy
Spark fixtures song song với ETL; các tests Java/component trước giữ evidence cũ.

**DAT-03 — Catalog nghiên cứu/handoff** vẫn In Progress cho full288months,
official two-split collection export/loader/rerun và receipts ba máy. Xem
[progress](./DAT-03_PROGRESS.md), [batch policy](../runbooks/DAT-03_BATCH_EXECUTION.md)
và [task](../task/tasks/DAT-03.md). Chưa mở scientific/Colab gates.

## Dừng sạch tại checkpoint101

Ngày26/10, coordinator đã kết thúc bình thường với
`reason=HOST_DISK_RESERVE`, không failed child hoặc timeout. PID87371 không
còn sống, coordinator lock đã tự bỏ sau clean exit; không mở coordinator/ETL
thứ hai. Hai phiên terminal mới đã được reporter offline xác minh lại:

| Phiên | Prefix Published | Đến tháng | Canonical/bridge | Child wall | Own lease |
|---|---:|---|---:|---:|---|
|`8635dafd045149db8da0b93974978c67`|93/288|2007-09|990.281/990.281|2455,010s|RELEASED|
|`9515ebd43baa451a847e849fba3c103a`|101/288|2008-05|1.064.869/1.064.869|2545,568s|RELEASED|

[Metadata093](./DAT-03_CATALOG_CHECKPOINT_093.json) và
[metadata101](./DAT-03_CATALOG_CHECKPOINT_101.json) pin acceptance/result/
execution SHA, six snapshots, counts và own release. Latest host observation
`runtime-observation-f18d6f60bcb04592a34c8e42aee78edd` có
host_command_succeeded=true, host2582,908s; host entrypoint/source cutoff và
namespace giữ nguyên. Không suy `month=2007-10` trong batch log là toàn prefix;
reporter đã kiểm contiguous101months tới2008-05.

COW chỉ new terminal host copies, giữ bytes/paths/metadata và accepted
progress trước/sau; không sửa runtime container hoặc lakehouse. Local receipts
trong ignored `staging/dat03-cleanup-20261026/`:

- 93: `cow-receipt-5479b6dbd672456eb8be76eddb0cec3c.json`, SHA
  `e09f1b9043bbb37ce802f4af3056cd1c8504e5f4341e1037f2cbd5fdc9e1a25a`;
  duplicate logical604.060.485bytes.
- 101: `cow-receipt-df480fbacb8d436186c66147f19422ec.json`, SHA
  `6864764aa692d484d10c4a0cb009411c9b6256b55d280797bd8541809af9b9de`;
  duplicate logical639.423.381bytes.

Sau COW101, physical free quan sát3.913.207.808bytes; event checkpoint ghi
3.913.068.544bytes (<4.294.967.296bytes reserve). Không gọi logical duplicates
là physical space còn thu hồi được hoặc nới guard để tiếp tục. Chưa xác định
nguyên nhân toàn bộ tăng disk usage; không kết luận lakehouse/log là nguồn
gây tăng chỉ từ delta. Không xóa thêm source ZIP, warehouse, log hoặc evidence.

Fresh postflight read-only đạt:9containers healthy,4required services healthy,
0active DagRun/Java/Trino query, source lease trống. Không thay DAG/cron/pause,
DB state, credentials, service lifecycle hoặc business lease. Follow-up đã
xóa sau terminal handoff theo điều kiện owner đặt; không polling lặp khi
không đủ disk và không tạo automation thay thế.

### Bước tiếp tục của DAT-03

Owner cần giải phóng thêm dung lượng vật lý có chủ đích hoặc duyệt phương án
lưu trữ ngoài; không tự prune Docker/volume hoặc xóa dữ liệu khác. Mốc4GiB là
reserve tối thiểu để preflight, không bảo đảm đủ disk cho187tháng còn lại.
Sau khi đủ chỗ, kiểm lại health/quiescence/lease/pins/reserve rồi dùng existing
controller `resume` exact catalog/continuation từ checkpoint101, tháng kế
2008-06. Không prepare catalog mới hoặc dùng directory mtime chọn observation.

DAT-03 còn full288months, official REPRODUCTION/EXTENSION collection ML
export/loader/rerun và ba máy verifier receipts. Chưa actual research ZIP,
official_exported=false/research_ready=false; không dùng pilot thay research
hoặc chuyển Done. Mốc thông báo đã đạt vẫn chỉ25%, chưa50%.

### Kiểm tra cho commit bàn giao101

- Reporter offline kiểm lại prefix93/101 cùng exact observations/session và
  SHA chain; không biến báo cáo offline thành fresh Trino readback mới.
- `make check-docs test-docs check-task-status check-config ENV_FILE=/dev/null`
  PASS:253Markdown/2.292links/0problems,10docs regression tests,77task statuses
  khớp; configuration/secret hygiene repository/example đạt, không đọc local
  `.env` của deployment trong lệnh config check này.
- `git diff --check` PASS. Chỉ docs và hai checkpoint metadata được chọn để
  stage; không raw/source ZIP/JAR/private logs hoặc credentials. Không chạy
  Spark fixtures/build/deploy và không push/merge. Staged secret scan phải đạt
  trước local commit; kiểm cả pattern và configured deployment secrets, chỉ
  in tổng số findings, không in giá trị secret.
