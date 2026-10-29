# DAT-03 — Phục hồi range worker bị ngắt ở Verify

## Chọn recovery, không quarantine để chạy lướt qua

Luồng này phục hồi **Verify → Publish của batch Gold đã COMMITTED** trên đúng
máy/namespace bị lỗi. Không tải lại nguồn, chạy lại Spark/Gold commit, sửa failed
execution, xóa checkpoint hoặc tự thả lease để bỏ qua tháng lỗi.

Ca rosy179 ngày29/10 được chủ máy báo qua `error.md`:

- Range: `staging/dat03-range/3df82813b89e456b823f77d2a890bf3b`.
- Request SHA: `7b7069c8622e05556f42a11c49748160a2c308c9080c4abcab7966c2b929244e`.
- Failed session: `952973b42c8d4570b653ece99c79ed78`, exit1, timed_out=false.
- Báo cáo có33 checkpoint10/2018–06/2021; tháng đầu pending07/2021.
- Lỗi `TRINO_QUERY_FAILED` ở `GoldPublisher.verify`; Trino ghi
  `ABANDONED_TASK` trong khoảng máy sleep. Đây là bằng chứng phù hợp với
  gián đoạn runtime, không phải bằng chứng mọi quality check đều đã đạt.
- Một truy vấn chạy lại ra0 không thay full Verify/Publish/six-pin acceptance.

Đây là **thông tin bàn giao**, chưa phải nghiệm thu độc lập trên máy Trang.
Trước recovery còn30tháng chưa chốt; công cụ đọc `target_months` nguyên bản
(có thể nhiều tháng), không giả định chỉ07/2021. Chỉ cộng toàn batch sau acceptance.

## Điều kiện và những trường hợp phải dừng

Chạy trên **máy Trang**, Docker context local, đúng retained volume; không
chạy request Trang trên MinIO của HoaiTam. Giữ nguyên host folder chứa84 frozen
files, volume staging/warehouse và `.env` riêng của máy. Không chạy `prepare`
range mới. Không build/start/recreate Docker để áp dụng code này.

Các gate bắt buộc:

1. Request/source/config/controller/JAR37pins nguyên bản khớp; explicit request
   SHA lấy từ bàn giao, không tự lấy hash file tải về làm trusted pin.
2. Chỉ một session lỗi đã terminal: exit1, không timeout, chưa result/acceptance;
   log kết thúc ở Verify, có `TRINO_QUERY_FAILED`. Không nhận unknown outcome.
3. Không Java/DagRun/Trino query active; lease **vẫn đúng own range run**.
   Services healthy, không đổi ID/start/restart trong phiên; host trống>=4GiB.
4. Prefix contiguous được kiểm record/context/parent/stored receipts/publication;
   pending context được dựng lại từ đúng original inputs/parent/code version;
   Silver bundle/registry/SHA và37 Bronze descriptors được đọc kiểm độc lập.
5. Gold commit raw SHA, scope/baselines và sáu current snapshot khớp. Chưa có
   verify/publish artifact hoặc publication journal. Luồng này không xử lý
   ambiguous partial Publish; nếu gặp phải dừng và giữ evidence.

Sai quality/count, snapshot đã di chuyển, foreign lease, thiếu file, timeout,
session khác chưa resolved hoặc pending không có COMMITTED đều **fail closed**.
Không quarantine rồi resume, không sửa flags/SQL profile/timeout để pass.
Nếu attempt mới cũng lỗi: giữ lease/artifacts, không tự gọi lại `run`, không
xóa attempt/session để thử lại. Cần xem failure mới riêng.

## 1. Lấy code và build bổ sung trên host

Dùng branch/commit của bản vá do HoaiTam bàn giao; agent kiểm `git status` trước
checkout để không làm mất code local. Java17+, Python3.11+, Bash/WSL; Docker
stack cũ phải healthy trước. Không checkout/reset folder staging.

```bash
make test-dat03-range-worker test-dat03-team-setup
./mvnw -pl spark -am -Dtest=ResearchRangePublicationRecoveryTest,ResearchRangeStateTest,ResearchRangeRecoveryTest,GoldRangeNamespaceTest,ResearchCatalogRecoveryJobTest -Dsurefire.failIfNoSpecifiedTests=false package
make check-docs check-task-status
```

Build này không thay runtime runner: recovery JAR được tạo bằng **toàn bộ entry
nguyên bản byte-identical**, chỉ thêm `ResearchRangePublicationRecovery.class`.
Ngay cả manifest, publisher và Trino client cũng không đổi. Không copy JAR mới
đè `staging/dat03-range/.../runner.jar`, không rebuild image.

## 2. Chuẩn bị attempt từ trạng thái thật — không ghi S3

Đứng tại root repo deployment **trên máy Trang**, không phải worktree trên máy
Tâm. Lệnh đọc trạng thái runtime, kiểm own lease/quiescence, pin failure files;
chỉ tạo folder attempt mới ở host. Không in raw private logs.

```bash
DAT03_RANGE="$PWD/staging/dat03-range/3df82813b89e456b823f77d2a890bf3b"
DAT03_REPAIR=$(python3 -B scripts/dat03-range-recovery.py prepare \
  --deployment-root "$PWD" --folder "$DAT03_RANGE" \
  --failed-session 952973b42c8d4570b653ece99c79ed78 \
  --request-sha256 7b7069c8622e05556f42a11c49748160a2c308c9080c4abcab7966c2b929244e) || exit 1
printf '%s\n' "$DAT03_REPAIR"
```

Expected absolute `staging/dat03-range-repair/<uuid>`. Lưu đường dẫn này để
các bước sau dùng **cùng attempt**; không tìm latest theo mtime. Host metadata
chứa failed-file hashes, không chứa nội dung private logs. Giữ nguồn/JAR cũ.

## 3. Preflight chỉ đọc dữ liệu

```bash
python3 -B scripts/dat03-range-recovery.py preflight \
  --deployment-root "$PWD" --attempt "$DAT03_REPAIR"
```

Expected `event=dat03_range_repair_validated mode=preflight`, rồi
`RANGE_REPAIR_HOST_POSTFLIGHT_PASSED`. Host có `preflight.json`:
`status=READY_FOR_PUBLICATION_RECOVERY`, request/commit SHA, actual target_months
và13 metadata SQL checks (6current refs,6snapshot identity,1publication absent).
Các lần đọc MinIO kiểm byte/hash/receipt/registry/bundle; SQL là read-only.
Chưa publish/append checkpoint/release lease ở bước này.
Preflight chỉ chạy một lần/attempt; không ghi đè output cũ nếu đã lỗi.

## 4. Phục hồi đúng publication và nghiệm thu

Giữ máy cắm sạc/không sleep; macOS có thể bọc đúng lệnh bằng `caffeinate -i`.
Windows/WSL tắt sleep bằng thiết lập máy trong thời gian chạy. Không mở một
controller khác. Recovery có non-blocking OS lock riêng, không thay source lease.

```bash
python3 -B scripts/dat03-range-recovery.py run \
  --deployment-root "$PWD" --attempt "$DAT03_REPAIR"
```

Tool chạy Java **không Spark**: fresh six refs → full quality/outside-scope
Verify → Publish → full receipt chain/record → contiguous checkpoint cho đúng
target batch → sáu refs cuối. Không dùng kết quả SQL trong `error.md` thay query
thật. Giữ SQL profile/guard của original runner, không retry query mù.

Chỉ sau Java exit0, result/record/SHA checks và host service postflight đạt,
controller mới release **own** lease và tạo recovery acceptance + resolution.
Expected cuối: `event=dat03_range_repair_accepted completed=N`,
`RANGE_REPAIR_HOST_POSTFLIGHT_PASSED`; host `accepted-progress.json` ghiN/63.
33 checkpoint ban đầu chỉ trở thành prefix của phiên recovery accepted khi
cả chuỗi đã kiểm xong. Failed execution/stderr/stdout/intent giữ byte nguyên bản.

Nếu dừng giữa publication/checkpoint/finalize, không gọi `run` lần nữa; công cụ
chặn overwrite/ambiguous state. Gửi reason/session/attempt metadata đã lọc.
Không tự release own lease bằng tay chỉ vì Java đã kết thúc.

## 5. Resume phần còn lại bằng bridge đã pin

Worker cũ cố ý chặn mọi failed session, vì vậy resume cũ vẫn từ chối session lỗi
đã giữ lại. **Không sửa controller frozen** hoặc fake acceptance exit0 cho nó.
Bridge đọc resolution gắn với original failure hashes và recovery acceptance;
chỉ session này được resolved. Các session khác vẫn phải acceptance bình thường.
ETL tiếp theo vẫn dùng **original range runner**, không dùng recovery JAR.

```bash
python3 -B scripts/dat03-range-recovery.py report \
  --deployment-root "$PWD" --attempt "$DAT03_REPAIR"
python3 -B scripts/dat03-range-recovery.py resume \
  --deployment-root "$PWD" --attempt "$DAT03_REPAIR" \
  --seconds 6000 --max-months 12
```

`report` là mốc recovery accepted, không phải latest progress sau mọi phiên.
Sau mỗi resume, lấy **session mới được in**, rồi lưu receipt bằng report gốc:

```bash
read -r -p 'Session vừa accepted: ' DAT03_SESSION
python3 -B scripts/dat03-range-worker.py report \
  --folder "$DAT03_RANGE" --session "$DAT03_SESSION"
```

Lặp `dat03-range-recovery.py resume` sau mỗi phiên accepted/postflight, không
quay lại `dat03-range-worker.py resume` cho range này. Trần12tháng không cam
kết mỗi phiên đủ12: budget/profile phase guard vẫn giữ nguyên. New failure
giữ lease và chặn phiên kế tiếp. Không tạo automation/retry vô hạn.

## Bàn giao cho agent máy Trang

```text
Đọc AGENTS.md, DAT-03.md và DAT-03_RANGE_RECOVERY.md tại commit Tâm bàn giao.
Đây là máy rosy179, giữ range3df82813b89e456b823f77d2a890bf3b/session952973b42c8d4570b653ece99c79ed78.
Không quarantine/release lease hoặc replay ETL. Build host helper, prepare
attempt từ exact request SHA, chạy preflight rồi publication-only recovery.
Chỉ sau acceptance/postflight mới resume bằng pinned bridge, max12tháng/6000s.
Nếu bất kỳ guard thất bại, giữ artifacts/lease và báo reason đã che thông tin
nhạy cảm; không tự retry, đổi request/runner/namespace hoặc reset Docker.
Gửi actual completed/required, target_months, acceptance/receipt và six pins.
Không đổi DAT-03 Done: còn portable merge/full288/official export/receipts ba máy.
```

## Giới hạn acceptance

- [DAT-03](../task/tasks/DAT-03.md): bản vá có offline regressions; chưa thực
  thi recovery case này trên máy Trang. Full request/volume/snapshot chỉ máy
  Trang có; `error.md` không cung cấp nguyên file request hoặc full evidence.
- [DAT-03](../task/tasks/DAT-03.md): portable catalog merge, official ML export
  và receipts ba máy vẫn riêng; recovery ETL không hoàn tất các gate đó.
- Không upload Drive, đổi IAM/ACL, clear DAG, dọn staging hoặc đổi lịch trong flow này.
