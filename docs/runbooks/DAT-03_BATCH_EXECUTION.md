# DAT-03 — Chạy theo đợt, checkpoint và commit

Owner yêu cầu25/10: bắt đầu tiếp từ27/288 tháng và báo mỗi25% catalog.
Không thay source cutoff, namespace, source pins, daily DAG hoặc deployment.
Đây là kế hoạch thực thi, không phải evidence cho các tháng chưa chạy.

## Đơn vị công việc

- Batch dữ liệu: contiguous months, <=12 tháng/75.000 profile rows, không
  vượt split. Actual resolver vẫn guard100.000 selected rows, không truncate.
- Phiên runtime: budget6000s, outer deadline7200s gồm preflight/staging;
  reserve3600s trước batch mới và1200s trước phase mới. Profile đã pin giữ nguyên.
- Đợt bàn giao/commit: khoảng tối đa3h, gồm các phiên nhỏ đã nghiệm thu, không
  là một process timeout3h. Gần hết đợt, không mở phiên mới nếu thiếu budget;
  kết thúc tại accepted checkpoint sạch thay vì ngắt job đang ghi.
- Commit nền chứa code/test/contract. Commit sau ghi metadata/evidence của
  đợt thực tế; không đưa archives, Parquet, private logs, JAR hoặc credential
  vào Git. Không tự push/merge; owner chỉ yêu cầu commit theo đợt.

## Mốc báo cáo

| Mốc catalog | Tháng Published tối thiểu | Không bao gồm |
|---|---:|---|
|25% |72 |Official ML export/team receipts |
|50% |144 |Official ML export/team receipts |
|75% |216 |Official ML export/team receipts |
|100% |288 |Official ML export/team receipts |

Chỉ đếm contiguous checkpoints thuộc session terminal có receipt
`CHECKPOINT_ACCEPTED`, result SHA/execution SHA và own lease release đã đối soát.
Mốc đạt được báo ngay sau acceptance; không tính tháng đang chạy, sample hoặc
synthetic capacity. Báo lỗi mới ngay cả khi chưa tới mốc, không che bằng auto retry.
Ước lượng12,2h là ngoại suy27-tháng đầu, không SLA hoặc số đợt cố định.

## Vận hành

Lệnh resume và continuation được mô tả ở
[runbook rollout](./DAT-03_RESEARCH_ROLLOUT.md#continuation-tối-ưu-sau-checkpoint-sạch).
Tái dùng exact original catalog và compatible continuation đã nghiệm thu;
không prepare catalog mới để tiếp tục một checkpoint cũ. Mỗi phiên preflight
health/idle/own scope/disk, kiểm input pins, rồi mới acquire lease.
Host kiểm >=4GiB trống trên filesystem vật lý trước mọi Docker action;
worker còn kiểm riêng >=4GiB trên volume. Không coi logical free của Docker VM
là bằng chứng ổ Mac đủ chỗ, không tự xóa dữ liệu để vượt reserve.
Các phiên chạy bằng host controller mới còn ghi `host-observation.json` trong
observation mới: entrypoint SHA, physical disk before/after và host wall time.
Đây không phải child acceptance; không tự thêm số đo đó cho phiên cũ thiếu file.

Profile `dat03-select-poll-300s-v1` (27/10) opt-in trong compatible continuation
`ad0877a536e04062b069a8afa5257f5f`: research SELECT polling tối đa300s thay120s;
DDL/DML/default client vẫn120s/HTTP90s, không POST retry. Java6000s/host7200s,
memory caps, all16-bucket quality checks và source pins giữ nguyên. Recovery
V2 cần actual slow-query read-only probe đạt trước `run`; không dùng profile
này để gọi full-period capacity đã đạt. [Evidence](../evidence/DAT-03_CATALOG_VERIFY_RECOVERY_200906.md).
Owner đã cho phép tiếp tục sau17:00, vẫn tối đa2h/phiên và chỉ đến07/2013 phần
HoaiTam; không mở phiên khác trong lúc recovery/host capture đang active.

Không chạy Spark fixture tests đồng thời với ETL trên máy đang thiếu RAM.
Trên macOS, full Airflow fixture suite cần temp root không qua alias symlink:
`env TMPDIR=/private/tmp python3 -m unittest discover -s airflow/tests`.
Không tắt symlink guards trong runtime để né lỗi đường dẫn của fixture.
Không mở session khác khi session trước active, thiếu acceptance hoặc giữ lease.
Batch catalog đã COMMITTED nhưng chưa verify/publish cần
[publication-only recovery riêng](./DAT-03_CATALOG_PUBLICATION_RECOVERY.md),
không bỏ qua failed session hoặc release lease để gọi normal resume.
Nếu failed/timeout/partial, lưu nguyên artifacts/lease, chỉ chẩn đoán read-only;
recovery có thay đổi trạng thái cần xem xét riêng, không restart/delete để pass.

Owner26/10 cho phép lưu trữ bản sao host cũ để giảm dung lượng: chỉ unlink
observation đã accepted sau khi tạo ZIP/receipt mới, verify exact file-set và
từng member SHA, recheck source không đổi. Không xóa runtime/inputs gốc hoặc
lakehouse; giữ checkpoint mới nhất dạng thư mục. Evidence lịch sử cần restore
đúng observation trước chạy reporter cũ. Năm bản13/20/27/45/63 sau đó đã restore
COW đủ file-set/SHA/mode/accepted progress tại original paths và bỏ ZIP trung
gian; có thể dùng reporter trực tiếp với exact session, không suy latest từ
directory mtime. Trial mới theo yêu cầu owner đã giữ ZIP và xóa lại riêng
observation13 sau full restore verification; bốn bản20/27/45/63 vẫn dạng COW.
Host free của trial giảm169,52MiB nên không tiếp tục nén các clone khác theo
giả định logical saving. Restore observation13 từ ZIP/receipt mới trước chạy
reporter lịch sử đó. [Lịch sử lưu trữ và trạng thái hiện tại](../evidence/DAT-03_OVERNIGHT_20261026.md#thử-giữ-zip-và-xóa-bản-giải-nén-theo-yêu-cầu-mới).

## Tạo metadata cho commit tiến độ

### Host observation storage v1

Phiên catalog mới dùng [CAS/manifest](../specs/RESEARCH_HOST_OBSERVATIONS.md),
chỉ chuyển payload SHA chưa có; không copy lại119frozen inputs hoặc full
history. Giữ catalog root + `host-observations-v1/blobs` + descriptors cùng
nhau. Latest là complete read-only view, không phải cây file copy độc lập.
Đừng ZIP/xóa kho blobs hoặc dùng helper `overnight_catalog.py` cũ tự scan
physical files; automation cũ đã xóa, không tự mở lại.

`make test-research-observation` là storage regression offline. Các lệnh sau
dùng exact roots/session thực tế đã được đối soát, không đoán latest theo mtime:

```bash
python3 -B scripts/research-observation.py verify --catalog "$dat03_catalog_folder" --observation "$dat03_observation" --session "$dat03_session"
python3 -B scripts/research-observation.py restore --catalog "$dat03_catalog_folder" --observation "$dat03_observation" --session "$dat03_session" --output "$dat03_new_restore_folder"
```

Restore không overlay, cần output mới; chạy reporter so với original proof.
Compact là thao tác explicit khác, không test: cùng arguments và
`compact --remove-verified-legacy-files`. Chỉ sau idle/lease-empty và accepted
proof; giữ failure/lock nếu lỗi, không rerun prune mù. Không thu gom blobs.

`scripts/research-catalog-progress.py` đọc exact host observation đã được
controller copy sau phiên, xác minh frozen input bytes, acceptance/result/
execution SHA, contiguous checkpoint/record chain và own release. Không gọi
Docker, SQL, ETL, không ghi lease và không sửa observation. Chỉ chạy trên
observation chứa final accepted prefix; bản copy cũ thiếu tháng sẽ bị chặn.

Chạy `python3 scripts/research-catalog-progress.py --help` để xem các tham số
`--catalog`, `--runtime`, `--session`. `--output` phải là file JSON mới ngoài
catalog/runtime, chỉ chứa metadata allowlisted; không overwrite evidence cũ.
Không coi báo cáo offline là fresh current-pin readback hay full DAT-03 acceptance.

DAT-03 vẫn In Progress khi thiếu full288-month coverage, official collection
ML lifecycle/export/loader/rerun hoặc receipts thật từ cả ba máy. Xem
[task DAT-03](../task/tasks/DAT-03.md) và
[evidence catalog](../evidence/DAT-03_CATALOG_ROLLOUT.md).
