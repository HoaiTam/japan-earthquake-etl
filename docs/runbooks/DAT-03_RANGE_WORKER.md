# DAT-03 — Worker theo khoảng tháng và USGS 2023 frozen

## 1. Phạm vi release

Thay blocker **chưa có range worker/USGS transport** của setup PR #106.
`scripts/dat03-range-worker.py` điều khiển `ResearchRangeJob` (Spark Java),
dùng lại real Bronze → Silver → six-table Gold → Trino publication.
Không sửa request, continuation, runner hoặc namespace catalog đang chạy của HoaiTam.

| Owner | Khoảng UTC `[start,end)` | Tháng | Tháng chạy thử đầu |
|---|---|---:|---|
| ThanhTris | 2013-08 đến 2018-10 | 62 | 2013-08 |
| rosy179 | 2018-10 đến 2024-01 | 63 | 2018-10 |
| HoaiTam | 2008-06 đến 2013-08 | 62 | Không khởi tạo lại; tiếp tục catalog cũ |

Mỗi máy có MinIO/Iceberg riêng, namespace `iceberg.dat03_range_<uuid>`,
lease owner `dat03_range_worker/dat03-range-<uuid>` và chain bắt đầu ở tháng
được giao, không bắt đầu từ2000. Mỗi phiên tối đa12tháng, vẫn chia technical
batch theo75000 profile rows và không vượt split; budget6000s, tối đa6600s.
Không nới100000 selected-row guard hoặc bỏ Trino checks để chạy nhanh hơn.

**Gate thực tế:** code/unit tests + prepare/source verification không phải
runtime acceptance. Phiên đầu bị ép `--max-months 1`; chỉ sau receipt accepted
trên máy đó mới cho phiên12tháng. Hai máy không cần chờ nhau hoặc chờ full288.
Portable Gold export/import/merge thuộc **DAT-03**, vẫn chưa được nghiệm thu;
điều này không chặn nghiệm thu ETL local, nhưng chặn gọi output là catalog đã
ghép/official ML dataset. Giữ Docker volume, source và evidence; chưa xóa local.

## 2. Source release — không fetch lại USGS

Drive: [frozen range inputs](https://drive.google.com/drive/folders/1EuR7uISq_EhCkKX7NCWJPwkJtnD0PbZ6).

- Remote: `dat03_team:data/dat03/shared-inputs/range-inputs-v1/e0752283a868`.
- Trusted manifest SHA256:
  `e0752283a868c601b3f2f996ace0cb77cd2e869f9d2a4b729a76a1b2b54fcc25`.
- 79files: manifest +78payload; payload130206607bytes.
- 25 JMA ZIP h2000–h2024 +25 capture metadata; 12 USGS GeoJSON tháng2023
  +12capture metadata; baseline/boundary inventories, plan và USGS summary.
- Cutoff `2026-10-25T06:13:04.747Z`; giữ nguyên original response, URL,
  retrieve time, SHA, window/16second halo. JMA2024 chỉ phục vụ biên UTC2023.
- Không có `.env`, credential, JAR, private capacity logs hoặc warehouse.

Tải **folder nguyên vẹn**, không chỉ25JMA ZIP cũ. USGS có thể đã revision trên
API: fetch mới không thay được pin này. SHA expected phải lấy từ release code
đã được HoaiTam duyệt, không lấy từ manifest vừa tải để tự chứng minh chính nó.

```bash
rclone copy dat03_team:data/dat03/shared-inputs/range-inputs-v1/e0752283a868 ../dat03-range-inputs-v1 --immutable --checksum
python3 -B scripts/dat03-range-worker.py verify-inputs --inputs ../dat03-range-inputs-v1 --input-sha256 e0752283a868c601b3f2f996ace0cb77cd2e869f9d2a4b729a76a1b2b54fcc25
```

Expected `FROZEN_37_INPUTS_VERIFIED_NOT_RUNTIME_ACCEPTANCE`. Repo verifier đọc
exact allowlist/SHA/provenance; `prepare` còn chạy Java USGS structure/summary
validation trên12capture thật. Không execute script tải từ Drive. Mỗi thành
viên tự OAuth bằng tài khoản được share; không copy token của HoaiTam.

## 3. Build một lần và prepare

Dùng exact full commit HoaiTam bàn giao, checkout sạch. Đọc
[TEAM START](DAT-03_TEAM_START.md) cho build/start Docker với secret máy riêng.
Không build/start trong mỗi test. Chưa có runtime thì chủ máy cho phép
`make env-init`, điền secret, `make docker-build`, `make docker-up WAIT_TIMEOUT=600`.
Không trigger/unpause DAG. Cần scheduler/metadata DB/MinIO/REST/Trino healthy,
không Java/DagRun/query active và source lease rỗng. Host physical free >=4GiB.

Java17+ và Python3.11+ trên Bash/WSL. Build runner + pure contract regressions:

```bash
make test-dat03-range-worker test-dat03-team-setup
./mvnw -pl spark -am -Dtest=ResearchRangeStateTest,ResearchRangeRecoveryTest,GoldRangeNamespaceTest,ResearchCatalogStateTest,ResearchCatalogBatchPlanTest,ResearchUsgsCaptureCheckTest -Dsurefire.failIfNoSpecifiedTests=false package
make check-docs check-task-status
```

Lệnh này không chạy full Spark integration suite; CI/full acceptance vẫn riêng.
`prepare` không ghi S3 và không lấy lease. Chọn đúng owner; ví dụ máy Trí:

```bash
DAT03_WORKER=$(python3 -B scripts/dat03-range-worker.py prepare --deployment-root "$PWD" --inputs ../dat03-range-inputs-v1 --input-sha256 e0752283a868c601b3f2f996ace0cb77cd2e869f9d2a4b729a76a1b2b54fcc25 --owner ThanhTris)
printf '%s\n' "$DAT03_WORKER"
```

rosy179 thay `--owner rosy179`. Expected absolute folder
`staging/dat03-range/<uuid>` được in ra **sau** Java verifier đạt.
Ghi lại đường dẫn; không prepare UUID mới khi chỉ muốn resume phiên cũ.
Không copy request của HoaiTam hoặc sửa start/end trong JSON.

## 4. Chạy tháng đầu, rồi phiên tối đa12tháng

Máy cắm sạc, không sleep/gập máy. Dùng Compose mặc định `japan-earthquake-etl`
trên máy riêng; không trỏ Docker context đến máy/stack của người khác.

```bash
python3 -B scripts/dat03-range-worker.py run --deployment-root "$PWD" --folder "$DAT03_WORKER" --seconds 6000 --max-months 1
```

Expected, theo thứ tự:

1. `RANGE_EXACT_FILES_STAGED`:84exact input/code/request files vào volume staging.
2. `event=dat03_range_started session=... owner=...`: lưu session ID.
3. Spark chạy sáu phase và lưu private logs trong volume, không dump payload.
4. `event=dat03_range_accepted`, completed1/62 hoặc1/63, own lease RELEASED.
5. `RANGE_HOST_POSTFLIGHT_PASSED`: services unchanged, runtime idle.

Chỉ khi đủ bước4–5 mới tiếp tục. Lấy exact session ID vừa in để lưu receipt
host (lệnh hỏi ID không tự tìm latest theo mtime):

```bash
read -r -p 'Session ID vừa accepted: ' DAT03_SESSION
python3 -B scripts/dat03-range-worker.py report --folder "$DAT03_WORKER" --session "$DAT03_SESSION"
python3 -B scripts/dat03-range-worker.py resume --deployment-root "$PWD" --folder "$DAT03_WORKER" --seconds 6000 --max-months 12
```

`report` là read-only trên runtime, kiểm result/execution/acceptance/request SHA,
contiguous checkpoints/record SHA/six snapshots/SQL reconciliation; lưu
`receipt-<session>.json` ở host. Cuối mỗi phiên đọc receipt mới rồi mới resume.
12tháng là **trần phiên**, không cam kết đủ12 nếu phase/budget guard dừng sạch.
Không mở hai controller đồng thời, không cộng tháng chỉ vì đã COMMITTED.

Nếu lỗi/timeout/pending/unknown: worker giữ lease và evidence; `resume` sẽ từ
chối phiên chưa accepted. Dừng, gửi session/request hash và reason đã lọc cho
HoaiTam; không release lease, retry, clear, reset hoặc prepare run mới né lỗi.

## 5. Bàn giao và giới hạn

Gửi full commit, owner, request SHA/namespace, session và receipt thật;
tháng đầu expected2013-08 hoặc2018-10, completed/required và six pins.
Agent báo ngắn `Batch … · N/62 hoặc N/63 · phase · truy vấn A/tổng`.
Worker hiện chưa expose counter SQL trực tiếp: ghi `chưa có counter`, không đoán.

- **DAT-03 — research handoff:** còn actual range acceptance mỗi máy,
  portable six-table export/import/merge với gap/overlap/lineage verification,
  full288, official two-split ML export và receipts ba máy. Các Iceberg snapshot
  ID của hai máy **không thể ghép bằng cách copy ID/metadata**.
- **SEC-01 — cloud handoff:** source release đã kiểm ACL user-only bằng tài
  khoản owner; vẫn cần từng người tự xác minh đúng account/download. Không bật public.
- Không gọi `RANGE_PUBLISHED` là `CATALOG_PUBLISHED`, không đổi research_ready
  hoặc scientific approval. Không xóa Docker volumes/data chỉ vì đã upload sources.
- Catalog cũ của HoaiTam vẫn dùng frozen continuation đã nghiệm thu. Không
  thay runner cũ bằng JAR range mới hoặc mở compatibility allowlist để ép
  upgrade; DAT-03 migration catalog cũ cần kiểm riêng nếu sau này cần đổi code.

[Evidence runtime](../evidence/DAT-03_RANGE_WORKER_20261028.md): đã chạy thật
2018-10 và resume2018-11,45.826canonical/bridge trên máy owner; đây không là
receipt máy rosy179. Lần thử2013-08 phát hiện namespace gate, đã giữ failure
và phục hồi Verify/Publish riêng; không dùng lại request/JAR lỗi ấy để rollout.

## 6. Prompt cho agent thành viên

```text
Đọc AGENTS.md, DAT-03.md và DAT-03_RANGE_WORKER.md ở exact commit HoaiTam gửi.
Tôi là ThanhTris (hoặc rosy179). Dùng frozen source release/SHA trong runbook,
không fetch USGS latest. Kiểm input, build runner, prepare range riêng.
Sau khi chủ máy cho phép runtime, chạy tháng đầu max-months1, kiểm receipt và
postflight thật. Nếu đạt mới resume từng phiên max-months12/budget6000.
Không sửa range/pins/guards, không release foreign/failed lease hoặc tự retry.
Gửi receipt và phạm vi đã PUBLISHED, giữ nguyên artifacts. Portable merge và
official ML export chưa có acceptance thì không gọi DAT-03 Done.
```
