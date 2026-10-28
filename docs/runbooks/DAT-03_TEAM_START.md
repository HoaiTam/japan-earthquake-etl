# DAT-03 — Agent bắt đầu trên máy thành viên

Ngày28/10/2026. **Bước tiếp theo hiện hành:**
[Range worker + frozen USGS2023](DAT-03_RANGE_WORKER.md) có code prepare/run/
resume/report và source release trên Drive. Chạy một tháng nghiệm thu trên
từng máy trước rollout12tháng/phiên; không suy unit tests thành runtime PASS.
File này giữ setup/OAuth/runtime cơ bản của PR #106. `execution_enabled=false`
trong CLI setup chỉ nói CLI đó không execute; dùng worker mới, không sửa flag.

## 1. Phân công và những gì có sẵn

| Người | Khoảng UTC inclusive đã giao | Tổng | Đã accepted | Còn |
|---|---|---:|---:|---:|
| HoaiTam | 2008-06–2013-07 |62|40 đến2011-09|22|
| ThanhTris | 2013-08–2018-09 |62|Chưa có receipt máy Trí|62|
| rosy179 | 2018-10–2023-12 |63|Chưa có receipt máy rosy179|63|

101tháng2000-01–2008-05 đã có, không chạy lại. Catalog owner141/288;
không suy đã làm từ số tháng được giao. Task DAT-03 giữ HoaiTam làm coordinator.
[Config máy đọc](../../config/research/team_assignments_v1.json) pin cutoff,
inventory SHA, source manifest/verifier, từng range và gate chưa đạt.
Transport group tối đa24tháng **không phải** một ETL batch: vẫn phải theo
profile rows/budget. Hai split khoa học giữ nguyên225/63tháng, không theo owner.

## 2. Release code — agent không tự chọn main/latest

HoaiTam gửi **full commit SHA sau push** của PR này qua nhóm. SHA đó là release
setup, không phải runner ETL distributed đã nghiệm thu. Commit SHA không thể
ghi vào chính nội dung commit của nó; dùng pin ngoài repo do owner gửi.

Với checkout mới, terminal Bash/WSL; máy đã có repo dirty thì giữ nguyên, dùng
clone riêng, không reset/stash tự động:

```bash
git clone https://github.com/HoaiTam/japan-earthquake-etl.git japan-earthquake-etl-dat03
cd japan-earthquake-etl-dat03
read -r -p 'Full commit SHA HoaiTam gửi: ' DAT03_RELEASE
git fetch origin
git switch --detach "$DAT03_RELEASE"
git rev-parse HEAD
git status --short
make test-dat03-team-setup
make check-docs
make check-task-status
```

Đối chiếu HEAD đúng40ký tự pin, Git sạch, tests OK. Không copy `.env`, OAuth
token, Docker volume hoặc staging của owner. Python3.11–3.13, Make/Git.

In kế hoạch theo đúng tài khoản (rosy179 thay tên tại `--owner`):

```bash
python3 -B scripts/dat03-team-setup.py plan --owner ThanhTris
```

Expected `ASSIGNMENT_PINNED_NOT_EXECUTABLE`,62tháng08/2013–09/2018,
groups24/24/14; runtime và Drive `NOT_CHECKED`, không là lỗi parser.
HoaiTam có22tháng còn lại10/2011–07/2013; rosy179 có63tháng10/2018–12/2023.

## 3. Drive và dữ liệu nguồn — từng người kiểm tài khoản riêng

[Setup OAuth](DAT-03_GROUP_DRIVE.md), root ID
`17Wntw5LIRvFdzaU0RpnBRL60gzsRdFTB`. Dùng tài khoản Google được owner share,
remote `dat03_team:` của chính máy đó, không lấy config/token HoaiTam.
Owner đã gỡ anyone/writer tại data/dat03 ngày28/10, giữ ba user; việc này
không chứng minh máy Trí/rosy179 đã login/download được. Không bật public lại.

Tải nguồn vào folder **ngoài repo**, không sửa/giải nén archive gốc:

```bash
rclone lsd dat03_team:data/dat03
rclone about dat03_team:
rclone copy dat03_team:data/dat03/shared-inputs/jma-sources-2000-2024/79a44764037f ../dat03-jma-sources --immutable --checksum
python3 -B scripts/dat03-team-setup.py verify-inputs --owner ThanhTris --source-root ../dat03-jma-sources --approved-commit "$DAT03_RELEASE"
```

Chỉ chạy copy vào folder mới hoặc đúng immutable release cũ. Không sync/move.
Expected `JMA_INPUT_BYTES_VERIFIED_NOT_RANGE_READY`:25ZIP h2000–h2024,
128.784.513compressed bytes; exact files/SHA/ZIP CRC và code/config pins đạt.
CLI dùng verifier trong repo đã pin, không execute code tùy ý từ Drive.
Verifier nguồn không chứng nhận BronzeReady, source coverage hoặc Gold Published.
h2024 chỉ boundary cho UTC2023; không thêm vào scientific period.

Gói JMA cũ chỉ để kiểm source ZIP. Worker dùng **gói frozen37sources mới** tại
[range runbook mục2](DAT-03_RANGE_WORKER.md): bổ sung12USGS capture2023,
original metadata, summary và plan. Không fetch latest để thay pins.

## 4. Đọc dữ liệu đã xử lý ngay, không cần Docker

[Release101](DAT-03_PARTIAL_CATALOG_HANDOFF.md) và
[hai release24 + 9](../evidence/DAT-03_DRIVE_RANGES_20261028.md) có tổng134tháng.
Tải folder đầy đủ, kiểm ZIP/verifier SHA ngoài bundle, cài pyarrow21.0.0 trong
venv riêng, chạy verifier theo hướng dẫn. Chỉ sau PASS mới giải nén vào folder
mới; đọc Parquet theo schema trong manifest, không wildcard trộn bảng.
Gửi stdout thật kèm tên người nhận/pin. Không chạy ba lần trên máy HoaiTam để
thay receipts ba máy. Các gói này EXPLORATORY_ONLY, không official ML export.

## 5. Chuẩn bị Docker local — cần người dùng máy đó đồng ý

Đọc [local operations](../LOCAL_OPERATIONS_RUNBOOK.md) trước. Agent chỉ đọc
`docker version`, `docker compose version`, disk/RAM; dừng nếu daemon treo.
Sau khi chủ máy cho phép build/start, tại checkout riêng:

```bash
make env-init
# Chủ máy điền secret local theo contract; không in .env vào chat/log.
make check-config-local
make docker-build
make docker-up WAIT_TIMEOUT=600
make status
make docker-check
```

Không build ở mỗi test, không reuse credentials/MinIO/registry của HoaiTam.
Expected init exit0, services healthy, runtime freshness đạt. Ghi RAM/DockerRAM,
host physical free; reserve4GiB là mức tối thiểu, không capacity của62tháng.
Không trigger/unpause DAG, clear run, release lease hoặc prune để test setup.
Máy Windows dùng WSL2/Linux shell; chưa nghiệm thu native Windows.

## 6. Trạng thái sau bổ sung range worker

Các khối dưới thuộc **DAT-03**, chưa Done, chưa có task ID riêng:

1. **Range worker:** đã có request version mới pin owner/interval/cutoff/
   sources/runner/config; namespace/lease/checkpoint độc lập, Spark Java dùng
   lại real adapters. Unit/prepare đạt không thay một tháng runtime trên mỗi máy.
2. **Transport/import/merge (owner HoaiTam tích hợp):** portable Parquet/schema/
   source lineage/receipt pins, whitelist/SHA/fresh restore; verify duplicate,
   gap/overlap/halo liên máy và publication mới trên catalog tích hợp. Gói
   partial134tháng hiện tại không có sáu Gold tables hoặc Iceberg importer.
3. **USGS2023 pins:** đã upload original frozen bytes/metadata/summary cùng
   JMA2024 boundary; download/checksum/allowlist verifier đạt. Không thay bằng
   response mới. Exact release/SHA và commands nằm trong range runbook.
4. **Máy thành viên:** nhận released commit, chốt runtime+Drive receipt rồi
   chạy thử tháng đầu. Portable merge chưa đạt không chặn ETL local, nhưng
   không xóa local output hoặc gọi local snapshots là catalog đã tích hợp.

`dat03-team-setup.py` vẫn chỉ plan/verify-inputs. Dùng
`dat03-range-worker.py` theo runbook mới để prepare/run/resume/report.
Không còn báo range worker hoặc USGS transport là chưa có implementation;
gate còn lại là actual runtime mỗi máy và portable merge/export.

## 7. Prompt gửi agent máy thành viên

```text
Đọc AGENTS.md, docs/task/tasks/DAT-03.md và docs/runbooks/DAT-03_TEAM_START.md
ở exact full commit HoaiTam gửi. Tôi là ThanhTris (hoặc rosy179).
Thực hiện setup/check code, in plan theo owner, tải và verify nguồn pinned,
đọc thêm DAT-03_RANGE_WORKER.md để tải frozen37sources và chạy worker mới.
Xin chủ máy xác nhận OAuth và quyền build/start Docker nếu chưa có.
Không đổi ACL/public, không copy credential owner, không chạy catalog từ2000.
Chỉ sau tháng đầu accepted/own release/postflight mới resume12tháng/phiên.
Gửi report theo mục8, giữ output. Không đánh đồng setup/unit tests với runtime
hoặc local publication với portable merge/official ML export.
```

## 8. Report để nhóm tích hợp

Gửi tên tự khai, OS/Python, approved SHA/actual HEAD, config/source SHA,
plan interval/count, từng check PASS/FAIL/NOT_RUN và exit code, receipt stdout
verifier thật, Drive account tự xác nhận qua kênh riêng, runtime health/resource.
Không gửi email/token/.env/full Docker inspect/log riêng tư vào repository.
Chỉ ghi INPUT_VERIFIED khi CLI verify thật đạt; TEST_PASS không là CODE_READY
nếu sai release; RUNTIME_READY không là RANGE_READY. DAT-03 còn full288,
official two-split ML export và receipts; SEC-01 còn cloud recipient acceptance.
