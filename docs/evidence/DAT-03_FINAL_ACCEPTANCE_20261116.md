# DAT-03 — Đối chiếu nghiệm thu cuối, 16/11/2026

**Done**, assignee HoaiTam, reviewer unassigned. Continuation PR #131 trên
branch `feat/dat-03-official-collections`; không tạo approval hoặc reviewer giả.
[Machine-readable evidence](DAT-03_FINAL_ACCEPTANCE_20261116.json) pin SHA của
các bằng chứng gốc và receipt. Những evidence trước ghi In Progress/thiếu
receipt là snapshot lịch sử, không bị ghi đè hoặc đổi failure thành success.

## Đối chiếu acceptance

| Tiêu chí DAT-03 | Bằng chứng thực tế | Kết quả |
|---|---|---|
| Pilot dùng trước, source/profile25archives, boundary/limits | [Profile acceptance](DAT-03_RESEARCH_PROFILE_ACCEPTANCE.md), [bounded E2E](DAT-03_COLLECTION_BOUNDED_E2E.md) | Đạt; pilot không thay research |
| Capacity/ETA và rollout được duyệt | [Capacity/provenance](DAT-03_CAPACITY_PROVENANCE_20261115.md), [profile90k + actual exports](DAT-03_OFFICIAL_COLLECTIONS_20261115.md) | Đạt; guard/resource có version, không sampling |
| Catalog/source coverage thật, controller/checkpoint | [Portable import](DAT-03_PORTABLE_IMPORT_20261115.md) | 288tháng/27batch,225+63,0gap/overlap; source pins/cutoff giữ nguyên |
| Global/boundary/revision/source-link/reconciliation | Portable import và accepted reuse | balanced=true;0exact duplicates;4.489.823bridge−4.488.650canonical=1.173accepted links |
| Six-table Gold commit/verify/publish/rerun | [Gold acceptance](DAT-03_PORTABLE_GOLD_20261115.md) | Published;139+41SQL lượt đầu,139+15rerun; six pins/SHA bất biến |
| Official ML audit/Mc/strict windows/lifecycle/registry | [Official collections](DAT-03_OFFICIAL_COLLECTIONS_20261115.md) | Hai EXPORTED; Mc0,5/0,4;6/3temporal exclusions; không clip/cross-split |
| Loader/rerun/release/readback thật | Official collections và [kitv4](DAT-03_WINDOWS_HANDOFF_20261116.md) | 390.374memberships/239mainshocks/6shards;311SQL +13readbacks độc lập;20collection files không đổi |
| Bàn giao cùng release trên ba máy | Ba receipts đối chiếu trực tiếp, bảng dưới | TEAM_RESEARCH_VERIFIED/all_shards_verified=true, IDs/SHA/Gold pins/counts khớp |
| Docs/task/index/giới hạn | Task, index, progress và runbook nhận dataset | Đồng bộ Done; không tự đổi science hoặc SEC-01 |

Hard dependencies ORC-07, MLD-05, MLI-02 và EXP-01 đều Done theo task component.
Không buộc DAT-03 chờ EXP-02/03: scientific experiments và capacity model fit
là bước sau, không là deliverable dataset handoff. Không biến PREPARED thành
Published/EXPORTED bằng metadata; từng bước có actual acceptance riêng.

## Ba receipt thật, cùng kit v4

ZIP SHA256 `687170587e9db1d5d767722bf2fd4e1e2eecc1c3141ebfcdb10269a8b83b4c21`;
handoff SHA256 `6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297`.
Remote ZIP size/SHA và exact4file set được kiểm lại16/11, không upload/đổi ACL.

| Người nhận | Receipt | Python | SHA256 receipt |
|---|---|---|---|
| HoaiTam | Fresh Drive restore/shipped verifier, bản local retained | 3.13.3 | `7c840027e2c886ba62131a450654b3b9139b4abead018d5c0be8cc3bcb7119c5` |
| ThanhTris | [JSON trên Drive](https://drive.google.com/file/d/1R14b-naac-whWzyolKjSnEwi5Yd2Q_uH/view) | 3.12.10 | `73e1b66a2be094e0fcea0d8d5893ffa1d579b7519e7a29dba7a038151736a676` |
| rosy179 | [JSON trên Drive](https://drive.google.com/file/d/179iR5legh-4gdZAt6eXNjBb8c3nfKH4-/view) | 3.11.0 | `0b7ee0cb7b885580800e8261f16b7ddb3e8be11de416c8b62368f434468c7433` |

Hai receipt tải trực tiếp bằng read-only Drive; SHA content khớp remote metadata.
Schema/recipient/version/handoff và toàn bộ hai collection dict khớp receipt
owner: REPRODUCTION156710/194/2, EXTENSION233664/45/4. Ba fingerprint khác nhau,
nhưng là máy tự báo, **không phải hardware attestation**. Không tạo hoặc sửa
receipt của thành viên. Source evidence JSON, failed attempts và kitv2/v3 giữ nguyên.

## Kiểm tra trong phiên chốt

- So lại SHA request/execution/candidate/report/acceptance/catalog của hai
  phiên import accepted trong retained host capture; catalog nối đúng Gold evidence.
- Đối chiếu 4accepted ML build/rerun sessions, quality PASSED/invalid0, trusted
  handoff, own release và six Gold pin giữ nguyên; independent postflight13SQL đã đạt.
- [CI của head922e8d2](https://github.com/HoaiTam/japan-earthquake-etl/pull/131/checks):
  23/23checks SUCCESS, gồm Windows/Linux portable loaders. Không lấy CI thay
  receipts real-data hoặc coi docs chưa push là đã được CI kiểm tra.
- Không chạy ETL/SQL mới, không bật/sửa Docker, cron, namespace, lease hoặc ACL.
  Đây là nghiệm thu retained acceptance + actual receipts, không phải một lần
  live Trino re-audit mới. Các kiểm tra docs/local sau sửa được ghi ở bàn giao.
- Sau cập nhật docs:36loader/kit/interop +43CI policy tests PASS;317Markdown/
  2.906links/0problems;81task metadata/tracking/index nhất quán, whitespace và
  CFG-01 repository/example secret hygiene PASS. Checkout không có `.env` local,
  không gọi CFG check là deployed SEC-01 audit. Explicit8changed/new docs secret
  pattern scan được chạy riêng; không commit receipt raw/fingerprint hoặc dataset.

## Giới hạn / bước tiếp theo

- **SEC-01 — Review secret và bề mặt truy cập:** còn identity/cloud acceptance
  theo [task SEC-01](../task/tasks/SEC-01.md). DAT-03 Done không tự chốt security.
- **EXP-02 — Chạy Window, DBSCAN và HDBSCAN global** và **EXP-03 — Xây HDBSCAN
  adaptive và giải quyết multi-sequence:** official fits/evaluation/capacity còn
  theo [EXP-02](../task/tasks/EXP-02.md) và [EXP-03](../task/tasks/EXP-03.md).
  Không cấp scientific approval, không tuyên bố dự đoán động đất hoặc ghi nhãn vào Gold.
- Reviewer vẫn unassigned; không tự commit/push/merge trong phiên cập nhật trạng thái.
