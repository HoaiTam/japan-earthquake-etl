# DAT-03 — Actual offline lineage frontier, 12/11/2026

Branch `feat/dat-03-lineage-readiness` từ main `7e1f4b0`. Owner HoaiTam;
DAT-03 In Progress. [Công cụ/runbook](../runbooks/DAT-03_LINEAGE_FRONTIER.md).
Không nghiệm thu Gold integration, official ML hoặc complete transitive closure.

## Thực hiện thật và trusted basis

Host đầu phiên còn khoảng20GiB theo df; chỉ tải3ZIP metadata và2requirements
JSON nhỏ vào ignored staging, không tải data lớn, không giải nén full trees.
Không bật Docker, gọi ETL, query/write Trino, sửa lease hoặc đổi ACL/Drive.

Requirements được tải lại từ checkpoint Drive
`data/dat03/integration-audits/20261102/1d9a7435e2eb`:

| Owner | Original filename | Expected SHA-256 ngoài archive |
|---|---|---|
| ThanhTris | ThanhTris-required-provenance.json | `33f9773fee0c1d87972cc8020459cf1b230cfa8687b04b513e40183e92f10aa6` |
| rosy179 | rosy179-required-provenance.json | `f4fdc2115fc97da409231823b54a6eb7cecffe0557cb9f893e905b9cbdb39a4c` |

Trí first-hop archive có duplicate remote filename; không lấy tên file làm
trust. Bản tải được kiểm bằng tất cả75trusted object pins, không chỉ internal
manifest/SHA. Metadata lineage ZIP của Trí có external SHA
`6324a540175502c6f549e6b8b444ccd4b4028486ea3095a75d829f9bc490874b`.
Trang ZIP có SHA
`f818ba4697d3915376420ad66cd040c542853333a31bc2b4e4a5d0b4115d859e`.
Archive inventories/SHA và root basis còn được ghi trong các request JSON.

## Kết quả actual

| Loại | ThanhTris | rosy179 |
|---|---:|---:|
| First-hop exact objects MATCHED | 75/75 | 83/83 |
| Dataset manifests tầng tiếp MATCHED | 76/76 | 0/92 trong packages cung cấp |
| Parent Gold commits MATCHED | 18/18 | 0/22 trong packages cung cấp |
| Identity references chưa có bytes | 18 | Chưa mở được từ missing commits |
| Original publication references chưa có pin/bytes | 18 | Chưa mở được từ missing commits |
| Snapshot references (không phải portable files) | 108 | Chưa mở được từ missing commits |
| Silver payload references chưa có trong metadata ZIP | 119 /522659149bytes | Chưa mở được từ missing manifests |
| Bronze raw references chưa có trong metadata ZIP | 37 | 37 |

Trí parent `commit_key` là relative SilverObjectStore. Khi resolve physical
`silver/gold-commits/`,18objects khớp trusted SHA lấy từ registry đã matched.
Không gọi root alias NOT_FOUND là mất dữ liệu. Tất cả references được dedup
theo physical key và yêu cầu checksum nhất quán.

Không có bytes payload trong metadata ZIP không chứng minh data ZIP đã bàn giao
thiếu chúng. Chưa đọc data ZIP lớn trong phiên này để đối chiếu từng payload;
inventory phải được so SHA với shared bytes trước khi yêu cầu upload thêm.
Publication URI suy theo protocol chưa có SHA độc lập: ghi unpinned, không PASS.

## Checkpoint local và artifacts cho nhóm

Actual outputs: `staging/dat03-lineage-20261112/tri-audit-v3.json` và
`rosy-audit-v2.json`; cả hai exit2 đúng nghĩa frontier incomplete.
Giữ nguyên reports/inputs chẩn đoán trước: initial combined scan dừng vì package
manifest/readme trùng tên; sau phân biệt container/object, Trí transport wrapper
`metadata/` được nhận diện. Report `tri-audit.json` roots0 là chẩn đoán layout,
**không dùng làm kết quả trusted provenance**. Không retry ETL hoặc sửa source.

- [Trí metadata request](../../config/research/dat03_lineage_ThanhTris_metadata.json):18pinned identities,18unpinned publications,108snapshot references.
- [Trí payload inventory](../../config/research/dat03_lineage_ThanhTris_payload.json):119exact parts; chưa mở bulk transfer.
- [Trang metadata request](../../config/research/dat03_lineage_rosy179_metadata.json):92manifests+22commits có expected SHA.

Chỉ metadata requests được đưa vào Git; archives, raw/Parquet/private logs
không vào Git. Không upload hoặc xóa gì trong phiên này. Chưa implement actual
portable import vì missing closure; không mở official lifecycle bằng fixture.

## Kiểm thử

Offline unit tests bao phủ root pins, parent prefix, child tamper/size mismatch,
JSON/ZIP duplicates, traversal/foreign bucket, symlink, decoded budget,
transport wrappers, conflicting object/ref hashes và snapshot non-portability.
`test-dat03-lineage` được nối vào ci-docs-base để không chỉ chạy local.
Kết quả checks cuối phiên được ghi bổ sung sau khi chạy.

### Checks cuối phiên

- `make test-dat03-lineage`:17tests đạt, gồm kiểm nhất quán3request inventories.
- `make test-dat03-provenance`:13packer+9extractor tests đạt.
- `make check-task-status`:81task/index/tracking đồng bộ.
- `make check-docs`:295Markdown files/2710links/0problems.
- `make test-docs`:10+7tests đạt.
- CI selection regressions:21impact+17suite tests đạt.
- `make check-config ENV_FILE=/dev/null`:repository/example secret hygiene đạt,
  không có `.env` local và không chứng nhận deployment/runtime.
- `git diff --check`:đạt. Staging chỉ khoảng1,6MiB ở cuối phiên; không cleanup
  vì inputs/checkpoint nhỏ còn dùng để kiểm lại.

| Local artifact | SHA-256 |
|---|---|
| tri-first-hop.zip | `3ed1afa64b688e943f6988862b494d727a2ac9d8f0594f9bd8cc361b4f802638` |
| tri-audit-v3.json | `a4d8baebbd78b21a49b455ab5d6d75fe526b47bb821da7868fa7d36a4cf158c8` |
| rosy-audit-v2.json | `e154d659a7265c2da5a32e0c2be27c90e006a9feb5a303c85dc6d6b8c56f767e` |

DAT-03 vẫn In Progress, không Done. Requests chưa upload Drive và code/docs
chưa commit/push/merge. Hai report hiện hành giữ full_transitive_closure_verified,
portable_import_ready và official_export_ready đều false.
