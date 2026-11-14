# DAT-03 — Actual payload reuse inventory, 13/11/2026

> Mốc lịch sử13/11. [Audit14/11](DAT-03_BYTE_AVAILABILITY_20261114.md) đã tìm
> được content595bytes, kiểm kê119/119Trí/144/144Trang và xác nhận114metadata
> Trang. Không sửa kết quả/report13/11 thành một lần nghiệm thu khác.

Owner HoaiTam; branch `feat/dat-03-payload-reuse-inventory` từ main
`df2fb01418f81e0bfaf54c8306e7cce11d759a8d` (#128). Task **In Progress**.
[Runbook](../runbooks/DAT-03_PAYLOAD_REUSE.md),
[machine-readable summary](DAT-03_PAYLOAD_REUSE_20261113.json).

## Input và thực hiện thật

Host đầu phiên khoảng19GiB trống theo df, đủ reserve4GiB. Fresh download data/
proof/frontier ZIP của Trí và checkpoint JSON nhỏ vào ignored staging. Không
extract trees, Docker/ETL/Trino, Drive/ACL writes hoặc cleanup.

Data/proof pins lấy từ retained checkpoint độc lập
`data/dat03/integration-audits/20261102/1d9a7435e2eb/ThanhTris-verification.json`,
khớp worker checksum list. Fresh checkpoint JSON SHA
`5b97313953ae1ccaa8a10809dbeb8b98337953a9d3fb63e3979e6f5a06f1b55d`.

| Artifact | Bytes | External SHA-256 |
|---|---:|---|
| ThanhTris-dat03-62months-data.zip | 229190896 | `c072e14f4aa2964af3f9a641e0973bdff6b4dbdc6084d820df79a18707d3b231` |
| ThanhTris-dat03-62months-proofs.zip | 1009356 | `b88c65f5a14a7f2ebe4c1c5483d5c6575bae28c73e4ac6cdc7b7bf83fe614b68` |
| ThanhTris-dat03-frontier-metadata-20261112.zip | 149712 | `cb6cee8e1fd3315f35548bcd21f40f40d5392a33b533bdbce20207e6e2342415` |

## Metadata readback sau #128

Combined audit75/75roots,76dataset manifests,18parent commits và18identities
MATCHED. Probe18/18payload SHA identity khớp declared và expected pins.
Strict generator với exact request/audit đạt18/18publication backreferences/
six-table pointers, giữ **PUBLICATION_NOT_VERIFIED**. Không chứng nhận publication
bytes authenticated hoặc full closure.

Actual outputs trong `staging/dat03-payload-20261113/`:

| Output | SHA-256 |
|---|---|
| tri-combined-audit.json | `80caa2624c8d30855b825e0394387de8aa65931aeb798e66a50520a7f571c8a6` |
| tri-probe.json | `b31d9952b5dd8615ac5b90a6db63f75ee4e986c954cacc36b295278e1adda535` |
| tri-payload-inventory-with-proofs.json | `d76c87d993adf2a8ffb0bf977e06d7e0ec8ae0df84a7635dcf9887cc9d54adcd` |

## Kết quả payload

[Trusted request119refs](../../config/research/dat03_lineage_ThanhTris_payload.json)
có SHA `0643f60280fa381f610b69c1f1e0c17fcc899521818363d32392b4883bdd1e25`.

| Chỉ số | Actual |
|---|---:|
| Expected URI references / reference bytes | 119 / 522659149 |
| Expected unique SHA/size contents | 83 |
| Byte-identical references trong data ZIP | **100** |
| Bytes đã hash trong100Parquet members | **522647844** |
| References chưa có trong data/proof ZIP | **19** |
| Missing reference bytes | 11305 |
| Missing unique content / bytes | **1 / 595** |

Data ZIP400entries/100Parquet, proof ZIP222entries/0Parquet. Raw ZIP hashes
khớp external pins trước scan, stream payload1MiB/chunk, không giải nén.
Inventory exit2 đúng nghĩa partial; nguồn cùng SHA/size ở tên transport khác
được giữ mapping để tái dùng, không tự ghi vào original S3keys.

19references còn lại đều là reject_record/part-00000.parquet, cùng SHA
`d07bbf8dbdcf824135f51d69e79418a4f5f35f37ed87f9ad7eac5396cab668c7`/595bytes.
[Delta request](../../config/research/dat03_lineage_ThanhTris_payload_delta.json)
giữ đủ19URIs/SHA/size; cần original content, không regenerate hoặc gửi lại498MiB.
Đây là tiết kiệm **transfer bổ sung**, không tuyên bố đã giải phóng498MiB disk.

## Blockers và bước tiếp theo

Fresh list13/11 tại data/dat03/workers/rosy179 vẫn có first-hop ZIP và gói
138phase journals401.335bytes/checksum spec; chưa thấy gói bytes gốc92dataset
manifests+22parent commits hoặc frontier114spec trong worker folder.
Không kết luận mất objects trên MinIO; cần đúng gói và Drive link/ID/SHA.

**DAT-03** còn content595bytes của Trí,114frontier objects của Trang, raw-source
closure và trusted publication proofs/complete Iceberg graph. Sau đó mới actual
portable integration/boundary/reconciliation/Trino, hai official exports và
receipts3máy. Không giả receipts/approval hoặc chuyển task Done.

## Kiểm thử

- `make test-dat03-payload-inventory`:17tests đạt, gồm kiểm delta request giữ
  nguyên URI/SHA/size của original inventory, unique content budget595bytes.
- `make test-dat03-lineage`:45tests đạt.
- `make test-dat03-provenance`:13packer+9extractor tests đạt.
- `make check-docs`:298Markdown files/2731links/0problems.
- `make check-task-status`:81task/index/tracking đồng bộ.
- `make test-docs`:10+7tests đạt.
- CI impact/suite regressions:21+17tests đạt (đọc `tests/ci/`, không Docker).
- Actual inventory rerun ra output mới `tri-payload-repro.json`:exit2 đúng
  partial, report SHA vẫn `d76c87d993adf2a8ffb0bf977e06d7e0ec8ae0df84a7635dcf9887cc9d54adcd`.
- `make check-config ENV_FILE=/dev/null`:repository/example secret hygiene đạt,
  không có `.env` local, không chứng nhận runtime credentials hoặc cloud ACL.
- `git diff --check`:đạt; không stage/commit/push/merge hoặc đưa raw data vào Git.

Fixtures nhỏ offline bao phủ reuse/rename, missing/tamper, exact-key conflict,
duplicate JSON/member/URI, symlinks/special entries, byte budgets và immutable
output. Actual inventory và metadata readback không thay Parquet row semantics,
source-link/boundary hoặc Trino acceptance. Inputs nhỏ/ZIP giữ ignored staging
để kiểm lại; không xóa source pins, warehouse hoặc evidence.
