# DAT-03 — Windows kit patch, 16/11/2026

Continuation PR #131, branch `feat/dat-03-official-collections`, base commit
`55ff906e464978e6bb63a8172edf2cfa62a26e92`. Owner HoaiTam/reviewer unassigned,
task In Progress. [Comment gốc ThanhTris](https://github.com/HoaiTam/japan-earthquake-etl/pull/131#issuecomment-6897510175)
xác nhận ZIPv2 đúng SHA, nhưng Windows11/Python3.12.10/PyArrow21 chưa xuất được
receipt. Không đổi nhận xét này thành native Windows PASS từ tests trên macOS.

## Hai nguyên nhân và bản vá

1. Directory index dùng str(Path.parent) trên Windows trả backslash; path đang
   kiểm lại dùng as_posix(), gây RESEARCH_TEAM_EXTRA_DIRECTORY. Dùng
   PurePosixPath/as_posix cho archive metadata trên mọi OS; guard exact file set,
   symlink, traversal, canonical paths và checksums vẫn giữ.
2. pc.assume_timezone(UTC) yêu cầu C++ IANA database của PyArrow21 trên Windows.
   Thay bằng integer ns → exact microseconds → timestamp(us,UTC), chỉ sau exact
   frozen-writer/physical INT96/alignment guard. Không floating-point, timezone
   guessing, download tzdata hoặc dependency mới. Kiểm integer epoch roundtrip
   và created_at UTC anchor vẫn bắt buộc; unknown writer, submicrosecond, null
   hoặc shifted anchor vẫn fail. Đóng ParquetFile bằng context manager để không
   giữ file handle khi lỗi/cleanup trên Windows.

Đây là reader/verifier fix, không thay science/Mc/window/vector/guards, tạo
features bằng Python, build JAR hoặc replay ETL. Không cần bật Docker hoặc sửa
source lease/Gold/ML namespace. Original v2 và evidence15/11 giữ nguyên.

## Kiểm chứng đã thực hiện

- 36 offline loader/paired-kit/interop tests PASS, gồm Windows path semantics,
  unavailable assume_timezone, pre-epoch/zero/large epoch và precision/anchor faults.
- 42 CI selection/workflow/gate tests PASS; kiểm cả dependency/gate/matrix
  không được bỏ Windows, skip required step hoặc suppress failure.
- All-shard read-only trên real REPRODUCTION/EXTENSION với assume_timezone bị
  stub thành lỗi:6/6shards,390.374features/239mainshocks PASS,0timezone lookup calls.
- V3 pack + rerun same ZIP/handoff SHA; fresh isolated shipped verifier PASS
  trên máy HoaiTam.20/20collection files byte-identical so ZIPv2; dataset IDs,
  Gold pins, Mc, portable manifests/checksums không đổi.
- CI thêm Python3.12/PyArrow21 trên Windows native +Linux khi pilot impact !=none;
  docs-contracts required gate cần cả matrix đạt. Native CI fixture acceptance
  và actual real-data receipt của ThanhTris/rosy179 là hai phạm vi khác nhau.

## Release v3

- ZIP: `jma-research-team-20261115-v3.zip`,37.524.111bytes,27files.
- ZIP SHA256: `2759f735590451975d286ef147b03e8c883e2b23fae073a94adfada72e926cc5`.
- handoff SHA256: `f6b5b0e9694526c55fb0a82fc0283503c55584ea2ee2fa2f3fa4eed7210e1875`.
- Exact pins/receipt/code hashes: [machine-readable evidence](DAT-03_WINDOWS_HANDOFF_20261116.json).
- [Hướng dẫn setup/tải bản hiện hành](../runbooks/DAT-03_OFFICIAL_RESEARCH_HANDOFF.md).

## Giới hạn / bước tiếp theo

**DAT-03 — Chuẩn bị catalog nghiên cứu thật và bàn giao dataset dùng chung**
vẫn chờ actual v3 verify trên máy riêng ThanhTris/rosy179. Không giả receipt,
scientific approval hoặc suy macOS/PureWindowsPath tests thành native acceptance.
**SEC-01 — Review secret và bề mặt truy cập** vẫn có identity/recipient/cloud gate
riêng; release không đổi ACL. **EXP-02/EXP-03** sở hữu official model experiments,
không fit hoặc ghi kết quả vào Gold trong phiên này.
