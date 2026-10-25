# DAT-03 — Paired research team kit v1

Version `dat03-team-research-v1`, artifact_kind=PAIRED_COLLECTION_EXPORT.
Mục tiêu: ba người nhận **một ZIP byte-identical**, gồm hai collection exports
REPRODUCTION225tháng và EXTENSION63tháng từ cùng exact Published Gold pin.
Không đổi dataset/collection IDs, schema, scaling, global Mc hoặc shard layout.

## Gate của operator trước đóng gói thật

DAT-03 phải nghiệm thu full288 catalog, cả hai split có actual MLI-02
DATASET_EXPORTED/final independent handoff readback, quality/counts/pins,
all-shard loader và rerun. Packer **không** thay gate source coverage/Trino
hoặc chứng minh nguồn từ một JSON tự khai. Expected hashes phải lấy từ hai
actual trusted export receipts, giữ ngoài ZIP; không dùng mock/pilot thay thế.

| Field trong export receipt | File/pin dùng |
|---|---|
|bundle_manifest_sha256 |Portable dataset_manifest.json, truyền manifest-sha256 |
|checksums_sha256 |Portable checksums.sha256, truyền checksums-sha256 |
|dataset_manifest_sha256 |Trusted registry ds_*.json; **không** dùng thay portable manifest pin |

## Layout, bounds và capture

    README.md
    requirements.txt
    verify_dataset.py
    tools/research_collection.py
    tools/experiment_harness.py
    tools/pilot_bundle.py
    bundles/REPRODUCTION/{dataset_manifest.json,collection-preparation.json,...}
    bundles/REPRODUCTION/shard_00000/{features.parquet,mainshocks.parquet}
    bundles/EXTENSION/...
    handoff.json

File set chỉ gồm6support files và exact indexed files của hai bundles.
Tối đa64shards/split,64MiB/file,8GiB tổng payload,270payload files +handoff.
Handoff <=512KiB, pin size/SHA của mọi file và summaries hai split; dataset/
collection IDs phải khác nhau nhưng same physical Gold table/publication/
bundle SHA/snapshot. Snapshot ID giữ integer precision qua string.

Packer kiểm cả hai external pins và toàn shard semantics trước capture. ZIP
stream ZIP_STORED (không gom payload vào RAM), timestamp/perms cố định; mỗi
file được hash trong lúc copy và phải khớp original verified digest. Source
đổi sau validation không được rehash thành bộ hợp lệ. Chỉ publish output sau
capture đầy đủ; existing same bytes reuse, different bytes fail, không overwrite.
Không sửa/xóa input, không upload hoặc ghi lake/DAG/lease.

Host cần reserve4GiB, cộng hai lần total source bytes cho temp ZIP/final copy;
thiếu disk dừng trước output. Private logs, source archives, registry credentials,
warehouse, notebook outputs và venv không có trong allowlist. ZIP/code hash không
thay xác thực người gửi; không gọi checksum là chữ ký JMA/USGS.

## Verify và receipts

Shipped verifier nhận expected handoff SHA bên ngoài, kiểm exact files/size/
hash/symlink trước chạy code. Ba modules được rehash rồi compile đúng bytes
đã kiểm; không dùng ambient sys.modules hoặc timestamp pyc. Caller module/path
state được khôi phục sau verify/failure. Đọc lần lượt all shards của cả hai
splits, không concat toàn catalog. Không cần repo/Docker/Java/MinIO.

Receipt ở ngoài kit có recipient, verified_at, machine fingerprint hash,
Python version, two dataset/collection IDs/pins/counts và all_shards_verified.
Recipient tự khai, hash hostname không là chứng thực danh tính/máy. Owner cần
hai thành viên thực sự tự chạy và gửi receipt, không tạo hộ trên máy owner.
Packer không tự tạo receipt cho bất kỳ thành viên nào.

Scientific approval luôn false; không tự mở model fit/import/Colab gates từ
việc có ZIP. [EXP-02](../task/tasks/EXP-02.md)/[EXP-03](../task/tasks/EXP-03.md)
còn scientific run/evaluation; Colab/Drive cần mở lại
[SEC-01](../task/tasks/SEC-01.md) cloud trước migration. Kit là input local
readonly, output/experiment_run_id/venv/receipt riêng từng người.

## Commands và kiểm thử

`make test-research-team PILOT_PYTHON=python3` cần pyarrow đã cài. Kiểm thử dùng
fixtures nhỏ trong temp, không được công bố như research release.
Sau actual export acceptance, `make pack-research-team` yêu cầu các biến
RESEARCH_TEAM_REPRODUCTION/EXTENSION (exact directories), bốn biến SHA tương
ứng và optional RESEARCH_TEAM_ZIP. Không có default source hoặc tự tìm latest.
Xem `python3 scripts/pack-research-team.py --help` cho CLI tương đương.

[README shipped](../runbooks/DAT-03_RESEARCH_TEAM_README.md) hướng dẫn người
nhận. Operator tự bàn giao ZIP +zip SHA+handoff SHA qua kênh tin cậy; chưa có
actual research ZIP/three-machine receipts chỉ từ việc code/test đã đạt.
