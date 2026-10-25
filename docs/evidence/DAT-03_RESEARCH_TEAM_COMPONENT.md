# DAT-03 — Paired kit component,25/10/2026

Code/test cho [paired research kit](../specs/RESEARCH_TEAM_KIT.md), **chưa có
actual research ZIP hoặc receipts ba máy**. Không thay source/ML runtime gate.
Các ZIP/receipts trong unit tests là synthetic temp fixtures, không bàn giao.

## Thành phần

- Packer chỉ nhận hai official collection exports có external pins, đúng hai
  split và same Gold publication/snapshot/bundle SHA, distinct dataset IDs.
  Toàn shard semantics được kiểm, capture streaming đối chiếu original digests,
  same bytes reuse/different bytes no overwrite, không gom data vào RAM.
- Standalone verifier kiểm exact code/data set trước compile checked bytes;
  không đọc ambient modules/pyc, khôi phục caller state, không cần repository.
  Readonly input, hai splits, receipt ngoài kit cho một recipient tự khai.
- Chỉ ship6support files +indexed bundles, không source archives, credential,
  logs, warehouse, venv hoặc auto upload. Reserve physical disk trước output.

## Kiểm thử thực tế

- Paired-kit11PASS: deterministic reuse/both split validation, fresh process
  không repo/PYTHONPATH, ambient module poisoning, wrong external pin, different
  Gold pin, tampered code/extra file/symlink, no overwrite, source change sau
  validation, low disk và external/non-overwrite recipient receipt.
- Collection loader9PASS: tăng từ6 với boolean Mc reject, unknown split/missing
  month count reject và typed mainshock count. Main nullable resource_reason_code
  vẫn được giữ theo Java schema; không nới guard hoặc đổi schema để pass fixture.
- Full `make test-pilot` bằng existing doc01 venv và TMPDIR=/private/tmp đạt
  **138PASS**:16pilot +14development-team +46harness +9collection +11research-team
  +19baseline +23adaptive. Model inputs đều fixtures nhỏ, không real scientific
  fit hoặc scientific approval. Có ZMQ ResourceWarnings, make vẫn exit0;
  không coi run này là stability/benchmark acceptance.
- Lần dùng dat02 venv tối thiểu đạt loader/harness/kit nhưng dừng ở baseline
  vì thiếu NumPy; không tính full PASS. Không tự pip install hoặc đổi environment.
  Existing doc01 có đủ NumPy/SciPy/sklearn/hdbscan/PyArrow, rerun mới đạt full.
- Không chạy Spark fixture đồng thời với catalog; tests không gọi Docker/lake,
  đổi DAG/credential/lease hoặc tạo research ZIP trong shared-datasets.

## Giới hạn / tiếp theo

[DAT-03](../task/tasks/DAT-03.md) còn full288 catalog, actual two-split official
export/rerun/loader/resource acceptance, operator đóng gói từ trusted receipts
và ba máy nhận cùng release. Source profiles/checkpoint/fixture không thay gate.
[EXP-02](../task/tasks/EXP-02.md)/[EXP-03](../task/tasks/EXP-03.md) sở hữu scientific
experiments; [SEC-01](../task/tasks/SEC-01.md) cần cloud acceptance trước Colab/
Drive. Không nâng các cờ research_ready/scientific_approval hoặc ghi task Done.
