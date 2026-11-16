# COL-02 — local fresh kernel → Colab CPU → independent readback

Chạy sweep đã khóa trước trên pilot JMA thật: 279 candidate rows, 2 mainshocks,
4 DBSCAN + 4 HDBSCAN_GLOBAL và 1 Window control. Không phải tìm model tốt nhất,
không thay feature/scaling/window hay loại cửa sổ failed/noise. Không cần Docker,
Java, Airflow, MinIO, `.env` hoặc mount Drive.

[Acceptance ngày 05/11/2026](../evidence/COL-02_COLAB_20261104.md) và
[metadata/pins](../evidence/COL-02_COLAB_ACCEPTANCE_20261105.json) ghi actual
cloud + independent readback đã đạt. Hướng dẫn dưới đây dùng cho lần chạy mới,
không tái sử dụng trạng thái PASSED của lần cũ cho source/input khác.

## 1. Setup và input

Dùng Python 3.11–3.13, venv riêng và exact direct dependencies:

```bash
python3 -m venv .venv-colab
.venv-colab/bin/python -m pip install -r notebooks/requirements-colab-lock.txt
.venv-colab/bin/python -m pip check
make test-col02 PILOT_PYTHON=.venv-colab/bin/python
```

Windows dùng `.venv-colab/Scripts/python.exe`; không có Make thì chạy hai
pattern `test_parameter_sweep.py` và `test_col02_colab.py` bằng
`-m unittest discover -s tests -p PATTERN`. Không cần quyền admin.

Tải ba ZIP giữ nguyên bytes từ release nhóm:

- [Pilot JMA](https://drive.google.com/file/d/12eLvQJzEkBuT0WYaxHYvb9KZlk5MK_vb/view),
  SHA `f58291b72873e1b26791fef97818fdcf5e2b028a5dfa528885a666d8cdc1b870`.
- [EXP-02 default baseline handoff](https://drive.google.com/file/d/1taAG1gUWhzoYhQKSU7clsI3uK2s1c3vE/view),
  SHA `d6be707111be8d2b1a2ba64434d6c0cacaa67b73c5aae3235f9772403c544333`.
- [EXP-02 actual Colab output](https://drive.google.com/file/d/1YV-A0FeNtcdv2t_zCR507PIpQsW05bog/view),
  SHA `df5dae61e79cb707e8136af2b46037b27b5818f0abc89bedb59dab218d4b8665`.

Hai ZIP EXP-02 là gate đã nghiệm thu qua PR #114, không dùng làm sweep input.
Packager kiểm trusted SHA, readback ba default baselines và đối chiếu các
shared model/config/lock bytes chưa thay đổi. Nếu thay shared math hoặc defaults,
nghiệm thu EXP-02 lại; không sửa pin để bỏ qua gate.

## 2. Local gate và handoff mới

Thay các absolute path bằng file thật trên máy; recipient là người chạy thật:

```bash
.venv-colab/bin/python -B scripts/col02-colab.py local-pack --pilot-zip /absolute/pilot.zip --baseline-handoff /absolute/exp01-colab-handoff.zip --baseline-output /absolute/exp01-colab-output.zip --output staging/col02-local-NEW --recipient HoaiTam
```

Expected `LOCAL_COL02_HANDOFF_VERIFIED`, fresh `nbclient` kernel, đủ 9 configs,
rerun DBSCAN_C1 sang output/run ID mới, input hashes không đổi. Không có
synthetic fallback hoặc `allow_overwrite`. Output đã tồn tại phải chọn tên mới;
giữ nguyên evidence lần lỗi. Kernel cần cổng loopback, không cần expose network.

`handoff-receipt.json` chứa external ZIP SHA, source/config SHA và inspection.
`col02-colab-handoff.zip` chỉ đóng source allowlist, pilot ZIP, gate/report và
10 × 7 result files (9 configs + 1 rerun). Matrix chia sẻ dùng đường dẫn tương
đối; matrix nguyên bản giữ local. Source content hash là pin thay đổi chưa commit,
không giả `git_base_commit` là commit chứa source mới. Không sửa allowlist EXP-01.

## 3. Colab thật

1. Mở notebook sạch `notebooks/col_02_colab_acceptance.ipynb` trong **session
   CPU mới**, upload đúng handoff ZIP của bước 2 qua Files. Không dùng session
   đang chạy công việc khác hoặc notebook EXP-01 cũ.
2. Run all một lần. Nhập absolute ZIP path trên VM, external SHA từ receipt
   local, và GitHub name người vận hành thật. Tên này self-reported, không
   chứng minh danh tính/receipt ba máy. Assignee task vẫn là rosy179.
3. Cell 1 kiểm SHA trước thực thi source, bounded ZIP/path/file set/hashes.
   Cell 2 tạo venv cô lập, host pip `--python` cài exact pins vào venv, không
   thay host packages; lưu actual installed packages/Python/lock hashes.
4. Cell 3 kiểm local gate rồi dùng **cùng** shared `parameter_sweep`/baseline
   API; probe base interpreter phải thật sự có `google.colab` trên Linux dưới
   `/content`. Không cài package giả hoặc gán cờ runtime để nhận cloud acceptance.
5. Cell 4 tạo ZIP allowlist mới, readback từng member, in SHA và tải về.
   Tải thêm notebook `.ipynb` **giữ output/execution counts** để audit Run all.

Expected `PINNED_COL02_HANDOFF_BYTES_VERIFIED`, `ISOLATED_COL02_CPU_READY`,
`COLAB_COL02_SWEEP_AND_PARITY_PASSED`, `OUTPUT_ZIP_READBACK_VERIFIED` và SHA.
Chỉ upload/verify ZIP chưa là cloud acceptance. Khi lỗi giữ traceback/output,
sửa ở repo, local-pack lại với source/ZIP pin mới rồi chạy session mới.

## 4. Đọc lại độc lập trên máy

Đối chiếu SHA output tải về với SHA in từ Colab/notebook thật. Giải nén bằng
bounded extractor (module `colab_runtime.extract`) vào thư mục mới, không vào
handoff/kit. Ví dụ chạy từ root repo, thay ba argument bằng actual path/SHA:

```bash
.venv-colab/bin/python -B -c 'import sys; from pathlib import Path; sys.path.insert(0,"notebooks"); import colab_runtime as c; c.extract(Path(sys.argv[1]),Path(sys.argv[2]),sys.argv[3])' /absolute/col02-colab-output.zip staging/col02-cloud-output-NEW EXTERNAL_SHA256_FROM_COLAB
```

Sau đó:

```bash
.venv-colab/bin/python -B scripts/col02-colab.py readback --handoff-root staging/col02-local-NEW/handoff --output /absolute/output-extracted-NEW
```

Expected `INDEPENDENT_READBACK_PASSED`, 75 files, 10 parity results PASSED.
Verifier kiểm exact file set/checksums/schema, model/config/code/seed pins,
279 memberships/2 summaries mỗi run, 10 run IDs riêng, matrix đối chiếu metrics,
đủ failed windows và DBSCAN_C1 rerun. Parity so grain/roles/noise/memberships,
cluster partitions không phụ thuộc label số; float abs1e-8/rel1e-6. Không so
run IDs/timestamps/runtime byte-identical. Giữ null memory khi chưa đo.

Sau actual notebook + independent readback đạt mới cập nhật COL-02 Done,
evidence/pins/operator và index. Repository notebooks phải sạch outputs/counts;
actual cloud notebook/output ZIP giữ ngoài Git. Không commit pilot/Parquet/venv.

Nếu nút download Colab không trả file: giữ runtime/artifacts, không rerun fit
hoặc gọi lại cell export vào output đã tồn tại. Lần nghiệm thu 05/11 đã tải
original notebook trước (4 code cells/counts 1–4/AST parity), rồi thêm một cell
chỉ đọc ZIP có sẵn, assert external SHA/size và xuất HTML link nhúng bytes.
Tải notebook có output qua Drive file ID bằng kết nối **đã được cấp quyền**,
decode đúng một ZIP link, kiểm external SHA/size lại rồi dùng cùng extractor/
readback. Không cài credential vào Colab, mount Drive, thay ACL hoặc dựng ZIP
từ local reference. Giữ cả original notebook và transfer notebook để audit;
cell transfer không được dùng làm fresh-kernel/model acceptance.

## Ranh giới

- DAT-03: pilot không thay full research catalog/export hoặc receipt ba máy.
- EXP-02/EXP-03/EXP-04: không chứng nhận official research, adaptive cloud hoặc
  scientific quality/best parameter selection từ pilot này.
- COL-03: evaluation/Omori/DBCV không thuộc sweep portability acceptance.
- SEC-01: không mount Drive/đổi ACL; vẫn cần actual recipient/security audit.
- MLI-03/MLI-04: không import development artifacts vào official `ml.*`.

Mọi report giữ PARTIAL/DEVELOPMENT_ONLY, `research_ready=false`,
`official_exported=false`, `scientific_approval=false`; không sinh `_SUCCESS.json`.
Direct versions đã pin, chưa khóa toàn bộ transitive wheels/hashes. Ghi actual
versions và tolerance; không hứa bitwise reproducibility trên mọi VM.

## CPU-dispatch compatibility policy (04/11)

Actual A/B trên cùng Colab/pilot/config phát hiện HDBSCAN_C2 có 25 probability
khác Mac khi dùng NumPy dispatch mặc định, dù grain/roles/counts giữ nguyên.
Đổi nearest-neighbor backend sang brute không khắc phục. Chỉ tắt nhóm dispatch
x86 trong Python subprocess đã khôi phục parity; không sửa feature/math,
hyperparameters, package pins hoặc tolerance để chấp nhận sai khác lớn.

`col02_portability.py` khóa policy `col02-numpy222-portable-cpu-v1`:

- x86_64/AMD64: đặt `NPY_DISABLE_CPU_FEATURES` thành
  `AVX2,AVX512F,AVX512_SKX,AVX512_CLX,AVX512_CNL,AVX512_ICL` trước import NumPy;
  kiểm actual flags đều false trước fit, không tin biến env được đặt muộn.
- ARM64/AArch64: không đặt các x86 flags; ghi architecture thực tế. Không
  ghi đè foreign CPU policy, không hỗ trợ âm thầm architecture/NumPy version khác.
- Chỉ ảnh hưởng process COL-02/child kernel, không chỉnh host Colab, shell,
  các experiment khác hoặc NumPy wheel. Actual policy đi vào sweep report và
  readback gate; source hash bao phủ module policy. Process/library phải khởi
  tạo sạch qua CLI local-pack trước khi cloud. Không set env trong kernel đã
  import NumPy rồi coi như đã tắt dispatch.

Tham khảo [NumPy CPU runtime dispatch](https://numpy.org/doc/2.2/reference/simd/build-options.html#runtime-dispatch),
[quicksort/tie-order policy](https://numpy.org/doc/2.2/reference/generated/numpy.argsort.html),
và [HDBSCAN 1.6.1 implementation](https://github.com/scikit-learn/scikit-learn/blob/1.6.1/sklearn/cluster/_hdbscan/hdbscan.py).
Đây là compatibility policy từ phép kiểm soát CPU-dispatch thực tế, không
tuyên bố đã chứng minh mọi chi tiết nguyên nhân bên trong thư viện hoặc độ
chính xác khoa học của probability. Colab phải chạy lại cả sweep với source pin
mới rồi tải về đối soát thật; probe A/B riêng không đủ chuyển task Done.
