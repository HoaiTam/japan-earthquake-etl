# COL-01 — Khảo sát pilot thật, local và Colab

Không ETL, không fit/tune, không Docker hoặc credential pipeline. Dùng pilot
đã verify từ [Drive handoff](./DAT-03_DRIVE_DATA_HANDOFF.md), không dùng ZIP
catalog/source làm feature bundle. [Task](../task/tasks/COL-01.md) yêu cầu
nghiệm thu cloud thật trước Done; local PASS không là bằng chứng Colab.

Nghiệm thu ngày 02/11/2026 đã đạt trên fresh Colab session; xem
[evidence và kết quả readback](../evidence/COL-01_PILOT_EDA_20261102.md).

## Chuẩn bị local

Dùng Python 3.11–3.13 và venv đã cài thư viện COL-01. Ví dụ chạy một lần:

```bash
python3 -m venv staging/venvs/col01
staging/venvs/col01/bin/python -m pip install -r notebooks/requirements-col01-lock.txt
make test-col01 PILOT_PYTHON=staging/venvs/col01/bin/python
```

CLI yêu cầu đường dẫn `bundle/` gồm exact năm file DAT-02. Trong ví dụ sau,
đặt pilot kit đã tải/verify ở `staging/ml-pilot/pilot-kit/`. Output phải mới,
nằm ngoài bundle; không ghi đè kết quả cũ để rerun.

```bash
staging/venvs/col01/bin/python scripts/col01-eda.py run --bundle staging/ml-pilot/pilot-kit/bundle --output staging/col01-local-run1 --code-version "$(git rev-parse HEAD)"
staging/venvs/col01/bin/python scripts/col01-eda.py pack --bundle staging/ml-pilot/pilot-kit/bundle --output staging/col01-handoff.zip --code-version "$(git rev-parse HEAD)"
```

Code commit là base; SHA runner/module ghi riêng để nhận diện cả thay đổi chưa
commit. Handoff in `zip_sha256`; giữ SHA ở nơi tin cậy ngoài ZIP. Không tự sửa
pin nếu input không khớp. Kit có năm file; output đặt ngoài cả kit, không thêm
receipt/log vào bundle. Archive chỉ chứa bốn code/lock files, năm input files
và `handoff.json`; không chứa Git, env, account path, credential hoặc raw logs.

## Colab CPU mới

1. Upload `notebooks/col_01_pilot_eda.ipynb` bằng File → Upload notebook.
2. Kết nối runtime CPU, Files → Upload `col01-handoff.zip` đã kiểm local.
3. Run all; nhập path thường là `/content/col01-handoff.zip` và exact ZIP SHA
   từ local receipt. Notebook verify SHA/exact allowlist trước thực thi code.
4. Venv riêng cài ba direct pins, `pip check`; notebook import `google.colab`
   rồi chạy chính CLI local. Không coi `runtime_kind` trong JSON tự khai là
   proof cloud: giữ notebook/session và ZIP output từ session thật.
5. Expected: `COL01_HANDOFF_VERIFIED`, `COL01_ENV_READY`,
   `COL01_COLAB_PASSED 279 279`, hiển thị PNG, và `COL01_OUTPUT_ZIP_VERIFIED`.
   Tải `col01-output.zip`; readback exact bốn files và SHA trong checksum JSON.

Không restart toàn máy/stack khi lỗi notebook. Sai path/SHA: dừng, kiểm lại
file đã upload. Hết runtime: session mới, không gắn receipt local thành cloud.
Nếu bản cũ lỗi `matplotlib_inline` khi import Matplotlib: lấy code COL-01 mới
và pack ZIP mới; module phải đặt Agg **trước** import vì backend host không có
trong venv riêng. Không bỏ pin hoặc cài tùy tiện vào host để vượt lỗi.
Venv without-pip được kiểm bằng pip host với `--python` đúng interpreter venv;
không gọi pip chưa tồn tại bên trong venv.
Không đổi ACL/share public. Nếu download tự động bị chặn, dùng Files pane để
tải exact `col01-output.zip` đã có (không phải ZIP handoff).

## Đọc kết quả

- `pilot-eda.json`: identity, code/module/loader/runner SHA, Python/packages,
  input before/after hashes, null/nonfinite/duplicate, self vectors và scaling;
  required/available/missing intervals giữ nguyên từ validated coverage.
- `windows.csv`: mỗi mainshock một dòng, PRE/MAINSHOCK/POST và self-only.
- `window-counts.png`: số candidate theo thứ tự CSV, kèm pilot/feature pin.
- `output-checksums.json`: các SHA để readback ba artifact trên.

Pilot thật: 279 feature rows, 2 mainshocks, 279 unique candidate IDs và 279 cặp.
Window1: 1 dòng MAINSHOCK, 0 PRE/POST; window2: 278 dòng gồm22 PRE,1 MAINSHOCK,
255 POST. Đây là kết quả tính từ bundle, không hardcode vào EDA. Unique event
count không mặc định bằng grain count, dù tình cờ bằng nhau trong pilot này.

Available scope chung `[2000-01-21T14:21:08.760Z, 2000-02-04T14:21:08.760Z)`
là14ngày; required window dài hơn, còn missing ranges ở hai đầu. Coverage chỉ
đo temporal input scope, không chứng nhận catalog completeness. Self-only
không chứng minh không có dư chấn trong thực tế. Pilot bị giới hạn bởi selection,
Mc/window/depth/magnitude và missing scope; không suy rộng toàn Nhật Bản/toàn kỳ.
Vector vật lý km/giờ, scaled dimensionless; không tạo lại feature/scaling.

## Giới hạn và bước sau

COL-01 không scientific approval. **DAT-03 — catalog/handoff** còn full catalog,
official ML export và receipts. **COL-03 — baseline metrics/plots** và
**EXP-04 — evaluation/stability** sở hữu diễn giải clustering; không thuộc EDA này.
