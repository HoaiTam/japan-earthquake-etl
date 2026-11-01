# EXP-01 — Local gate → isolated Colab CPU

Entry point cho baseline portability của tuần7. Không cần Docker, MinIO,
Airflow hoặc `.env`. Notebook cloud chưa chạy đạt thì EXP-01 chưa Done.
Xem [evidence ngày01/11](../evidence/EXP-01_COLAB_20261101.md).

## 1. Chuẩn bị local

Lấy code có các file trong PR EXP-01 này. Dùng Python3.11–3.13 và tải
[pilot JMA chính xác](https://drive.google.com/file/d/12eLvQJzEkBuT0WYaxHYvb9KZlk5MK_vb/view).
ZIP gốc phải có SHA256
`f58291b72873e1b26791fef97818fdcf5e2b028a5dfa528885a666d8cdc1b870`.
Không dùng partial catalog hoặc ZIP source JMA làm feature input.

Từ root repo, macOS/Linux:

```bash
python3 -m venv .venv-colab
.venv-colab/bin/python -m pip install -r notebooks/requirements-colab-lock.txt
.venv-colab/bin/python -m pip check
.venv-colab/bin/python -B scripts/exp01-colab.py local-pack --pilot-zip /absolute/path/jma-development-pilot-team.zip --output staging/exp01-local-first --recipient HoaiTam
```

Windows: thay `.venv-colab/bin/python` bằng
`.venv-colab/Scripts/python.exe`; thay đường dẫn ZIP bằng absolute path thật.
Recipient thuộc `HoaiTam`, `rosy179`, `ThanhTris`. Output phải chưa tồn tại;
rerun dùng tên mới, không xóa/overwrite evidence lỗi.

Expected: `LOCAL_GATE_AND_HANDOFF_VERIFIED`, fresh `nbclient` kernel,279rows/
2mainshocks, WINDOW/DBSCAN/HDBSCAN_GLOBAL cùng seed42. `handoff-receipt.json`
ghi trusted `zip_sha256`; `exp01-colab-handoff.zip` là input cloud duy nhất.
Không lấy một JSON có chữ PASSED thay local gate; fallback/synthetic bị chặn.

## 2. Chạy Colab thật

1. Mở bản sạch [notebook bootstrap](../../notebooks/exp_01_colab_bootstrap.ipynb)
   trên tài khoản Colab của mình, chọn runtime CPU/None mới.
2. Qua Files → Upload to session storage, tải **handoff ZIP** của bước1 lên.
   Không tải `.env`, OAuth JSON/token, warehouse hoặc toàn bộ staging.
3. Run all. Nhập lần lượt absolute ZIP path trên VM (thường
   `/content/exp01-colab-handoff.zip`), SHA từ receipt local, recipient.
4. Cell1 kiểm external SHA trước giải nén, bounded/exact file set và từng file
   hash trước chạy source trong ZIP. Cell2 tạo venv riêng không gọi ensurepip,
   dùng host pip `--python` cài vào venv (pip25.3 được pin), cài exact direct pins
   và chạy `pip check`; không thay thư viện host kernel.
5. Cell3 kiểm local report/source/config/package binding **trước fit**, chạy lại
   cùng shared baseline modules và đọc artifacts để đối chiếu. Cell4 tạo ZIP
   output allowlist, đọc lại từng member rồi tải về máy.

Expected lần lượt: `PINNED_HANDOFF_BYTES_VERIFIED`,
`ISOLATED_CPU_ENV_READY`, `COLAB_HARNESS_PASSED` với parity cả ba PASSED,
`OUTPUT_ZIP_READBACK_VERIFIED`. Chỉ thấy notebook được upload hoặc cell1 đạt
chưa phải cloud acceptance. Không tự chỉnh gate/flags/version để bỏ qua lỗi.

Mỗi lần Run all tạo session UUID mới, output mới. Khi lỗi: giữ output/log lỗi,
đọc traceback, sửa bằng PR, rerun local-pack để có source/ZIP SHA mới trước khi
chạy cloud. Không sửa code đã pin bên trong ZIP hay notebook runtime để che lỗi.

## 3. Identity và parity

- Content identity SHA bao phủ16 source/config/requirements/notebook files
  allowlist; không giả `git_base_commit` là commit chứa thay đổi chưa commit.
  Sửa một file trong allowlist phải local-pack lại.
- Direct dependencies exact-pinned, gồm pandas2.2.3/matplotlib3.10.0.
  **Chưa khóa toàn bộ transitive wheels/hashes**; ghi actual `pip list`, Python
  và requirements SHAs cho mỗi run. Không hứa bitwise reproducibility mọi VM.
- Readback kiểm bảy file mỗi model, exact checksum index, schema và279/2counts.
  Parity so grain/role/noise/membership, cluster partitions (không phụ thuộc số
  cluster label), xác suất tolerance abs1e-8/rel1e-6; bỏ qua runID/time/runtime.
- `environment-plot-smoke.png` chỉ chứng minh pandas/matplotlib hoạt động;
  không là biểu đồ đánh giá khoa học hoặc bằng chứng model tốt hơn.

Bootstrap venv không dùng pip của hệ điều hành để ghi vào host site-packages;
`--python` trỏ interpreter venv, `--isolated` bỏ user config/env của pip.
Xem [tài liệu pip chính thức](https://pip.pypa.io/en/stable/topics/python-option/).

## 4. Lưu output, không cấp thêm quyền

Mặc định Files upload/download, **không mount Drive**, không sửa ACL. Kiểm SHA
ZIP và `output-checksums.json` của file tải về trước khi chia sẻ kết quả.
Nếu tự mount Drive, đọc/trust code và tự xác nhận phạm vi quyền; mount cấp
quyền rộng hơn folder pilot. Không tự public link.

Optional copy khi đã mount và chủ tài khoản chọn destination mới:

```bash
/content/ie212-exp01/SESSION/venv/bin/python -B /content/ie212-exp01/SESSION/input/code/scripts/exp01-colab.py copy --handoff-root /content/ie212-exp01/SESSION/input --output /content/ie212-exp01/SESSION/output --destination /content/drive/MyDrive/IE212-Colab-Outputs/HoaiTam/NEW-RUN
```

Thay `SESSION`/destination theo notebook thật. Copy chỉ nhận25file allowlist:
report, pilot receipt, PNG,21model artifacts và checksum map; kiểm hash nguồn
và readback destination. Không copy input, venv hoặc plot-cache. Tên file ZIP
không chứng nhận recipient ACL/session; phần đó thuộc SEC-01.

## 5. Test và ranh giới

Activate model venv rồi `make test-exp01`; target có cả legacy harness và
bootstrap regression tests. `make test-pilot` cũng chạy bootstrap tests trong
CI. Local gate mở kernel loopback; sandbox chặn socket cần cấp quyền chạy
test, không dùng fallback để công bố PASSED.

- DAT-03: full research catalog/export và receipt ba máy vẫn độc lập pilot.
- EXP-02/EXP-03: pilot portability không thay official/scientific acceptance
  hoặc adaptive cloud experiment; bootstrap này chỉ chạy ba baseline mặc định.
- EXP-04: chưa đánh giá chất lượng nghiên cứu/tối ưu tham số.
- SEC-01: còn audit actual Colab/Drive permissions; không chứng nhận từ local.
- MLI-03/MLI-04: không import output development này vào official `ml.*`.

Mọi report giữ `DEVELOPMENT_ONLY`, `research_ready=false`,
`official_exported=false`, `scientific_approval=false`. Không phát `_SUCCESS`
official, không ghi Iceberg/Gold hoặc sửa dataset lifecycle.
