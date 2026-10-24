# Bộ pilot JMA dùng chung — đọc trước khi chạy

Đây là **dữ liệu thật để phát triển**, không phải dataset nghiên cứu toàn kỳ.
Giữ DEVELOPMENT_ONLY, coverage PARTIAL và research_ready=false. Bản nghiệm thu
của nhóm có 279 candidate rows / 2 mainshock; row grain là cặp mainshock/candidate,
không được hiểu là 279 động đất độc lập. Một window self-only vẫn giữ nguyên.

## Nhận file và kiểm tra

Người gửi cung cấp ZIP, SHA-256 của ZIP và `handoff_sha256` qua tin nhắn/tài liệu
tin cậy **ngoài ZIP**. Trước khi giải nén/chạy code, đối chiếu hash ZIP bằng
`shasum -a 256` (macOS/Linux) hoặc `Get-FileHash -Algorithm SHA256` (PowerShell).
Sai hash thì dừng và yêu cầu gửi lại; không tự sửa manifest để pass.
Giải nén vào thư mục mới, không trộn với dataset/attempt khác.

Từ thư mục vừa giải nén, dùng Python 3.11–3.13 và venv **ngoài kit**, chẳng hạn
`python3 -m venv ../jma-pilot-venv`, rồi kích hoạt venv của OS đang dùng.
Không tạo `.venv`, notebook, `__pycache__` hoặc receipt bên trong kit vì verifier
kiểm exact file set. Cài dependencies một lần bằng
`python -m pip install -r requirements.txt`. Chỉ cần pyarrow,
không Docker/Java/Airflow/MinIO/.env hoặc tài khoản pipeline.

Chạy `python verify_dataset.py --expected-handoff-sha256 <pin-người-gửi>
--recipient <tên-GitHub> --receipt ../<tên-GitHub>-pilot-receipt.json` trên một
dòng; thay pin/tên thực tế, không giữ dấu ngoặc. Tên hợp lệ: HoaiTam, ThanhTris,
rosy179. Receipt nằm **ngoài** thư mục kit, file mới, không sửa/ghi đè bundle.

Thành công: `status=TEAM_PILOT_VERIFIED`, đúng pilot_id, feature/mainshock counts,
coverage PARTIAL, research_ready=false. Gửi receipt cho HoaiTam để ghi nhận
đã nhận/đọc được trên máy của bạn. Receipt tự khai không là xác thực danh tính,
review approval hoặc bằng chứng đã fit model.

## Dùng trên Jupyter

Input ở `bundle/`. Đặt `sys.dont_write_bytecode=True` trước import để không tạo
`__pycache__` trong kit. Có thể import `pilot_bundle` từ `tools/` rồi gọi
`load_bundle("bundle")`: kết quả có `features`, `mainshocks`, `manifest`,
`coverage`, `summary`; features/mainshocks là PyArrow Table, có thể `.to_pandas()`
nếu môi trường có pandas. Không viết lại coordinate/scaling trong notebook.

Để chạy notebook/algorithms của project, checkout code phù hợp trên GitHub,
chuẩn bị model environment theo EXP-01/02 và đặt PILOT_BUNDLE trỏ đúng `bundle/`
đã verify (absolute path). Không dùng fallback synthetic rồi hiểu đó là pilot thật.
Mỗi người giữ input readonly và output/experiment_run_id riêng, không sửa Parquet
hoặc ghi kết quả vào `bundle/`. Không fit HDBSCAN một lần trên toàn catalog.

## Có gì và giới hạn

- `bundle/`: đúng 5 file DAT-02 (features, mainshocks, manifest, coverage, checksums).
- `tools/pilot_bundle.py`, `verify_dataset.py`: kiểm checksum/schema/grain/finite/
  self/coverage; không tạo feature hoặc kết quả nghiên cứu.
- `handoff.json`: pilot ID/counts và hash chính xác các file, được pin bên ngoài.
- Input JMA của pilot chỉ khoảng 14 ngày, không chứng minh đủ tháng/năm/nghiên cứu.
- Không phải official EXPORTED dataset, không eligible MLI-03 official import,
  không phải nhãn ground truth hoặc chứng minh thuật toán tốt hơn.
- DAT-03 còn chuẩn bị catalog nghiên cứu thật; EXP-02/03 còn official acceptance,
  EXP-04/05 evaluation, MLI-04/MLQ-01 report/ML E2E.
- SEC-01 hiện chỉ nghiệm thu local; mở lại phần Drive/Colab trước khi upload/migration.
  Không chia sẻ .env, credential, private logs hoặc warehouse. Không có cloud link
  public nào được tạo bởi kit này.
