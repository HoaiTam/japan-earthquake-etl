# Shared scripts

Thư mục này chứa script dùng chung cho phát triển, CI và vận hành local. Script
phải chạy từ bất kỳ working directory nào, fail fast và không xóa data/volume
theo mặc định.

Hiện có:

- `check-repository-layout.sh`: smoke check cho scaffold `REP-01`.
- `check-config.sh`: kiểm tra `.env.example`, `.env` và secret hygiene cho
  `CFG-01`; dùng `--require-local` trước runtime và không in giá trị cấu hình.
- `check-compose.sh`: validate Compose schema, network, named volumes và
  lifecycle labels cho `CMP-01` mà không khởi động service.
