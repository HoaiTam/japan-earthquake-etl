# Dataset nghiên cứu dùng chung — DAT-03

Kit gồm hai official collection exports: REPRODUCTION225tháng và EXTENSION63tháng,
cùng Published Gold pin. Đây không phải DAT-02 development pilot. Không có
pipeline credential, source archives, Iceberg physical warehouse, logs hoặc venv.

## Trước khi chạy code

Nhận ZIP, SHA256 ZIP và SHA256 handoff.json từ HoaiTam qua kênh nhóm tin cậy;
giữ hai expected hashes **ngoài ZIP**. Kiểm SHA ZIP rồi giải nén vào thư mục mới.
Không tắt verifier hoặc tự tính expected hash từ file bị nghi ngờ. SHA xác nhận
byte identity, không thay xác thực người gửi nếu cả file và pins đều bị đổi.

Tạo Python3.11–3.13 venv ngoài kit, cài requirements.txt (chỉ verifier/loader,
không đủ dependency để fit model). Mỗi người tự chạy trên máy riêng:

```bash
python verify_dataset.py --expected-handoff-sha256 "$EXPECTED_HANDOFF_SHA256" --recipient HoaiTam --receipt ../HoaiTam-research-receipt.json
```

ThanhTris/rosy179 đổi cả recipient và receipt. Verifier kiểm toàn file set/code
hash trước import, rồi lần lượt đọc/validate tất cả shards của cả hai split.
Không gộp mọi feature vào RAM. Thành công: TEAM_RESEARCH_VERIFIED,
all_shards_verified=true, đúng hai dataset/collection IDs, hashes và counts.
Gửi receipt cho owner; không tạo hộ receipt thành viên khác trên một máy.

## Dùng trong Jupyter

Thêm thư mục tools vào Python path; dùng ResearchCollection trong
tools/research_collection.py với expected manifest/checksums SHA của split
trong handoff đã verify. Gọi load_shard với đúng shard_id; mỗi shard giữ nguyên
whole-mainshock windows, parent dataset_id/global Mc và collection_scope.
Không đổi schema, rescale features, chia window, trộn split hoặc ghi vào input.
Notebook/output/venv/receipt để ngoài kit; mỗi người có experiment_run_id riêng.

Kit không tự cấp scientific approval, không chứng nhận thuật toán tốt nhất
hoặc dự đoán động đất. Môi trường fit phải dùng lock/config versions của EXP-01/02.
Colab/Drive sharing cần mở lại SEC-01 cloud trước; kit này chỉ nghiệm thu local.
