# Dataset nghiên cứu dùng chung — DAT-03

Kit gồm hai official collection exports: REPRODUCTION225tháng và EXTENSION63tháng,
cùng Published Gold pin. Đây không phải DAT-02 development pilot. Không có
pipeline credential, source archives, Iceberg physical warehouse, logs hoặc venv.

Kit v4 sửa đường dẫn Windows và reader UTC, không đổi bytes hai collections.
Không cần tải timezone database C++ hoặc cấu hình thư mục Downloads/tzdata.
Trên Windows, requirements pin thêm gói Python tzdata cho zoneinfo khi Arrow
trả về datetime; cài bằng pip -r requirements.txt như dưới, không cài thủ công
database C++. Verifier dùng epoch integer, vẫn reject precision loss, UTC
anchor sai hoặc writer không nhận diện. Không tự sửa script/manifest trong kitv2;
tải bản mới cùng external ZIP/handoff SHA do HoaiTam cung cấp.

## Trước khi chạy code

Nhận ZIP, SHA256 ZIP và SHA256 handoff.json từ HoaiTam qua kênh nhóm tin cậy;
giữ hai expected hashes **ngoài ZIP**. Kiểm SHA ZIP rồi giải nén vào thư mục mới.
Không tắt verifier hoặc tự tính expected hash từ file bị nghi ngờ. SHA xác nhận
byte identity, không thay xác thực người gửi nếu cả file và pins đều bị đổi.

Tạo Python3.11–3.13 venv ngoài kit, cài requirements.txt (chỉ verifier/loader,
không đủ dependency để fit model). Giải nén vào `research-kit`; đứng ở thư mục
cha của `research-kit` và tự chạy trên máy riêng:

```bash
python research-kit/verify_dataset.py --expected-handoff-sha256 "$EXPECTED_HANDOFF_SHA256" --recipient HoaiTam --receipt HoaiTam-research-receipt.json
```

ThanhTris/rosy179 đổi cả recipient và receipt. Verifier kiểm toàn file set/code
hash trước import, rồi lần lượt đọc/validate tất cả shards của cả hai split.
Không gộp mọi feature vào RAM. Thành công: TEAM_RESEARCH_VERIFIED,
all_shards_verified=true, đúng hai dataset/collection IDs, hashes và counts.
Gửi receipt cho owner; không tạo hộ receipt thành viên khác trên một máy.
Receipt phải ở ngoài kit, không dùng đường dẫn chứa `..`; verifier chủ động
reject đường dẫn không canonical. Có thể dùng absolute canonical path cho receipt.

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
