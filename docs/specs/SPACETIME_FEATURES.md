# MLD-04 — feature không-thời gian 4-D

`SpacetimeFeatureEngine` dùng chung cho snapshot local và DAT-02, không tạo
feature bằng Python. Công thức theo [task MLD-04](../task/tasks/MLD-04.md):
dx/dy chuyển độ sang km với cos(latitude **radian**), dz giữ dấu độ sâu,
dt lấy chênh lệch epoch microsecond rồi đổi sang giờ. Vector duy nhất:
`[x_scaled, y_scaled, z_scaled, time_scaled]`; magnitude chỉ là lineage/filter.

## Scaling và quality gate

- GLOBAL: R/Z/T lấy từ config versioned.
- ADAPTIVE_WINDOW: R = search_radius_km, T = post_window_hours của từng
  mainshock; Z khóa theo config. Không fit scaler trên candidates.
- Hai preset `config/ml/feature_scaling_*_dev_v1.json` dùng 50 km/30 km/720 giờ
  cho thử nghiệm phát triển, **không phải tham số nghiên cứu đã hiệu chỉnh**.
  Với adaptive, R/T trong preset chỉ là giá trị cấu hình cơ sở; giá trị thực tế
  lấy từ window metadata và lưu trong từng row, Z vẫn 30 km.
- Giữ raw relative fields, kiểm tra lại với công thức trước khi thay bằng
  giá trị đã tính. Null/NaN/Infinity, ID/grain/count/self, sai role, lineage,
  membership hoặc resource guard có reason và count. Bất kỳ invalid row nào
  đều chặn toàn bộ lần ghi, không export tập con hợp lệ.
  Một row có thể có nhiều reason; tổng reason occurrences có thể lớn hơn
  invalid_count, nhưng input_count = valid_count + invalid_count luôn phải đúng.
- Self xác định bằng ID và raw time/coordinates phải trùng mainshock; đúng
  một self/window với vector zero. Non-self cùng timestamp vẫn POST với dt=0.
  PRE âm, không dùng epsilon để đẩy sự kiện sang phía khác.

## Chuẩn bị và chạy local

Chọn JDK 17 hoặc 21 qua JAVA_HOME. Java 26 không được hỗ trợ. Runtime local
được chuẩn bị riêng; không sửa nhãn/fingerprint container cũ để lách freshness.

1. `python3 scripts/local-ml.py prepare`: build JAR/classpath một lần, ghi
   checksum dependency/JAR/source trong target (ngoài Git).
2. Stack hiện có phải sẵn sàng, có evidence và staging MLD-03 đúng SHA.
   `python3 scripts/stage-ml-windows.py --output staging/ml-window-inputs/reproduction-v1`
   chỉ copy/read/hash các file pin cụ thể từ container đang chạy; không ingest,
   build/pull/start/init. Thiếu nguồn thì dừng, không tự tạo dataset thay thế.
3. `python3 scripts/local-ml.py features` đọc input local, đối chiếu Published
   Gold qua Trino loopback (mặc định 8081; dùng `--trino` nếu port khác), kiểm tra
   từng canonical ID/value với physical snapshot và parent BUILDING.
4. Adaptive: thêm `--scaling config/ml/feature_scaling_adaptive_dev_v1.json`.

Output `staging/ml-features/feature_<hash>/`: features.parquet và
feature-report.json, schema/vector/scaling/count/physical+logical checksum.
Đọc lại Parquet trước công bố local; rerun cùng identity so byte toàn bộ file,
không overwrite. Footer được canonicalize bằng rule có sẵn của Silver; giữ
nguyên pages/rows/schema, chỉ sort metadata không có ý nghĩa thứ tự để ổn định
checksum giữa JVM. Có regression độc lập qua ba tiến trình JVM.
`feature_id` gồm input receipt, dataset, config, JAR hash và runtime versions.
Đây là `FEATURE_READY_LOCAL`, **không phải** VALIDATED/EXPORTED. SQL resolver
chỉ SELECT; không gọi DatasetManifestWriter.build vì phương thức đó có ghi.

## Giới hạn và downstream

- [DAT-02 — Tạo dataset pilot nhỏ để test trên Jupyter](../task/tasks/DAT-02.md)
  bổ sung selection/coverage/manifest/checksums và notebook inspection riêng.
- [MLD-05 — Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md)
  sở hữu [official validation/snapshot/export](./FEATURE_EXPORT.md) và coverage
  gate; LocalFeatureJob/DAT-02 không tự nâng parent thành EXPORTED.
- [EXP-02 — Chạy Window, DBSCAN và HDBSCAN global](../task/tasks/EXP-02.md)
  sở hữu fit/đánh giá; task này không fit hoặc tune thuật toán.
