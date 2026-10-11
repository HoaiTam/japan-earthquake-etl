# Evidence MLD-03 - Bản vá review PR #61

- Task: MLD-03 - Chọn mainshock và tạo candidate windows.
- Assignee bản vá: **HoaiTam**; tác giả implementation ban đầu: rosy179.
- Reviewer: **unassigned** (chưa có review độc lập).
- Branch: `feat/mld-03-mainshock-candidate-windows`.
- PR: [#61](https://github.com/HoaiTam/japan-earthquake-etl/pull/61), sửa trực tiếp, không tạo PR/branch mới.
- Trạng thái: **Needs Update**; acceptance kỹ thuật có regression tests, chưa nghiệm thu Published JMA/Mc thật.
- Main được tích hợp: `b29478bd6190338a8d60818177b12370650a5375`, giữ Gold writer/publication, dataset manifest và MLD-02 mới.
- Ngày kiểm tra: **2026-10-11**, Asia/Ho_Chi_Minh.

## 1. Đối chiếu 5 điểm review

| Điểm review | Bản vá và bằng chứng |
|---|---|
| Non-equi join vẫn BroadcastNestedLoopJoin | Window expand các bucket UTC-day / floor(latitude) / floor(longitude); candidate thuộc một bucket; equi-join ba keys rồi time/bbox/distance residual. Test executed physical plan không có CartesianProduct/BroadcastNestedLoopJoin, có hash/sort-merge join; chạy cả broadcast mặc định và tắt broadcast với input repartition. WindowModelConfig Serializable để UDF hoạt động qua shuffle. |
| Invariant collect toàn candidates và lặp M×C trên driver | GroupBy grain, aggregate self/count, full-outer đối soát window và distributed join/filter kiểm tra role/time/vector. Chỉ scalar count/boolean trả về driver; không collect candidate rows. Engine chỉ collect metadata theo window cho report. |
| Parquet lỗi nhưng catch(Throwable) fallback JSON báo success | Bỏ fallback; propagates storage/config exceptions; readback cả hai Parquet counts rồi mới ghi success reports. Invalidate hai report cũ trước computation. Regression tạo missing output committer, xác nhận job throw và không còn success report/JSON fallback; reject hard-limit không export. |
| Cast timestamp xuống giây làm mất precision | Dùng unix_micros/timestamp_micros cho bounds và delta; PRE -0.8s và POST +0.2s giữ đúng dấu. Test exact boundaries inclusive, ngoài biên một microsecond, qua ngày và spatial cell. Tie non-self cùng timestamp giữ POST/delta=0, không bịa epsilon/aftershock label. |
| Done/evidence dùng synthetic input gây hiểu nhầm | Task/index đồng bộ Needs Update, assignee HoaiTam. Integration hiện là audit→window **fixture**, lấy mcApplied được chấp nhận từ MLD-02, không dùng central Mc chưa reliable. Chưa có Published JMA audit/Mc thật; không gọi đây là runtime E2E hoặc nghiệm thu dataset thực. |

## 2. Kiểm thử và cách chạy lại

Target `make test-ml-window` gồm **21 tests**:

- WindowModelResolverTest: 6 tests công thức/config/bounding-box theo mô hình phẳng cục bộ. Không dùng test này để tuyên bố kiểm chứng khoảng cách cầu/haversine.
- MainshockWindowEngineTest: 6 tests grain/self/PRE/POST, overlapping windows, physical plan, resource flag không truncate, nested mainshock và selection boundaries.
- MainshockWindowIntegrationTest: 1 **fixture** audit→accepted Mc→window→Parquet readback/report/manifest enrichment.
- MainshockWindowRegressionTest: 8 tests precision/corrupt vector, simultaneous non-self, exact microsecond/day/spatial boundaries, shuffle join/order independence, invalid count/duplicate/missing self, Parquet/validation failure invalidation, rejected export và empty/invalid Mc.

Chạy tại repo root:

```bash
make test-ml-window
make ci-docs
make ci-airflow
make ci-java
git diff --check
```

Môi trường kiểm chứng Java: image builder local, Linux ARM64, Java 17.0.20.1,
Maven 3.9.16, Spark 3.5.9, Spark loopback; chạy trực tiếp recipe Maven vì image
builder không có GNU Make. Dependency Iceberg và build plugin mới trên main
được tải từ Maven Central vào cache tạm, không thay version/bỏ tests. Unit/fixture tests không
gọi USGS/JMA API hoặc thay đổi bucket/warehouse.

Kết quả cục bộ đã kiểm chứng:

- Targeted MLD-03: **21 tests PASS**, 0 failures/errors/skips.
- Full Maven `clean verify`: **307 tests PASS**, 0 failures/errors/skips,
  **BUILD SUCCESS**, đóng gói runner JAR thành công; thời gian 6m06s.
  Full suite chạy sau thay đổi cuối cùng, bao gồm regression invalidate report
  khi validation lỗi trước export.
- `make ci-docs`: PASS, gồm contract/config/Compose static checks và 12 Java
  build-input regression tests; không có local .env, chỉ kiểm tra repo/example.
- `make ci-airflow`: **193 tests PASS**, Python 3.13.
- `make test-ci-workflow`: **4 tests PASS**, gồm 16 mutation subcases.
- actionlint 1.7.12, `make check-task-status`, `git diff --check` và staged
  diff check: PASS. Không còn conflict markers/unmerged files.

Đây là local evidence, không phải kết quả GitHub Actions. Sau push, xem
checks trên merge ref mới nhất của PR #61; không suy ra checks PR khác đã xanh.
Các lỗi lần đầu (cache thiếu Iceberg, config chưa Serializable ở nhánh shuffle)
đã được xử lý và giữ regression test để tái lập; không coi lượt lỗi là PASS.

## 3. Giới hạn và gate còn thiếu

- [MLD-02 - Audit Gold và xác định magnitude of completeness](../task/tasks/MLD-02.md):
  còn Needs Update, thiếu audit/Mc có scope từ Published JMA thực. Khi có output
  này, chạy MLD-03 với dataset_id/snapshot đã pin qua
  [MLD-01 - Pin Gold snapshot và tạo dataset manifest](../task/tasks/MLD-01.md),
  lưu config/hash/Mc, reconcile counts và artifact readback thật rồi nghiệm thu.
  Không dùng Mc=2 fixture cho runtime hoặc chuyển task Done vì CI xanh.
- [QA-05 - Profile dữ liệu lịch sử và tài nguyên local](../task/tasks/QA-05.md):
  chưa benchmark full historical, phân bố bucket/skew và chi phí metadata report
  theo số windows; equi-join không phải bằng chứng hiệu năng toàn bộ 40 năm.
- [MLD-04 - Tạo và scale feature không-thời gian 4-D](../task/tasks/MLD-04.md):
  feature scaling/vector chưa thuộc bản vá này.
- [MLD-05 - Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md):
  chưa có verified Colab bundle/checksum hoặc lifecycle dataset VALIDATED.
- Hai local Parquet directories không có atomic multi-table commit; khi export
  lỗi có thể còn partial directory, nhưng không được dùng nó làm readiness.
  Completed window audit report chỉ là gate local, không phải Published Iceberg.
- Candidate window là tiền lọc hồi cứu, không phải nhãn dư chấn hoặc dự báo động đất.

## 4. Tài liệu liên quan

- [Task MLD-03](../task/tasks/MLD-03.md)
- [Mainshock/window spec](../specs/MAINSHOCK_AND_CANDIDATE_WINDOWS.md)
- [ML logical model](../specs/ML_DATA_MODEL.md)
- [CI và stacked PR](../conventions_and_workflow/CONTINUOUS_INTEGRATION.md)
