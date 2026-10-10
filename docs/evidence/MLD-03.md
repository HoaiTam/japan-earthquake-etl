# Evidence Bàn Giao Task MLD-03

## Thông Tin Chung

- **Task ID:** MLD-03
- **Tên task:** Chọn mainshock và tạo candidate windows
- **Assignee:** rosy179
- **Reviewer:** unassigned (chưa có reviewer độc lập)
- **Workstream:** Mainshock and candidate windows
- **Block:** G - ML Dataset
- **Scope:** Core
- **Priority:** P0
- **Branch:** `feat/mld-03-mainshock-candidate-windows`
- **Thời gian thực hiện:** Tuần 4 (kế hoạch tuần 4 mở rộng)

---

## 1. Kết Quả Kiểm Thử Tự Động (13/13 Tests Pass)

Toàn bộ 13 test cases đã được thực thi và vượt qua 100% trên môi trường Spark Java 17/21 thông qua lệnh:
```bash
./mvnw -pl spark -am -Dtest=WindowModelResolverTest,MainshockWindowEngineTest,MainshockWindowIntegrationTest test
# hoặc make test-ml-window
```

### 1.1. `WindowModelResolverTest` (6/6 tests pass)
1. `testUhrhammer1986CalculationsAndMonotonicity`: Xác minh công thức Uhrhammer (1986) cho bán kính $d(M)$ và thời lượng $t(M)$ tại $M=5.5, 6.0, 7.0$; kiểm chứng tính tăng đơn điệu chặt chẽ theo magnitude.
2. `testGardnerKnopoff1974Calculations`: Xác minh công thức Gardner & Knopoff (1974) cho bán kính $L(M)$ và thời lượng $T(M)$.
3. `testExpandedModelWithCaps`: Kiểm chứng các trần an toàn (caps) cho bán kính và thời lượng tại các siêu trận động đất cực lớn ($M=9.0$).
4. `testBoundingBoxConservativeCoverage`: Xác minh hình học rằng khoảng bounding box vĩ độ/kinh độ phủ hoàn toàn bán kính mặt đất hình cầu tại mọi vĩ độ, bao gồm ca biên cực (lat $89.5^\circ$).
5. `testConfigCanonicalJsonAndSha256Determinism`: Xác minh tính tất định của serialization canonical JSON và mã băm SHA-256 64 ký tự hex.
6. `testInvalidInputsThrowException`: Kiểm tra bắt lỗi an toàn khi truyền NaN, Infinite hoặc vĩ độ ngoài phạm vi $[-90, 90]$.

### 1.2. `MainshockWindowEngineTest` (6/6 tests pass)
1. `testGrainUniquenessAndSelfMainshockInvariants`: Kiểm chứng hạt composite `(dataset_id, mainshock_event_id, candidate_event_id)` là duy nhất; mainshock tự xuất hiện đúng 1 lần trong cửa sổ với `is_mainshock=true`, vai trò `MAINSHOCK`, vector độ lệch bằng 0; phân định đúng vai trò `PRE` và `POST`.
2. `testEventInMultipleWindowsNotTreatedAsDuplicate`: Kiểm chứng sự kiện nằm trong vùng chồng lấn của hai mainshock được giữ nguyên vẹn trong cả hai cửa sổ mà không bị deduplicate sai.
3. `testNoCrossJoinPrefilterInPlan`: Phân tích Execution Plan của Spark SQL chứng minh join sử dụng tiền lọc theo thời gian và bounding box không gian trước khi tính khoảng cách phẳng cục bộ.
4. `testHardLimitResourceGuardFlaggingNoSilentTruncation`: Kiểm chứng khi số lượng candidate vượt ngưỡng trần tài nguyên (`maxCandidatesPerWindow`), cửa sổ bị gắn cờ `FLAGGED` kèm lý do `ML_RESOURCE_LIMIT_EXCEEDED` và **toàn bộ 100% ứng viên được giữ lại**, không âm thầm cắt bớt (no silent truncation).
5. `testNestedMainshockDetection`: Kiểm chứng phát hiện và đếm số lượng nested mainshocks trong các cửa sổ kề nhau mà không loại bỏ chúng khỏi tập dữ liệu.
6. `testMainshockSelectionCriteriaBoundaryConditions`: Kiểm chứng chính xác các ca biên lọc mainshock: $M=5.5$ bị loại, $M=5.51$ được chọn; độ sâu $49.9\text{ km}$ bị loại, $50.0\text{ km}$ và $200.0\text{ km}$ được chọn, $200.1\text{ km}$ bị loại; sự kiện ngoài khoảng thời gian bị loại.

### 1.3. `MainshockWindowIntegrationTest` (1/1 test pass)
1. `testEndToEndAuditToWindowPipelineWithReportExport`: Mô phỏng quy trình E2E hoàn chỉnh từ đầu vào Gold $\rightarrow$ kiểm toán và ước lượng $M_c$ qua `GoldInputAuditEngine` (MLD-02) $\rightarrow$ sinh cửa sổ ứng viên qua `MainshockWindowJob` (MLD-03) $\rightarrow$ xuất artifact báo cáo Markdown/JSON và Parquet snapshots $\rightarrow$ làm giàu dataset manifest.

---

## 2. Đối Soát Tiêu Chí Hoàn Thành (Acceptance Criteria)

| Tiêu chí hoàn thành trong MLD-03.md | Trạng thái | Bằng chứng thực tế |
|---|:---:|---|
| Grain `(dataset_id, mainshock_event_id, candidate_event_id)` là duy nhất | **ĐẠT** | Phương thức `assertInvariants()` đối soát `count() == distinct().count()`. Test `testGrainUniquenessAndSelfMainshockInvariants` và `testEventInMultipleWindowsNotTreatedAsDuplicate` đã chứng minh. |
| Mỗi window chứa chính mainshock đúng một lần và giữ candidate PRE/POST | **ĐẠT** | Mainshock row có `is_mainshock = true`, vai trò `MAINSHOCK`, vector độ lệch `0.0`. PRE có $\Delta t < 0$, POST có $\Delta t > 0$. Kiểm chứng qua `testGrainUniquenessAndSelfMainshockInvariants`. |
| Không cross join mainshock với toàn catalog; physical plan/test chứng minh có prefilter | **ĐẠT** | Range join condition chứa cả 6 vị từ thời gian và hộp không gian $[\min,\max]$. Test `testNoCrossJoinPrefilterInPlan` trích xuất optimized plan chứng minh không có unconditioned Cross Join. |
| Event có thể ở nhiều window mà không bị coi là duplicate sai | **ĐẠT** | Test `testEventInMultipleWindowsNotTreatedAsDuplicate` chứng minh `SHARED_EV` xuất hiện ở cả `M1` và `M2`, cả 2 bản ghi đều được giữ trong snapshot. |
| Window vượt hard limit bị fail/flag có reason, không âm thầm truncate | **ĐẠT** | Test `testHardLimitResourceGuardFlaggingNoSilentTruncation` chứng minh khi số lượng vượt trần (6 > 3), cả 6 bản ghi đều được giữ và window nhận trạng thái `FLAGGED` kèm lý do `ML_RESOURCE_LIMIT_EXCEEDED`. |

---

## 3. Liên Kết Upstream và Downstream

1. **Upstream:**
   - [MLD-01 - Pin Gold snapshot và tạo dataset manifest](../task/tasks/MLD-01.md): Cung cấp `dataset_id` bất biến và snapshot ID đã publish.
   - [MLD-02 - Audit Gold và xác định magnitude of completeness](../task/tasks/MLD-02.md): Cung cấp cờ `is_eligible` và ngưỡng $M_c$ có version để lọc ứng viên $\text{magnitude} \ge M_c$.
2. **Downstream:**
   - [MLD-04 - Tạo và scale feature không-thời gian 4-D](../task/tasks/MLD-04.md): Tiếp nhận `ml.sequence_candidate_snapshot` ở giai đoạn window để chuẩn hóa không-thời gian và tính vector đặc trưng 4 chiều $(x, y, z, t)$ tương đối quanh mainshock.
   - [MLD-05 - Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md): Đóng gói và kiểm định checksum trước khi xuất bundle sang Colab cho HDBSCAN clustering.
