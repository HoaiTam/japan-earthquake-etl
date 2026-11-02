# EXP-02 — Baseline Colab Portability & Parity Verification

- **Ngày kiểm chứng:** 2026-11-02
- **Tuần thực hiện:** Tuần 7 (02–08/11/2026), Khối 2
- **Assignee:** `rosy179`
- **Reviewer:** `unassigned`
- **Branch:** `feat/exp-02-colab-baseline`
- **Kế hoạch dẫn chiếu:** [Tuần 7 — Kế hoạch Colab](../task/WEEK_7_COLAB_PLAN.md) và [Runbook EXP-01 Colab Bootstrap](../runbooks/EXP-01_COLAB_BOOTSTRAP.md)

---

## 1. Mục tiêu và bối cảnh

Task [EXP-02 - Chạy Window, DBSCAN và HDBSCAN global](../task/tasks/EXP-02.md) đã hoàn tất giai đoạn phát triển và kiểm chứng kỹ thuật cục bộ (Local Development) tại mốc 2026-10-18 (Review Round 4, xem [EXP-02.md](./EXP-02.md)).

Tại Tuần 7, theo kế hoạch ba khối độc lập, **rosy179** phụ trách Khối 2 với mục tiêu:
1. Tiếp nhận pilot JMA thật dùng chung từ Google Drive (279 dòng đặc trưng 4-D không-thời gian, 2 mainshock windows).
2. Thiết lập môi trường kiểm chứng cục bộ (Local Gate), thực thi đóng gói kiểm chứng toàn vẹn mã nguồn và cấu hình mô hình qua `scripts/exp01-colab.py local-pack`.
3. Chuyển giao gói thực nghiệm lên máy chủ Google Colab (CPU runtime), thực thi độc lập cả ba thuật toán baseline (`WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`) trên môi trường đám mây cô lập.
4. Đối soát tương đương (parity) giữa Local và Google Colab theo các tiêu chí nghiêm ngặt: grain matching, role assignment, noise classification, cluster membership partitions và numerical tolerance ($rtol = 10^{-6}, atol = 10^{-8}$).

Toàn bộ dữ liệu thực nghiệm giữ nguyên định danh `DEVELOPMENT_ONLY`, `research_ready=false`, `official_exported=false`, không phát sinh `_SUCCESS.json` và không ghi đè vào không gian official `ml.*`.

---

## 2. Kiểm chứng Local Gate (`rosy179`)

- **Pilot JMA thật:**
  - File: `jma-development-pilot-team.zip`
  - SHA-256: `f58291b72873e1b26791fef97818fdcf5e2b028a5dfa528885a666d8cdc1b870`
  - Handoff SHA-256: `9121d6839f9ef8e5646a06cd0bf870fcde50b65da00c03e7b268760237cddc9a`
  - Pilot ID: `pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c`
  - Kích thước hạt: 279 hàng candidate, 2 mainshocks, coverage `PARTIAL`.
- **Môi trường cục bộ:**
  - Runtime: Python 3.11.0 trên Windows x86_64, môi trường ảo cô lập `.venv-colab`.
  - Khóa thư viện direct exact-pinned: `pyarrow==21.0.0`, `numpy==2.2.2`, `scipy==1.15.1`, `scikit-learn==1.6.1`, `hdbscan==0.8.40`, `nbclient==0.10.2`, `nbformat==5.10.4`, `pandas==2.2.3`, `matplotlib==3.10.0`, `pip==25.3`.
  - `pip check`: `PASSED` (0 broken requirements).
- **Thực thi Local Gate:**
  - Lệnh: `.venv-colab/Scripts/python -B scripts/exp01-colab.py local-pack --pilot-zip "..." --output staging/exp02-local-rosy179 --recipient rosy179`
  - Trạng thái kiểm chứng: `LOCAL_GATE_AND_HANDOFF_VERIFIED`.
  - Inspection: Fresh `nbclient` kernel, thực thi 2 code cells trong 17.04s, `RESTART_RUN_ALL_PASSED`, `fresh_kernel_acceptance=true`, `colab_migration_allowed=true`.
  - Checksum danh tính mã nguồn (`source_sha256`): `7a65c8fa203c143283a07de2678dc9f7fd16569342f6d6f43e5edcfdf102ff4d`.
  - Checksum báo cáo cục bộ (`local_smoke_sha256`): `b68ee4e1606f12349f234c7d2776d12bd14da9627d515c6c1940c5d1f0c2b01a`.
  - Gói Handoff xuất ra: `staging/exp02-local-rosy179/exp01-colab-handoff.zip` (171.993 bytes), SHA-256: `d6be707111be8d2b1a2ba64434d6c0cacaa67b73c5aae3235f9772403c544333`.

---

## 3. Thực thi và Đối soát trên Google Colab (`rosy179`)

- **Phiên thực thi đám mây:**
  - Nền tảng: Google Colab (CPU runtime, Linux x86_64).
  - Probe môi trường: `{'module': '/usr/local/lib/python3.13/dist-packages/google/colab/__init__.py', 'system': 'Linux'}`.
  - Runtime Python: Python 3.13, môi trường venv cô lập (`--without-pip`, host pip `--isolated --python`).
  - Notebook sử dụng: `notebooks/exp_01_colab_bootstrap.ipynb`.
  - Recipient: `rosy179`.
  - Thời điểm hoàn tất (UTC): `2026-11-02T03:31:43.410294+00:00`.
- **Kết quả nghiệm thu tổng thể:**
  - Trạng thái: `COLAB_HARNESS_PASSED`.
  - Toàn bộ 3 thuật toán baseline đều đạt đối soát tuyệt đối: `WINDOW: PASSED`, `DBSCAN: PASSED`, `HDBSCAN_GLOBAL: PASSED`.
- **Gói kết quả xuất ra từ Colab:**
  - File: `exp01-colab-output.zip`
  - SHA-256: `df5dae61e79cb707e8136af2b46037b27b5818f0abc89bedb59dab218d4b8665`
  - Đã giải nén và kiểm chứng readback tại `staging/exp02-colab-rosy179/`.
  - Đủ 24/24 artifacts allowlist, toàn bộ mã băm trong `output-checksums.json` khớp 100%.

---

## 4. Bảng số liệu đối soát (Reconciliation & Parity Details)

Toàn bộ 279 hạt ứng viên `(mainshock_event_id, candidate_event_id)` và 2 mainshock windows được đối chiếu từng thuộc tính:

| Thuật toán | Tổng ứng viên | Windows | Sequence Members | Aftershocks | Noise Events | Failed Windows | Local–Colab Parity |
|---|---:|---:|---:|---:|---:|---:|:---:|
| **WINDOW** | 279 | 2 | 257 | 255 | 0 | 0 | **PASSED** |
| **DBSCAN** | 279 | 2 | 251 | 240 | 9 | 1 | **PASSED** |
| **HDBSCAN_GLOBAL** | 279 | 2 | 0 | 0 | 108 | 2 | **PASSED** |

### Chi tiết các tiêu chí kiểm chứng Parity:
1. **Grain Matching:** Khớp tuyệt đối $279/279$ cặp khóa `(mainshock_event_id, candidate_event_id)`.
2. **Role & Flag Invariants:** Phân loại `PRE`, `MAINSHOCK`, `POST`, cờ `is_noise`, và nhãn quan hệ sequence khớp $100\%$ giữa Local và Colab.
3. **Partition Invariance:** Cụm phân rã được so khớp theo cấu trúc tập hợp phân hoạch (frozenset of members), không phụ thuộc vào giá trị số của nhãn cụm (cluster label permutation). Cả Local và Colab sinh ra các tập phân cụm giống nhau tuyệt đối.
4. **Numerical Tolerance:** Xác suất thành viên (`membership_probability`) của HDBSCAN tuân thủ chặt chẽ ngưỡng sai số $rtol = 10^{-6}$ và $atol = 10^{-8}$. Giá trị xác suất của Window và DBSCAN giữ nguyên `null` theo đúng contract.

---

## 5. Đánh giá kỹ thuật và phát hiện thực nghiệm

1. **Hiệu năng lọc nhiễu nền của Density Clustering:**
   - Thuật toán `WINDOW` ghi nhận toàn bộ các sự kiện POST trong cửa sổ đếm ($255$ sau chấn), không có khả năng phân tách nhiễu nền ($0$ noise).
   - Thuật toán `DBSCAN` với cấu hình mặc định (`eps=0.5, min_samples=5`) đã phát hiện $9$ sự kiện nhiễu không gian - thời gian, cô lập được chuỗi sau chấn $240$ sự kiện tại cửa sổ thành công.
2. **Quy tắc fail-safe khi Mainshock rơi vào Noise:**
   - Tại cửa sổ thứ 2 của `DBSCAN` và cả 2 cửa sổ của `HDBSCAN_GLOBAL` (với cấu hình mặc định `min_cluster_size=15`), sự kiện mainshock mang nhãn `-1` (nhiễu).
   - Theo đúng đặc tả thiết kế: hệ thống đánh dấu `failed_windows` và không gán chuỗi sau chấn nhân tạo (`total_sequence_members = 0`), tuyệt đối **không tự động chọn cụm gần nhất để ép thành chuỗi**.
   - Việc ghi nhận noise hợp lệ là bằng chứng cho thấy thuật toán tuân thủ đúng ràng buộc vật lý, không phải lỗi triển khai.
   - Hiện tượng này trực tiếp mở đường cho các bước tiếp theo:
     - [COL-02](../task/tasks/COL-02.md): Bounded parameter sweep để khảo sát ảnh hưởng của `eps`, `min_samples`, `min_cluster_size` nhỏ hơn phù hợp với mật độ pilot.
     - [EXP-03](../task/tasks/EXP-03.md): Adaptive HDBSCAN giải quyết tính đa chuỗi và biến thiên mật độ cục bộ.

---

## 6. Danh mục tệp bằng chứng

- Báo cáo đối soát JSON: [EXP-02_COLAB_PARITY_20261102.json](./EXP-02_COLAB_PARITY_20261102.json)
- Gói Handoff cục bộ: `staging/exp02-local-rosy179/exp01-colab-handoff.zip`
- Biên nhận Handoff: `staging/exp02-local-rosy179/handoff-receipt.json`
- Gói kết quả Colab: `C:/Users/Lenovo/Downloads/exp01-colab-output.zip`
- Thư mục giải nén nghiệm thu: `staging/exp02-colab-rosy179/`
- Báo cáo Cloud: `staging/exp02-colab-rosy179/colab-report.json`
- Biên nhận Pilot người nhận: `staging/exp02-colab-rosy179/pilot-receipt.json`

---

## 7. Giới hạn / Rủi ro / Bước tiếp theo

- **Khảo sát tham số giới hạn (COL-02):** Khảo sát tối đa 8 cấu hình (DBSCAN 4 cấu hình, HDBSCAN 4 cấu hình) thuộc về [COL-02 - Sweep tham số baseline giới hạn trên pilot](../task/tasks/COL-02.md).
- **Trực quan hóa và bảng metrics (COL-03):** Kết xuất bảng số liệu tự động và biểu đồ không gian - thời gian tương đối cho ThanhTris thuộc về [COL-03 - Metrics và trực quan hóa baseline pilot](../task/tasks/COL-03.md).
- **Phân cụm thích ứng (EXP-03):** Thuật toán HDBSCAN adaptive và giải quyết multi-sequence thuộc về [EXP-03 - Xây HDBSCAN adaptive và giải quyết multi-sequence](../task/tasks/EXP-03.md) do ThanhTris phụ trách.
- **Nghiên cứu chính thức toàn kỳ:** Nghiên cứu chính thức phụ thuộc vào dataset catalog thật từ [DAT-03 - Chuẩn bị catalog nghiên cứu thật](../task/tasks/DAT-03.md) và [MLD-05 - Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md); pilot hiện tại giữ nguyên mục đích kiểm chứng kỹ thuật (`DEVELOPMENT_ONLY`).
- **Trạng thái task EXP-02:** Tiếp tục giữ `In Progress` theo quy định (Colab pilot parity đã đạt; chờ dataset chính thức từ DAT-03 để hoàn tất toàn bộ acceptance).
- **Reviewer:** Reviewer độc lập cho task P0 này hiện là `unassigned`.
