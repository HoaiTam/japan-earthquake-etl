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
  - Checksum danh tính mã nguồn thực tế khi đóng gói (`source_sha256`): `7a65c8fa203c143283a07de2678dc9f7fd16569342f6d6f43e5edcfdf102ff4d`.
  - Checksum báo cáo cục bộ (`local_smoke_sha256`): `b68ee4e1606f12349f234c7d2776d12bd14da9627d515c6c1940c5d1f0c2b01a`.
  - Gói Handoff xuất ra: `staging/exp02-local-rosy179/exp01-colab-handoff.zip` (171.993 bytes), SHA-256: `d6be707111be8d2b1a2ba64434d6c0cacaa67b73c5aae3235f9772403c544333`.

### Đối soát danh tính mã nguồn (Source Identity & Per-file Reconciliation)

Hàm `notebooks.colab_runtime.source_identity()` thu thập và tính mã băm SHA-256 trên 16 tệp mã nguồn, notebook và cấu hình. Khi đối soát giữa máy Windows (`rosy179`) và Linux snapshot (`HoaiTam`), phát hiện hai giá trị tổng:
- Bản đóng gói thực tế (Windows): `source_sha256 = 7a65c8fa203c143283a07de2678dc9f7fd16569342f6d6f43e5edcfdf102ff4d` (khóa trong `handoff.json` của `exp01-colab-handoff.zip`).
- Bản chuẩn hóa canonical (Linux / LF): `source_sha256 = d355ef349844900d94b86b56e25af15e228908101cec11ed8c84eb84f822d22a`.

**Bảng đối soát per-file 16 tệp:**

| STT | Đường dẫn tệp | SHA-256 Windows (`7a65c8fa...`) | SHA-256 Linux LF (`d355ef34...`) | Trạng thái |
|:---:|---|---|---|:---:|
| 1 | `config/ml/models/dbscan_global_dev_v1.json` | `ab5c5150eefd58c674937ebc885275f74f96810dc05197d936d94d70fec06c75` | `ab5c5150eefd58c674937ebc885275f74f96810dc05197d936d94d70fec06c75` | Khớp 100% |
| 2 | `config/ml/models/hdbscan_adaptive_dev_v1.json` | `d0f24c8732e47d094ea42af1eb41a9413a17af4760b3fc39864bc31f8d662c73` | `d0f24c8732e47d094ea42af1eb41a9413a17af4760b3fc39864bc31f8d662c73` | Khớp 100% |
| 3 | `config/ml/models/hdbscan_global_dev_v1.json` | `eb8c3e8cf63f1cfa30669094b0bcf3808f8fe780afdf24c4ae795bbbfee99111` | `eb8c3e8cf63f1cfa30669094b0bcf3808f8fe780afdf24c4ae795bbbfee99111` | Khớp 100% |
| 4 | `config/ml/models/window_baseline_v1.json` | `5ba1a7aadb46a00c85058c5d4f0a27812f97bfee877601acfae10168dc454797` | `5ba1a7aadb46a00c85058c5d4f0a27812f97bfee877601acfae10168dc454797` | Khớp 100% |
| 5 | `notebooks/adaptive_hdbscan.py` | `59727c0fdf8a3caa0956c58eacae9a566f5d2cb8b52dedc1ba1ab7f94ebd5277` | `59727c0fdf8a3caa0956c58eacae9a566f5d2cb8b52dedc1ba1ab7f94ebd5277` | Khớp 100% |
| 6 | `notebooks/baseline_models.py` | `81ac29e4b661f3985a12576a37f09570d7f8f0c6d0093950c886e308958d0546` | `81ac29e4b661f3985a12576a37f09570d7f8f0c6d0093950c886e308958d0546` | Khớp 100% |
| 7 | `notebooks/colab_runtime.py` | `a2a532da389bad57f533902797bff6506738faeb20bf13bc1329e84c634304d7` | `a2a532da389bad57f533902797bff6506738faeb20bf13bc1329e84c634304d7` | Khớp 100% |
| 8 | `notebooks/exp_01_colab_bootstrap.ipynb` | `bf84a315395df502ae482c134b4f5b5671abdc284c65e2cb0920dc0214de0b18` | `bf84a315395df502ae482c134b4f5b5671abdc284c65e2cb0920dc0214de0b18` | Khớp 100% |
| 9 | `notebooks/exp_01_portability_local.ipynb` | `81c42bc5e999d6785fcad51881035b96a27f2e44792bdb0a7738082bc328cd59` | `81c42bc5e999d6785fcad51881035b96a27f2e44792bdb0a7738082bc328cd59` | Khớp 100% |
| 10 | `notebooks/experiment_harness.py` | `408004c5e751e4f4b6a8c3c6b504ab43a49baa74e825c87fad9d601e7273b903` | `408004c5e751e4f4b6a8c3c6b504ab43a49baa74e825c87fad9d601e7273b903` | Khớp 100% |
| 11 | `notebooks/pilot_bundle.py` | `29d92e1e6bf085c6c3ef4bd88c16e5e670b2d9ca3ac7fdeea9a8a44ce59f86a7` | `893707d296655c6ce25585f97305d263919e18b6fc87e5ffea49a1f11c79e7e7` | CRLF vs LF |
| 12 | `notebooks/requirements-colab-lock.txt` | `e2a485576704724e52dfd9487ed8aded0bcd5755d2af2bed2c9b1780a9f693d1` | `e2a485576704724e52dfd9487ed8aded0bcd5755d2af2bed2c9b1780a9f693d1` | Khớp 100% |
| 13 | `notebooks/requirements-lock.txt` | `c5838ca3bc61cfdfc3615ab6d5301d0009ea687072b1171bbb6e2e40494e5baf` | `c5838ca3bc61cfdfc3615ab6d5301d0009ea687072b1171bbb6e2e40494e5baf` | Khớp 100% |
| 14 | `scripts/exp01-colab.py` | `1ec7715cf933b1a9e32fc998f6533650a41823f22218ec9ef08732efd7a05988` | `1ec7715cf933b1a9e32fc998f6533650a41823f22218ec9ef08732efd7a05988` | Khớp 100% |
| 15 | `scripts/inspect-exp02-notebook.py` | `9b7842ca6ffa0e8820ef805088b8ede8b1e42c21d8f77c895b0749793f90a616` | `9b7842ca6ffa0e8820ef805088b8ede8b1e42c21d8f77c895b0749793f90a616` | Khớp 100% |
| 16 | `scripts/verify-team-pilot.py` | `98f55b97549cca82f7e6e483644c719d1b2a0cde349ceeccb059d2f0077b3d40` | `98f55b97549cca82f7e6e483644c719d1b2a0cde349ceeccb059d2f0077b3d40` | Khớp 100% |

**Kết luận kỹ thuật:**
- **15/16 tệp mã nguồn và cấu hình giống nhau tuyệt đối từng byte (100% byte identical).**
- Tệp duy nhất có sự khác biệt checksum là `notebooks/pilot_bundle.py` do quá trình checkout tự động chuyển đổi ký tự kết dòng CRLF (`\r\n`) trên Windows sang LF (`\n`) trên Linux. Hoàn toàn không có bất kỳ thay đổi nào về cú pháp, logic hàm, định dạng dữ liệu hay tham số.
- Các cấu hình mô hình (`dbscan_global_dev_v1.json`, `hdbscan_global_dev_v1.json`, `window_baseline_v1.json`) cùng toàn bộ seed ngẫu nhiên (`random_seed=42`) và bộ lọc hạt đều đồng nhất $100\%$ giữa các môi trường.

---

## 3. Thực thi và Đối soát trên Google Colab (`rosy179`)

- **Phiên thực thi đám mây:**
  - Nền tảng: Google Colab (CPU runtime, Linux x86_64).
  - Probe môi trường: `{'module': '/usr/local/lib/python3.13/dist-packages/google/colab/__init__.py', 'system': 'Linux'}`.
  - Runtime Python: Python 3.13, môi trường venv cô lập (`--without-pip`, host pip `--isolated --python`).
  - Notebook sử dụng: [notebook Colab](https://colab.research.google.com/drive/1uIXjbwNQHZ-K1km-HU3ckKXyKCzYSsSq?usp=sharing) (File ID: `1uIXjbwNQHZ-K1km-HU3ckKXyKCzYSsSq`, 67.285 bytes, SHA-256: `188b299d735fe63c6d5d587cee1651ebb8ed48ead4272fd86d16e94b4c6f08eb`, 5 cells có đầy đủ outputs thực thi).
  - Recipient: `rosy179`.
  - Thời điểm hoàn tất (UTC): `2026-11-02T03:31:43.410294+00:00`.
- **Kết quả nghiệm thu tổng thể:**
  - Trạng thái: `COLAB_HARNESS_PASSED`.
  - Toàn bộ 3 thuật toán baseline đều đạt đối soát tuyệt đối: `WINDOW: PASSED`, `DBSCAN: PASSED`, `HDBSCAN_GLOBAL: PASSED`.
- **Gói kết quả xuất ra từ Colab:**
  - File: `exp01-colab-output.zip`
  - Dung lượng tải về: 70.896 bytes
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

Toàn bộ các số liệu và trạng thái trong bảng đối soát được sinh ra từ chính xác các file cấu hình và tham số sau:

1. **Thuật toán `WINDOW` (theo `window_baseline_v1.json`):**
   - Cấu hình: `include_post_only = true`, `post_window_role = "POST"`, `pre_window_role = "PRE"`.
   - Kết quả: Ghi nhận toàn bộ $255$ sau chấn hợp lệ trên 2 cửa sổ ($0$ noise). Cả 2 cửa sổ đều thành công (`successful_windows = 2, failed_windows = 0`). Thuật toán không có cơ chế lọc nhiễu không gian - thời gian.

2. **Thuật toán `DBSCAN` (theo `dbscan_global_dev_v1.json`):**
   - Cấu hình thực tế từ `experiment_config.json`: `eps = 0.5`, `min_samples = 3`, `metric = "euclidean"`.
   - Kết quả:
     - Tại cửa sổ 1: Mainshock thuộc cụm có mật độ đạt chuẩn, phân tách được $9$ sự kiện nhiễu không gian - thời gian (`is_noise = true`), cô lập chuỗi sau chấn $240$ sự kiện (`successful_windows = 1`).
     - Tại cửa sổ 2: Mật độ địa chấn thưa thớt khiến chính sự kiện mainshock mang nhãn `-1` (nhiễu).

3. **Thuật toán `HDBSCAN_GLOBAL` (theo `hdbscan_global_dev_v1.json`):**
   - Cấu hình thực tế từ `experiment_config.json`: `min_cluster_size = 3`, `min_samples = 2`, `allow_single_cluster = true`, `cluster_selection_epsilon = 0.0`.
   - Kết quả: Tại cả 2 cửa sổ, do mật độ phân tán cục bộ của tập pilot 279 dòng, sự kiện mainshock đều rơi vào nhãn `-1` (nhiễu), dẫn tới `failed_windows = 2`, `total_noise = 108` và `total_sequence_members = 0`.

4. **Tuân thủ quy tắc Fail-Safe Contract khi Mainshock rơi vào Noise:**
   - Theo đúng đặc tả hệ thống và `ML_DATA_MODEL.md`: khi sự kiện mainshock mang nhãn `-1`, hệ thống đánh dấu cửa sổ đó là `failed_windows` và ghi nhận `no productive sequence`, tuyệt đối **không tự ý chọn cụm gần nhất để ép thành chuỗi sau chấn nhân tạo**.
   - Việc ghi nhận noise là bằng chứng cho thấy mô hình tuân thủ đúng nguyên lý vật lý và quy chuẩn dữ liệu, không phải lỗi cài đặt hay lỗi mã nguồn.
   - Hiện tượng này chứng minh sự cần thiết của các bước tiếp theo:
     - [COL-02](../task/tasks/COL-02.md): Thực hiện bounded parameter sweep (khảo sát đa dạng các tổ hợp `eps`, `min_samples`, `min_cluster_size`).
     - [EXP-03](../task/tasks/EXP-03.md): Triển khai Adaptive HDBSCAN nhằm thích ứng với biến thiên mật độ không gian và phân tách đa chuỗi.

---

## 6. Danh mục tệp bằng chứng và Kênh chia sẻ nhóm

### Kênh chia sẻ Google Drive nhóm (đối soát độc lập)
Tuân thủ runbook [WEEK_7_COLAB_SETUP.md](../runbooks/WEEK_7_COLAB_SETUP.md), các gói ZIP và notebook đã được đưa lên Google Drive và cấp quyền truy cập để reviewer và nhóm tải về đối chiếu trực tiếp:
- **Thư mục nhóm IE212:** [Google Drive IE212](https://drive.google.com/drive/u/1/folders/17Wntw5LIRvFdzaU0RpnBRL60gzsRdFTB)
- **Tệp Handoff cục bộ:**
  - Tên: `exp01-colab-handoff.zip`
  - Direct Drive URL: [exp01-colab-handoff.zip](https://drive.google.com/file/d/1taAG1gUWhzoYhQKSU7clsI3uK2s1c3vE/view) (Drive File ID: `1taAG1gUWhzoYhQKSU7clsI3uK2s1c3vE`)
  - Dung lượng: 171.993 bytes
  - SHA-256: `d6be707111be8d2b1a2ba64434d6c0cacaa67b73c5aae3235f9772403c544333`
- **Tệp Kết quả Google Colab:**
  - Tên: `exp01-colab-output.zip`
  - Direct Drive URL: [exp01-colab-output.zip](https://drive.google.com/file/d/1YV-A0FeNtcdv2t_zCR507PIpQsW05bog/view) (Drive File ID: `1YV-A0FeNtcdv2t_zCR507PIpQsW05bog`)
  - Dung lượng: 70.896 bytes
  - SHA-256: `df5dae61e79cb707e8136af2b46037b27b5818f0abc89bedb59dab218d4b8665`
- **Google Colab Notebook (đã thực thi có outputs):**
  - Direct Colab URL: [exp_01_colab_bootstrap.ipynb (rosy179)](https://colab.research.google.com/drive/1uIXjbwNQHZ-K1km-HU3ckKXyKCzYSsSq?usp=sharing) (Drive File ID: `1uIXjbwNQHZ-K1km-HU3ckKXyKCzYSsSq`)
  - Dung lượng tải về: 67.285 bytes
  - SHA-256: `188b299d735fe63c6d5d587cee1651ebb8ed48ead4272fd86d16e94b4c6f08eb`
  - Trạng thái kiểm chứng: 5/5 cells thực thi đạt chuẩn, lưu giữ đầy đủ outputs đối soát đám mây.

### Tệp bằng chứng và báo cáo đối soát trong Repository
- Báo cáo đối soát JSON: [EXP-02_COLAB_PARITY_20261102.json](./EXP-02_COLAB_PARITY_20261102.json)
- Gói Handoff cục bộ: `staging/exp02-local-rosy179/exp01-colab-handoff.zip`
- Biên nhận Handoff: `staging/exp02-local-rosy179/handoff-receipt.json`
- Gói kết quả Colab: `staging/exp02-colab-rosy179/` (nguồn gốc từ file download `exp01-colab-output.zip`)
- Báo cáo Cloud: `staging/exp02-colab-rosy179/colab-report.json`
- Biên nhận Pilot người nhận: `staging/exp02-colab-rosy179/pilot-receipt.json`
- Bảng mã băm kết quả Colab: `staging/exp02-colab-rosy179/output-checksums.json`

---

## 7. Giới hạn / Rủi ro / Bước tiếp theo

- **Khảo sát tham số giới hạn (COL-02):** Khảo sát 8 cấu hình sweep mở rộng đã được thực hiện và đẩy lên nhánh `feat/col-02-parameter-sweep` tại [COL-02 - Sweep tham số baseline giới hạn trên pilot](../task/tasks/COL-02.md).
- **Trực quan hóa và bảng metrics (COL-03):** Bảng số liệu 18 hàng metrics và 9 biểu đồ không gian - thời gian đã được hoàn tất trên nhánh `feat/col-03-baseline-metrics-plots` tại [COL-03 - Metrics và trực quan hóa baseline pilot](../task/tasks/COL-03.md).
- **Phân cụm thích ứng (EXP-03):** Thuật toán HDBSCAN adaptive và giải quyết multi-sequence thuộc về [EXP-03 - Xây HDBSCAN adaptive và giải quyết multi-sequence](../task/tasks/EXP-03.md) do ThanhTris phụ trách.
- **Nghiên cứu chính thức toàn kỳ:** Nghiên cứu chính thức phụ thuộc vào dataset catalog thật từ [DAT-03 - Chuẩn bị catalog nghiên cứu thật](../task/tasks/DAT-03.md) và [MLD-05 - Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md); pilot hiện tại giữ nguyên mục đích kiểm chứng kỹ thuật (`DEVELOPMENT_ONLY`).
- **Trạng thái task EXP-02:** Tiếp tục giữ `In Progress` theo quy định (Colab pilot parity đã đạt; chờ dataset chính thức từ DAT-03/MLD-05 để hoàn tất toàn bộ acceptance).
- **Reviewer:** Reviewer độc lập cho task P0 này hiện là `unassigned`.
