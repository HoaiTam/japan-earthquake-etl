# Roadmap HDBSCAN từ Gold đến ML Iceberg

Tài liệu này chuyển phương án nghiên cứu HDBSCAN thành các khối công việc có
thể giao độc lập. File task riêng trong [`tasks/`](./tasks/README.md) là nguồn
theo dõi chính cho dependency, acceptance criteria, assignee và evidence.

## 1. Mục tiêu đã chốt

Pipeline nhận `gold.earthquake_event_current` tại một Iceberg snapshot cố định,
tạo candidate theo từng mainshock, xây feature không–thời gian 4-D, so sánh
Window/DBSCAN/HDBSCAN và nhập kết quả đã kiểm tra về namespace `ml`.

Kết quả là **sequence/aftershock candidate theo mô hình thống kê**. Đây là
clustering hồi cứu, không dự đoán trận động đất tiếp theo và không chứng minh
quan hệ nhân quả vật lý.

```mermaid
flowchart LR
    G["Gold snapshot"] --> D["ML dataset<br/>audit · Mc · windows · features"]
    D --> E["Parquet export<br/>manifest · checksum"]
    E --> C["Jupyter local → Colab<br/>Window · DBSCAN · HDBSCAN"]
    C --> R["Result bundle<br/>membership · summary · metrics"]
    R --> I["Airflow import<br/>schema · grain · lineage gates"]
    I --> M["Iceberg ml.*<br/>Trino/static report"]
```

## 2. Ranh giới dữ liệu

- Gold core giữ canonical event và không nhận `cluster_id` thử nghiệm.
- Mọi dataset phải pin `gold_snapshot_id` và có `dataset_id` bất biến.
- Mọi kết quả phải có `experiment_run_id`, `model_config_version`, checksum và
  code/notebook/package version.
- Feature baseline là `[x_scaled, y_scaled, z_scaled, time_scaled]`; không dùng
  latitude/longitude degree trực tiếp và không cộng kilomet với giờ chưa scale.
- HDBSCAN chạy theo từng mainshock window, không fit một lần trên toàn catalog.
- Colab không nhận credential MinIO. Export/result trao đổi qua bundle có
  manifest; pipeline local mới được ghi Iceberg/MinIO.

## 3. Phạm vi Core và Stretch

Core phải có:

- reproduction `2000-01-01` đến hết `2018-09-30` và extension `2018-10-01`
  đến hết `2023-12-31`;
- mainshock `50–200 km`, magnitude `> 5.5`, catalog era `UNIFIED`, primary JMA;
- phương pháp/version `Mc`, candidate window và scaling;
- Window, DBSCAN, HDBSCAN global và HDBSCAN adaptive dùng cùng dataset;
- DBCV/noise/membership, Omori consistency, sensitivity và out-of-period;
- result bundle, import gate, Iceberg `ml.*` và static report.

Power BI, shallow control group, OPTICS và tối ưu mở rộng là Stretch. Power BI
không còn là hard dependency của MVP HDBSCAN.

## 4. Các khối có thể chia người

| Khối | Task | Output độc lập | Có thể bắt đầu bằng |
|---|---|---|---|
| Contract/pilot | `PLN-01`, `CON-03`, `DAT-02` | Scope/DoD và logical schema ML | Tài liệu phương án, schema mẫu |
| Dataset | `MLD-01..05` | Dataset manifest, Mc, windows, features, export | Gold fixture/snapshot metadata giả lập |
| Experiment | `EXP-01..05` | Notebook, assignments, metrics, sensitivity/extension | Feature Parquet fixture nhỏ |
| Integration | `MLI-01..04` | Bundle validator, hai DAG, Iceberg tables, report | Result bundle fixture hợp lệ/lỗi |
| ML QA | `MLQ-01` | Evidence Gold snapshot → report | Output thật của các khối trên |

Ba luồng Dataset, Experiment và Integration không cần chờ nhau để viết
interface/test. Chỉ `MLQ-01` là integration gate buộc phải dùng output thật.

Trong [kế hoạch tuần 4 mở rộng](./WEEK_4_PARALLEL_PLAN.md), MLD-01..03 và
MLI-01 được đưa lên tuần 4: Trang pin snapshot/result contract, ThanhTris audit/
Mc/window pilot. JMA-05 trong [nhóm mở đường](./WEEK_4_PREP_GROUP.md) bổ sung
sample reproduction 2000, không tune reproduction bằng sample 2023 extension.
Kế hoạch tuần 4 ở trên là mốc lịch sử. [Tuần 5](./WEEK_5_PARALLEL_PLAN.md),
đồng bộ 22/10 theo phân công gần nhất, giao MLD-04/05/EXP-01/02/03 và MLI-02
cho rosy179, MLI-03/QA-02/03/04 cho ThanhTris; HoaiTam giữ ORC-06/07,
DAT-02/QA-01/DOC-01. Tổng 15 Core task/86h, gồm 11 task giao việc + 4 mở đường;
không thêm task/effort vào backlog 76 task/386h.
DAT-02 giao HoaiTam (6h), tạo development bundle nhỏ; EXP-01 local harness
phụ thuộc DAT-02/CON-03, không đợi official export MLD-05. EXP-02 local smoke
PASSED trước Colab; official acceptance vẫn cần dataset MLD-05 EXPORTED hợp lệ.
QA-04 từng dời tuần 6, nay đưa lại tuần 5 và Done với runtime evidence riêng.
MLD-01..03 không tự chuyển dataset thành EXPORTED khi feature/
export chưa đạt, và pilot theo vài năm không đại diện toàn research period.

## 5. Dependency chính

```mermaid
flowchart LR
    C03["CON-03 update"] --> M1["MLD-01 manifest"]
    G4["GLD-04 Gold verified"] --> M1
    M1 --> M2["MLD-02 audit + Mc"]
    M1 --> M3["MLD-03 mainshock + windows"]
    M2 --> M3
    M3 --> M4["MLD-04 4-D features"]
    M4 --> M5["MLD-05 validate + export"]

    M4 --> P["DAT-02 development pilot"]
    P --> E1["EXP-01 Jupyter harness"]
    P --> E2["EXP-02 local baselines → Colab"]
    E1 --> E2
    M5 --> E2
    E2 --> E3["EXP-03 adaptive"]
    E2 --> E4["EXP-04 evaluation"]
    E3 --> E4
    E4 --> E5["EXP-05 extension"]

    I1["MLI-01 result contract"] --> I3["MLI-03 import DAG"]
    M5 --> I2["MLI-02 build DAG"]
    I3 --> I4["MLI-04 tables/report"]
    M5 --> Q["MLQ-01 E2E"]
    E5 --> Q
    I2 --> Q
    I4 --> Q
```

EXP-01 và MLI-01 viết fixture/interface trước MLD-05; DAT-02 không có
hard dependency EXP-01/02, tránh vòng. [Jupyter workflow](../specs/JUPYTER_PILOT_WORKFLOW.md)
phân biệt development pilot và official dataset/result; local PASSED không
thay scientific APPROVED hoặc nâng BUILDING thành EXPORTED.

## 6. Task cũ bị tác động

| Task | Trạng thái mới | Lý do |
|---|---|---|
| `PLN-01` | `Done` | Baseline đã đưa HDBSCAN vào Core, Power BI sang Stretch và chốt lại KPI/DoD |
| `CON-03` | `Done` | Đã khóa grain/schema/lifecycle dataset, experiment và publication gate `ml.*` |
| `GLD-01/03/04` | `Done` theo task index hiện hành | Gold đã có component snapshot/Trino acceptance; không thay E2E toàn chuỗi |
| `GLD-02` | `Backlog`, chuyển Stretch | Aggregate dashboard không nằm trên đường găng HDBSCAN |
| `BI-01..05` | `Backlog`, chuyển Stretch | Power BI là output tùy chọn sau static report |
| `QA/SEC/DOC/DEMO` | Theo task index hiện hành, không suy trạng thái từ bảng tác động này | Phải bao phủ external bundle, import gate và ML reproducibility |

Các task Foundation và USGS Bronze đã hoàn tất vẫn giữ `Done` vì phương án mới
không làm mất evidence đã kiểm chứng. JMA, Silver, Gold và các khối downstream
giữ trạng thái riêng trong task index; không được suy ra `Done` từ bảng tác động
này.

ORC-06/07 mới sở hữu full-month state/real adapters; QA-01 nghiệm thu ETL thật,
không tự triển khai các phần này trong giờ QA. Pilot MLD-02/03 vẫn
BUILDING/research_ready=false; MLD-05 chỉ export lifecycle khi coverage/audit
đủ điều kiện. QA-03 báo coverage còn thiếu; EXP-03/04/05 và MLI-04/MLQ-01
còn phải làm adaptive/evaluation/extension/report/ML E2E, không suy Done từ lịch giao việc.

## 7. Definition of Done cấp workstream

- Một Gold snapshot được pin và truy vết trong `ml.dataset_manifest`.
- Dataset/experiment lifecycle được ghi qua `ml.dataset_manifest` và
  `ml.experiment_run`; import thành công chỉ tạo `CANDIDATE`, không tự approve.
- `Mc`, window, feature/scaling và model config đều có version.
- Mỗi candidate có grain `(dataset_id, mainshock_event_id, candidate_event_id)`.
- Window/DBSCAN/HDBSCAN đọc cùng `dataset_id`; cluster của mainshock được chọn
  bằng rule xác định và mainshock noise không bị thay bằng cluster gần nhất.
- Có DBCV/noise, stability, Omori consistency và out-of-period evidence.
- Result bundle có schema, config, package lock, checksum và `_SUCCESS.json`.
- Airflow reject checksum/schema/grain/lineage sai trước khi commit `ml.*`.
- Báo cáo dùng thuật ngữ candidate và nêu rõ limitation không dự đoán/causal.
