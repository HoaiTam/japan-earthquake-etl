# DEMO-01 — Gói Dữ liệu, Kịch bản Trình bày và Preflight Checklist

Tài liệu triển khai cho [DEMO-01](../task/tasks/DEMO-01.md) (trạng thái hiện hành: **In Progress**, chờ hoàn tất [MLI-04](../task/tasks/MLI-04.md) và [MLQ-01](../task/tasks/MLQ-01.md) để chốt số liệu official trước khi chuyển `Done`). Kịch bản này tuân thủ [hướng dẫn chọn evidence](../DEMO_FLOWS.md) và [các giới hạn hiện hành](../KNOWN_ISSUES.md).

> [!IMPORTANT]
> **Nguyên tắc bắt buộc khi trình bày:**
> 1. **Không chạy lại ETL 40 năm hay build/pull container trên sân khấu.** Dùng exact `run_id`, `publication_id`, 6 `snapshot_id`, truy vấn SQL và static report đã ghim trong [demo_package_manifest_v1.json](../../config/demo/demo_package_manifest_v1.json).
> 2. **Ranh giới khoa học:** Mọi kết quả phân cụm là **sequence/aftershock candidate hồi cứu**, tuyệt đối **không tuyên bố dự đoán động đất** hay quan hệ nhân quả vật lý; không dùng `accuracy` / `f1`; giữ nguyên cửa sổ lỗi (`failed_windows`) trong mẫu số.
> 3. **Không phụ thuộc Power BI:** Đầu ra Core là bảng Iceberg qua Trino SQL và báo cáo tĩnh (`MLI-04`); Power BI (`BI-01..BI-05`) chỉ là Stretch.
> 4. **Phân biệt rõ nhãn phạm vi:** Không đánh đồng dữ liệu pilot (`DEVELOPMENT_ONLY`, `research_ready=false`) hay QA bounded (`PROVISIONAL`) với official `APPROVED`.

---

## 1. Bằng chứng đại diện đã ghim (Verified Evidence Pins)

Toàn bộ con số trình bày đều gắn với `run_id` / `publication_id` / tệp đối soát cụ thể trong kho mã nguồn:

| Tầng / Chủ đề | Nhãn phạm vi | Run ID / Dataset ID / Pin chính | Số liệu đại diện đã kiểm chứng | Nguồn đối soát |
|---|---|---|---|---|
| Daily operational v2 (`QA-01`) | `OBSERVED_AS_OF` / `PROVISIONAL` (`research_ready=false`) | Run: `scheduled__2026-10-23T00:15:00+00:00`<br>Pub: `pub_744bb52905e31c0a7d59250333c50178048156fc3992ab87a47865e9cdab132f` | **8/8** task success, **58** events, **6** Iceberg snapshots, **42** SQL readbacks PASSED | [QA-01 acceptance](../evidence/QA-01_CURRENT_MONTH_ACCEPTANCE.md), [QA-01 JSON](../evidence/QA-01_CURRENT_MONTH_20261023.json) |
| Bounded real ETL (`ORC-07`) | Bounded real (`DAG.test`, tháng `2023-09`) | Run: `manual__2026-10-16T02:26:22.739029+00:00`<br>Pub: `pub_c76a33780d848e918a910f025cefc93c0e179cf67c8a230ab4522c339a95ac8f` | **22.351** JMA + **148** USGS (13 overlap) → **22.390** canonical events, **177** source-link rows, `balanced=true` | [ORC-07 evidence](../evidence/ORC-07.md) |
| Rerun & revision (`QA-02`) | Bounded runtime QA | Run: `qa02-runtime-1868715b4bb34f5a84b568c3cf64c1b6` | **8/8** cases PASS; **6** publications Trino readback, không duplicate khi rerun/late update | [QA-02 runtime](../evidence/QA-02_RUNTIME_ACCEPTANCE.md), [QA-02 JSON](../evidence/QA-02-runtime.json) |
| Historical backfill (`QA-03`) | Bounded runtime QA | Run: `qa03-runtime-2bf249ede8a84947afb22c48beb77e07` | **6/6** cases PASS (`h199701`, `h199710`, `h2000`, `h2023`, `usgs-2023-09`) | [QA-03 runtime](../evidence/QA-03_RUNTIME_ACCEPTANCE.md), [QA-03 JSON](../evidence/QA-03-runtime.json) |
| Failure & recovery (`QA-04`) | Bounded runtime QA | Run: `qa04-runtime-9504f2f71b884df0abb78ce50d4e7490` | **10/10** fault injection cases PASS; giữ exact lease/journal khi lỗi | [QA-04 runtime](../evidence/QA-04_RUNTIME_ACCEPTANCE.md), [QA-04 JSON](../evidence/QA-04-runtime.json) |
| Gold verify SQL (`GLD-04`) | Sample QA bundle | Pub: `pub_d03dad056f8ea9b4af290ddf92009042d123bf9db0512e549ca0e5e180449b9f` | Đủ kiểm tra 6 snapshot `$refs`, provenance bridge, UTC/JST và serving count (`16` rows mẫu) | [GLD-04 SQL](../evidence/GLD-04-verification.sql) |
| ML Pilot dataset (`DAT-02`) | `DEVELOPMENT_ONLY` (`PARTIAL`, `research_ready=false`) | Pilot: `pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c` | **279** feature rows / **2** mainshocks; ZIP SHA `f58291b72873e1b26791fef97818fdcf5e2b028a5dfa528885a666d8cdc1b870` | [DAT-03 team pilot JSON](../evidence/DAT-03_TEAM_RELEASE.json) |
| Baseline parity (`EXP-02`) | `DEVELOPMENT_ONLY` (Local ↔ Colab parity) | Status: `COLAB_PILOT_PARITY_VERIFIED` | `WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL` đồng bộ seed `42`, fresh `nbclient` kernel | [EXP-02 parity](../evidence/EXP-02_COLAB_PARITY_20261102.md), [EXP-02 JSON](../evidence/EXP-02_COLAB_PARITY_20261102.json) |
| Adaptive & multi-seq (`EXP-03`) | `DEVELOPMENT_ONLY` (local smoke) | Run: `pilot_run_cfe24f4de7704819ac1c6116360e1aae` | `RESTART_RUN_ALL_PASSED` via `nbclient` (`8.41s`), order-invariant tie resolution + `AMBIGUOUS` | [EXP-03 evidence](../evidence/EXP-03.md), [EXP-03 smoke JSON](../evidence/EXP-03-local-smoke.json) |
| Scientific eval & report (`EXP-04`, `MLI-04`) | `DEVELOPMENT_ONLY` (`PENDING OFFICIAL IMPORT`) | Script: [generate-experiment-report.py](../../scripts/generate-experiment-report.py) | Bảng 4 thuật toán (`DBCV`, noise, Modified Omori `p`, `Jaccard`/`ARI`, failure reasons) + 7 câu kiểm chứng ML SQL | [EXP-04 pilot](../evidence/EXP-04_PILOT_EVALUATION.md), [MLI-04 report](../evidence/MLI-04_STATIC_REPORT.md), [ML SQL](../../sql/ml/ml_verification_queries.sql) |

### Phần còn `PENDING` theo hard dependency (sẽ cập nhật khi hợp nhất chính thức)

| Cổng chờ (`Pending Gate`) | Lý do hiện hành | Tài liệu liên quan |
|---|---|---|
| `DAT-03` | Còn chờ đủ 3 receipt máy thật cho release v4 và hoàn tất official catalog/export | [DAT-03](../task/tasks/DAT-03.md) |
| `EXP-02` official | Chưa có official reproduction run trên `REPRODUCTION` `EXPORTED` | [EXP-02](../task/tasks/EXP-02.md) |
| `EXP-03` official + `SEC-01` Colab | Chưa có official adaptive reproduction bundle và Colab session thực cho `EXP-03` | [EXP-03](../task/tasks/EXP-03.md), [SEC-01](../task/tasks/SEC-01.md) |
| `EXP-04` official | Chưa có đánh giá chính thức và freeze rule trước `EXP-05` | [EXP-04](../task/tasks/EXP-04.md) |
| `MLI-04` official | Chưa đối soát count parity và SQL readback trên bảng `iceberg.ml.*` đã import thật | [MLI-04](../task/tasks/MLI-04.md) |
| `MLQ-01` | Chưa nghiệm thu E2E từ Gold snapshot → ML export → experiment → `ml.*` → static report | [MLQ-01](../task/tasks/MLQ-01.md) |

---

## 2. Thứ tự trình bày đề xuất (15–20 phút)

| Phần | Thời lượng | Nội dung trình bày | Artifact / Lệnh đối chiếu trực tiếp |
|---|---:|---|---|
| 1 | 2 phút | Bài toán, kiến trúc Lakehouse (`Bronze → Silver → Gold → ML`), ranh giới **aftershock candidate hồi cứu** | [SYSTEM_ARCHITECTURE.md](../SYSTEM_ARCHITECTURE.md), [MVP_SCOPE_KPI_AND_DOD.md](../specs/MVP_SCOPE_KPI_AND_DOD.md) |
| 2 | 3 phút | Thu thập hai nguồn (USGS + JMA) và Bronze bất biến (`manifest.json`, `SHA-256`, không sửa payload sau ghi) | [QA-01 JSON](../evidence/QA-01_CURRENT_MONTH_20261023.json), [ORC-07 evidence](../evidence/ORC-07.md) |
| 3 | 3 phút | Silver chuẩn hóa/revision/linking (`JMA + USGS`), Gold **6 bảng Iceberg** + Trino verification trước `Published` | [GLD-04 SQL](../evidence/GLD-04-verification.sql), [gold_aggregate_verification_queries.sql](../../sql/gold/gold_aggregate_verification_queries.sql) |
| 4 | 3 phút | Rerun, idempotency, backfill & recovery theo tầng (`QA-02/03/04`): giữ nguyên lease/journal khi fail, không `docker compose down -v` | [QA-02 runtime](../evidence/QA-02_RUNTIME_ACCEPTANCE.md), [QA-03 runtime](../evidence/QA-03_RUNTIME_ACCEPTANCE.md), [QA-04 runtime](../evidence/QA-04_RUNTIME_ACCEPTANCE.md) |
| 5 | 4 phút | ML Pipeline: pin Gold snapshot → `mainshock_window` + 4-D feature (`MLD-05`/`MLI-02`) → `WINDOW`/`DBSCAN`/`HDBSCAN_GLOBAL`/`HDBSCAN_ADAPTIVE` + giải quyết đa chuỗi (`AMBIGUOUS`) | [MLD-05](../evidence/MLD-05_RUNTIME_ACCEPTANCE.md), [EXP-02 parity](../evidence/EXP-02_COLAB_PARITY_20261102.md), [EXP-03 smoke](../evidence/EXP-03-local-smoke.json) |
| 6 | 3 phút | Đánh giá khoa học (`DBCV`, noise, Omori `p`, `Jaccard`/`ARI`, denominator integrity), **Static Report** và **7 ML Trino SQL checks** (không phụ thuộc Power BI) | [EXP-04 pilot](../evidence/EXP-04_PILOT_EVALUATION.md), [MLI-04 report](../evidence/MLI-04_STATIC_REPORT.md), [ml_verification_queries.sql](../../sql/ml/ml_verification_queries.sql) |
| 7 | 2 phút | *(Nếu còn thời gian / Stretch)*: Power BI Desktop Import đọc qua Trino `publication_status` đã ghim + Hỏi đáp giới hạn | [DEMO_FLOWS.md](../DEMO_FLOWS.md), [KNOWN_ISSUES.md](../KNOWN_ISSUES.md) |

---

## 3. Preflight Checklist trước giờ demo

### A. Kiểm tra tự động offline (chạy trên máy demo, không cần mạng/Docker)

```bash
python -B scripts/demo-preflight.py --json
```

Nếu máy có sẵn receipt/bundle v4 cục bộ ngoài repository (ví dụ trên Windows của thành viên), có thể truyền thêm cờ tùy chọn để kiểm tra chéo tình trạng tệp cục bộ:

```powershell
python -B scripts/demo-preflight.py --local-receipt C:\Users\ADMIN\dat03-v4\ThanhTris-research-receipt.json --json
```

Điều kiện preflight đạt:
- Exit code `0`, `overall_status = "PASS_WITH_PENDING_GATES"` (hoặc `"PASS_OFFICIAL_READY"` sau khi `pending_official_gates` trống).
- `depends_on_power_bi = false`, `stage_40_year_rerun_required = false`.
- Không phát hiện chuỗi secret/credential hoặc tệp minh chứng bị thiếu/lệch `run_id`/`snapshot_id`.

### B. Checklist vận hành nhanh

- [ ] **24 giờ trước demo:** Chạy `python -B scripts/demo-preflight.py` và `python -m unittest tests/test_demo_preflight.py -v` đạt 100%.
- [ ] **1 giờ trước demo:** Mở sẵn các tài liệu Markdown/JSON/SQL trong repo (`docs/evidence/MLI-04_STATIC_REPORT.md`, `sql/ml/ml_verification_queries.sql`, `docs/evidence/GLD-04-verification.sql`) ở tab riêng để chuyển nhanh khi cần.
- [ ] **5 phút trước demo:** Đảm bảo không mở file `.env`, private log chứa credential hoặc key cá nhân trên màn hình chia sẻ; không bật/trigger DAG backfill dài.

---

## 4. Phương án Fallback khi gặp sự cố (Bắt buộc gắn nhãn rõ)

| Tình huống sự cố | Nhãn công bố khi demo | Hành động thay thế ngay tại chỗ | Tuyệt đối KHÔNG làm |
|---|---|---|---|
| **Mất mạng / USGS hoặc JMA API timeout** | `PINNED_REPLAY_EVIDENCE` | Trình bày trực tiếp bộ JSON/log đã ghim từ `QA-01` (`scheduled__2026-10-23T00:15:00+00:00`) và `QA-03` (`qa03-runtime-2bf249ede8a84947afb22c48beb77e07`) + mô tả cơ chế replay từ Bronze bất biến. | Không gọi run đã lưu sẵn là live fetch đang chạy; không retry mù làm kẹt lease. |
| **Google Drive / Colab chậm hoặc mất kết nối** | `LOCAL_VERIFIED_FALLBACK` | Mở evidence fresh-kernel local (`EXP-03-local-smoke.json` chạy bằng `nbclient`), bảng đối chiếu `EXP-02_COLAB_PARITY_20261102.md` và receipt `TEAM_RESEARCH_VERIFIED` của kit v4 nằm sẵn ngoài repo. | Không giả lập kết quả cloud mới hoặc chỉnh sửa thủ công JSON receipt. |
| **Docker chưa bật / thiếu RAM / image stale** | `STATIC_EVIDENCE_NO_STAGE_ETL` | Dùng trực tiếp `GLD-04-verification.sql`, `ml_verification_queries.sql`, `MLI-04_STATIC_REPORT.md` và `QA-01..QA-04` JSON đã kiểm chứng sẵn. | Không chạy `make docker-build` hay `docker compose down -v` ngay giữa buổi trình bày. |
| **Power BI Desktop / ODBC gặp trục trặc** | `CORE_ONLY_NO_POWER_BI_DEPENDENCY` | Chuyển hoàn toàn sang Trino SQL verification views (`sql/ml/v_*.sql`, `sql/gold/v_*.sql`) và [MLI-04_STATIC_REPORT.md](../evidence/MLI-04_STATIC_REPORT.md); nhắc rõ Power BI chỉ là phạm vi Stretch. | Không coi sự cố Power BI là lỗi hỏng Core pipeline. |
