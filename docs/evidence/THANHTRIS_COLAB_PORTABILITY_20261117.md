# Bằng chứng Kiểm thử Colab Portability và Pilot Parity — ThanhTris (17/11/2026)

Báo cáo này ghi nhận phiên kiểm thử độc lập của **ThanhTris** cho quy trình portability Jupyter local → Google Colab thực tế trên development pilot **DAT-02** (`DEVELOPMENT_ONLY`, `research_ready=false`, `scientific_approval=false`), nhằm bổ sung artifact/session thực tế cho [SEC-01](../task/tasks/SEC-01.md) (theo yêu cầu trên PR #119) và xác minh điều kiện chuẩn bị môi trường cloud cho [EXP-03](../task/tasks/EXP-03.md).

- **Trạng thái phiên kiểm thử:** `COLAB_PORTABILITY_AND_PILOT_PARITY_VERIFIED`
- **Trạng thái task `EXP-03`:** `Needs Update` (không đánh dấu `Done`; giữ hard gate [EXP-02](../task/tasks/EXP-02.md) official và chưa có official adaptive run trên collection `DAT-03`)
- **Assignee:** ThanhTris | **Reviewer:** unassigned
- **Git base commit chạy `local-pack`:** `001e33b83c8277848c4debc9f6b8c07c29fc68b5`
- **Source identity SHA-256:** `18afd8d6c53ffed894f7a1cc336ae242220bf41f33f6206f562d741c5a427981`
- **Metadata JSON đối soát:** [THANHTRIS_COLAB_PORTABILITY_20261117.json](./THANHTRIS_COLAB_PORTABILITY_20261117.json)

---

## 1. Local Pilot Verification & Gate Trước Khi Lên Colab

### A. Xác thực Development Pilot (`DAT-02`)
- **Pilot ID:** `pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c`
- **Archive ZIP SHA-256:** `f58291b72873e1b26791fef97818fdcf5e2b028a5dfa528885a666d8cdc1b870`
- **Handoff SHA-256:** `9121d6839f9ef8e5646a06cd0bf870fcde50b65da00c03e7b268760237cddc9a`
- **Features Parquet SHA-256:** `3baddf613f237d2117248ad9095230ec17e428ea47f73d499623035d3097d2e6`
- **Checksums Index SHA-256:** `310b5f232ae34ca4f0a197b0181bff81efe6bee38897b6e151e9b58e8d5a6502`
- **Quy mô:** `279` candidate feature rows, `2` mainshock windows (`TEAM_PILOT_VERIFIED`).
- **Receipt SHA-256 (local):** `9218c414955d4e30200a343779f2893b7f7e2240726237d50f82bb699c669dea`

### B. Local Portability Gate (`EXP-01` Harness trên Windows `Python 3.12.10`)
- Thực thi `scripts/exp01-colab.py local-pack` trong virtualenv cô lập (`notebooks/requirements-colab-lock.txt`, `pip check` `PASSED`).
- **Trạng thái:** `LOCAL_GATE_AND_HANDOFF_VERIFIED` (`fresh_kernel_acceptance=true`, `nbclient`, `2` code cells, `25.6583s`, `source_notebook_outputs_cleared=true`).
- **Handoff Archive (`exp01-colab-handoff.zip`):** `172.179` bytes, SHA-256 `a35ec15fb6d5e7bcae68f669aa41c12659aecc8d15378886fc7308fa58304c78`.
- **Handoff Receipt (`handoff-receipt.json`) SHA-256:** `eb80f6942aefb223092365f132a07f3da4f97b69f33b9fa52c7bc70248a6484a`.
- **Local Smoke Report SHA-256:** `8d67054bec43071d3d3d4cd54d44ca98752acfc64818da6148078de9d0b7f9ae`.
- **Baseline local counts (`2` windows / `279` candidates):** `WINDOW` (`255` aftershocks / `2` thành công), `DBSCAN` (`240` aftershocks / `9` noise / `1` thành công / `1` failed), `HDBSCAN_GLOBAL` (`0` aftershocks / `108` noise / `2` failed).

### C. Local Fresh-Kernel Adaptive Smoke (`EXP-03` trên Bundle Pilot Thật)
- Thực thi `scripts/inspect-exp03-notebook.py` bằng `nbclient` (`Python 3.12.10`, `5` code cells, `5.2503s`).
- **Kết quả:** `RESTART_RUN_ALL_PASSED`, `local_smoke_status=PASSED`, `colab_migration_allowed=true`, `experiment_run_id=pilot_run_1a7b6edb9bb94ccf9caaac99e124f2b5`.
- **Inspection report SHA-256:** `e18ba904739f27698b74231a48d97f65232db26e1a5620e08559f67ff56a2643`.

---

## 2. Thực thi Thực tế trên Google Colab (`COLAB_HARNESS_PASSED`)

Notebook thực thi trực tiếp trên Google Colab (`Runtime: CPU`): [exp_01_colab_bootstrap.ipynb](../../notebooks/exp_01_colab_bootstrap.ipynb) (SHA nguồn `5fba9b14481173aecad624042b3a3a3d51b24b2b2d16298e6f1b2808dfd189f1`).

- **Timestamp UTC (`colab-report.json`):** `2026-11-17T02:27:41.532375+00:00`
- **Host Runtime Probe:** `Linux`, `Python 3.13.18`, module `/usr/local/lib/python3.13/dist-packages/google/colab/__init__.py`
- **Isolated venv:** `true`, `pip_check=PASSED`, `108` packages installed (gồm `numpy 2.2.2`, `pandas 2.2.3`, `pyarrow 21.0.0`, `scikit-learn 1.6.1`, `scipy 1.15.1`, `hdbscan 0.8.40`, `matplotlib 3.10.0`, `nbclient 0.10.2`)
- **Executed Notebook (`4` code cells, `0` error outputs):** `67.307` bytes, SHA-256 `e28fb1d77a97a9f39a5e87b8504bd2729ff8e0f76a06ba3a8c7476bd251b9ae0`
- **Output Archive (`exp01-colab-output.zip`):** `70.913` bytes, `25` entries (`24` allowlist files + `output-checksums.json`), SHA-256 `85c2b8769781edb89d4147d4c3def61e38b4864793b706d3a8154c98f0c8077b`
- **Extracted Key JSON Checksums:**
  - `colab-report.json`: `d64d1ba58940f08bb5a5d4d98b59f06d58754efba29c57a07448b720ae42a27c`
  - `output-checksums.json`: `9d9faf9c1170ef2cce4788dbf4fe65dfd097e242f7fde589595363a116fa8746`
  - `pilot-receipt.json`: `2cd0ef71b23b53b90aee52eef13549adb46961a342f1d82731faf7dda25814e5`

### Đối soát Parity giữa Local (`Windows Python 3.12.10`) và Google Colab (`Linux Python 3.13.18`)

| Thuật toán | Memberships | Mainshocks | Tolerance | Trạng thái |
|---|---:|---:|---|---|
| `WINDOW` | `279` | `2` | `rtol=1e-6`, `atol=1e-8` | `PASSED` |
| `DBSCAN` | `279` | `2` | `rtol=1e-6`, `atol=1e-8` | `PASSED` |
| `HDBSCAN_GLOBAL` | `279` | `2` | `rtol=1e-6`, `atol=1e-8` | `PASSED` |

> **Xác minh độc lập trên máy host sau khi tải về:** Đã giải nén `exp01-colab-output.zip`, kiểm chứng schema Parquet/checksums qua `colab_runtime.verify_result()` và chạy lại `colab_runtime.parity()` đối chiếu từng file kết quả với `local-pack-v1/baseline` — cả 3 thuật toán đều `PASSED`.

---

## 3. Handoff lên Shared Google Drive & Kiểm tra Bảo mật (`SEC-01`)

- **Folder Drive đích:** `dat03/workers/ThanhTris/ThanhTris-colab-20261117` — [Drive folder](https://drive.google.com/drive/folders/1jjyh_FfpIlcPb6y5n58Ssi7q0X0Ns6zb)
- **Kiểm toán ACL (`gdrive readonly audit-permissions` & `permissions`):** Đã kiểm tra folder và `4` artifact bên trong (`5` objects tổng cộng); **100% quyền có `type=user` (`anyone=0`, `domain=0`)**. Không tải lên `.env`, OAuth token, private log hoặc đường dẫn tài khoản cá nhân; email và permission ID được giữ kín khỏi tài liệu repo.

| Artifact trên Drive | Drive ID | Kích thước | SHA-256 | Liên kết |
|---|---|---:|---|---|
| `ThanhTris_exp_01_colab_bootstrap.ipynb` | `1Jb9uZufANHdsnn5ZxpayWyIwLIvQoeaF` | `67.307` B | `e28fb1d77a97a9f39a5e87b8504bd2729ff8e0f76a06ba3a8c7476bd251b9ae0` | [Colab Notebook](https://colab.research.google.com/drive/1Jb9uZufANHdsnn5ZxpayWyIwLIvQoeaF) |
| `exp01-colab-handoff.zip` | `1v-x9FrvDWtQctlmz8JugB1HGwjz1w8BV` | `172.179` B | `a35ec15fb6d5e7bcae68f669aa41c12659aecc8d15378886fc7308fa58304c78` | [Drive File](https://drive.google.com/file/d/1v-x9FrvDWtQctlmz8JugB1HGwjz1w8BV/view?usp=drivesdk) |
| `exp01-colab-output.zip` | `1sD_UJOeBztHTZNDcrg2zuNua1UxuRLau` | `70.913` B | `85c2b8769781edb89d4147d4c3def61e38b4864793b706d3a8154c98f0c8077b` | [Drive File](https://drive.google.com/file/d/1sD_UJOeBztHTZNDcrg2zuNua1UxuRLau/view?usp=drivesdk) |
| `handoff-receipt.json` | `1bv5UKh7NSh86YfBpMV-U8jGJxUt8aVAW` | `1.366` B | `eb80f6942aefb223092365f132a07f3da4f97b69f33b9fa52c7bc70248a6484a` | [Drive File](https://drive.google.com/file/d/1bv5UKh7NSh86YfBpMV-U8jGJxUt8aVAW/view?usp=drivesdk) |

---

## 4. Phạm vi, Giới hạn và Bước tiếp theo

1. Phiên Colab này chứng minh **portability của môi trường Google Colab (`ThanhTris`)**, parity **pilot baseline (`WINDOW`/`DBSCAN`/`HDBSCAN_GLOBAL`)**, kiểm tra **local `EXP-03` adaptive smoke trên real pilot**, và hoàn tất artifact/session phía **ThanhTris** cho [SEC-01](../task/tasks/SEC-01.md) (PR #119).
2. Không sử dụng kết quả pilot để suy diễn chất lượng khoa học hay dự đoán động đất / quan hệ nhân quả vật lý (`aftershock candidate hồi cứu`).
3. [EXP-03](../task/tasks/EXP-03.md) giữ nguyên trạng thái **`Needs Update`** vì còn thiếu:
   - Official baseline reproduction run trên `DAT-03` `REPRODUCTION` `EXPORTED` thuộc [EXP-02 - Chạy Window, DBSCAN và HDBSCAN global](../task/tasks/EXP-02.md).
   - Official adaptive HDBSCAN reproduction bundle trên `DAT-03` `REPRODUCTION` `EXPORTED` thuộc [EXP-03 - Xây HDBSCAN adaptive và giải quyết multi-sequence](../task/tasks/EXP-03.md).
   - Nghiệm thu sạch ACL/roster tổng thể tại [SEC-01 - Kiểm toán bảo mật Drive/Colab](../task/tasks/SEC-01.md) (PR #119).
