# MLI-02 - ML Dataset Build Orchestration Runbook

## 1. Phạm vi

`mli_02_build_dataset` điều phối toàn bộ chuỗi xử lý từ Gold snapshot đã `Published` đến export bundle độc lập để phục vụ thí nghiệm ML (Window, DBSCAN, HDBSCAN global và adaptive).

Quy trình gồm 10 task groups/tasks tuần tự nghiêm ngặt:

```text
wait_gold_published → resolve_gold_snapshot → audit_gold_input → resolve_mc
→ select_mainshocks → build_candidate_windows → build_spacetime_features
→ validate_feature_snapshot → export_parquet → mark_dataset_exported
```

### Nguyên tắc và ranh giới

1. **Khóa Gold snapshot:** Chỉ chạy trên snapshot Gold đã `Published` (`published=true`, `verify_status=PASSED`). Không resolve lại bảng current.
2. **Identity xác định:** `dataset_id` là bất biến và dẫn xuất xác định từ canonical identity payload:
   `dataset_id = "ds_" + sha256(canonical_identity_payload)`.
   Rerun cùng cấu hình cho cùng `dataset_id`; khác payload thì bị từ chối `DS_IDENTITY_CONFLICT`.
3. **Attempt isolation:** Mỗi lần xuất ghi vào thư mục attempt riêng (`export_attempts/attempt_<N>`) trước khi promote vào `ml-export/dataset_id=<dataset_id>/`. Retry không bao giờ trộn lẫn các file part `features-part-*.parquet`.
4. **Fail-closed trước `DATASET_EXPORTED`:** Nếu bất kỳ bước nào thất bại (audit không đạt, Mc không hội tụ, feature non-finite, checksum mismatch), pipeline dừng ngay. Dataset giữ trạng thái `BUILDING` hoặc `REJECTED`, không bao giờ đánh dấu `EXPORTED`.
5. **Handoff Colab thủ công (Không tự động trigger):** Colab là môi trường ngoài; DAG tuyệt đối không tự trigger Colab như một cloud job service giả định. Bundle sau khi xuất được chuẩn bị tại local staging/export root để operator kiểm tra checksum và chuyển thủ công lên Google Drive. Không chứa credential trong log, config hay artifact.

---

## 2. Request cấu hình

### 2.1. Reproduction split (`2000-01-01` đến `2018-10-01`)

```json
{
  "conf": {
    "dataset_split": "REPRODUCTION",
    "period_start_utc": "2000-01-01T00:00:00Z",
    "period_end_utc_exclusive": "2018-10-01T00:00:00Z",
    "observation_cutoff_utc": "2026-10-11T00:00:00Z",
    "filter_config": {
      "is_natural_earthquake": true,
      "is_in_study_area": true,
      "canonical_source_system": "JMA_BULLETIN",
      "catalog_era": "UNIFIED"
    },
    "dataset_config_version": "dataset-identity-v1",
    "audit_rule_version": "gold-audit-v1",
    "mc_method_version": "maximum-curvature-v1",
    "window_model_version": "window-model-v1",
    "feature_version": "relative-4d-v1",
    "scaling_config_version": "scaling-v1",
    "code_version": "56b0cf464c7492f27eb1e75e0bd9b43a31b77dcd",
    "publication_reference": {
      "gold_publication_id": "pub_d03dad056f8ea9b4af290ddf92009042d123bf9db0512e549ca0e5e180449b9f",
      "gold_snapshot_id": 1901025108914282099,
      "gold_table_name": "iceberg.gold.earthquake_event_current",
      "physical_gold_table_name": "iceberg.gold.event_current",
      "publication_bundle_sha256": "d03dad056f8ea9b4af290ddf92009042d123bf9db0512e549ca0e5e180449b9f",
      "verify_status": "PASSED",
      "published": true
    }
  }
}
```

### 2.2. Extension split (`2018-10-01` đến `2024-01-01`)

```json
{
  "conf": {
    "dataset_split": "EXTENSION",
    "period_start_utc": "2018-10-01T00:00:00Z",
    "period_end_utc_exclusive": "2024-01-01T00:00:00Z",
    "observation_cutoff_utc": "2026-10-11T00:00:00Z",
    "filter_config": {
      "is_natural_earthquake": true,
      "is_in_study_area": true,
      "canonical_source_system": "JMA_BULLETIN",
      "catalog_era": "UNIFIED"
    },
    "dataset_config_version": "dataset-identity-v1",
    "audit_rule_version": "gold-audit-v1",
    "mc_method_version": "maximum-curvature-v1",
    "window_model_version": "window-model-v1",
    "feature_version": "relative-4d-v1",
    "scaling_config_version": "scaling-v1",
    "code_version": "56b0cf464c7492f27eb1e75e0bd9b43a31b77dcd",
    "publication_reference": {
      "gold_publication_id": "pub_d03dad056f8ea9b4af290ddf92009042d123bf9db0512e549ca0e5e180449b9f",
      "gold_snapshot_id": 1901025108914282099,
      "gold_table_name": "iceberg.gold.earthquake_event_current",
      "physical_gold_table_name": "iceberg.gold.event_current",
      "publication_bundle_sha256": "d03dad056f8ea9b4af290ddf92009042d123bf9db0512e549ca0e5e180449b9f",
      "verify_status": "PASSED",
      "published": true
    }
  }
}
```

---

## 3. Cấu trúc Bundle xuất xưởng

Khi hoàn tất bước `mark_dataset_exported`, thư mục đích chứa layout sau:

```text
/opt/pipeline/staging/ml-export/dataset_id=<dataset_id>/
├── features-part-00000.parquet
├── dataset_manifest.json
└── checksums.sha256
```

Đồng thời, manifest cũng được ghi vào registry dùng chung:
`/opt/pipeline/staging/ml-artifacts/<dataset_id>.json`.

### Kiểm tra tính toàn vẹn (Checksum Verification)

Operator có thể đối soát checksum cục bộ trước khi bàn giao:

```bash
cd /opt/pipeline/staging/ml-export/dataset_id=<dataset_id>/
sha256sum -c checksums.sha256
```

Kết quả hợp lệ:
```text
features-part-00000.parquet: OK
dataset_manifest.json: OK
```

---

## 4. Handoff thủ công lên Google Drive cho Colab

Do Google Colab miễn phí không có quyền truy cập trực tiếp mạng nội bộ MinIO/Trino và không được phép lưu trữ credentials bí mật, việc chuyển giao bundle được thực hiện thủ công theo các bước:

1. **Xác nhận tính sẵn sàng trên DAG UI / Run Summary:**
   - Kiểm tra `run_summary.json` có `status="DATASET_EXPORTED"`.
   - Kiểm tra `colab_handoff.ready=true` và `colab_handoff.auto_triggered=false`.
2. **Nén / Chuyển bundle:**
   - Sao chép toàn bộ thư mục `dataset_id=<dataset_id>/` vào Google Drive tại đường dẫn dự án:
     `My Drive/japan-earthquake-etl/ml-export/dataset_id=<dataset_id>/`.
3. **Mở Google Colab Notebook:**
   - Trong notebook `notebooks/experiment_harness.py` hoặc Colab harness:
     ```python
     from google.colab import drive
     drive.mount('/content/drive')
     # Trỏ dataset_root vào thư mục Drive vừa mount:
     DATASET_DIR = '/content/drive/MyDrive/japan-earthquake-etl/ml-export/dataset_id=ds_...'
     ```
   - Kiểm tra SHA-256 đối soát trong notebook; Colab chỉ đọc Parquet thuần túy, không cần token AWS hay MinIO.

---

## 5. Xử lý lỗi, Retry và Idempotency

- **Khi Gold chưa Published:** Task `wait_gold_published` sẽ fail với mã lỗi `DS_GOLD_NOT_PUBLISHED`. Kiểm tra lại tiến trình Gold Publish (ORC-01/ORC-07) và không chạy ép ML dataset.
- **Khi xung đột cấu hình:** Nếu dataset ID đã tồn tại với canonical payload khác, task `resolve_gold_snapshot` trả về `DS_IDENTITY_CONFLICT`. Cần điều chỉnh version hoặc dùng đúng configuration của dataset gốc.
- **Khi một step thất bại (ví dụ JVM OOM, timeout hoặc feature invalid):**
  - Trạng thái dừng ở `BUILDING` hoặc `REJECTED`, không bao giờ tạo nhãn `DATASET_EXPORTED`.
  - Staging lease được giữ lại để điều tra vết lỗi (`stdout.private.log`, `stderr.private.log`).
  - Khi retry, Airflow tạo `attempt_<N>` mới, không ghi đè hoặc trộn lẫn các part file từ lần chạy trước.
- **Xóa lease tồn sau khi đã xử lý lỗi hạ tầng:**
  Chỉ khi chắc chắn không còn process Spark nào đang ghi dữ liệu:
  ```bash
  rm -f /opt/pipeline/staging/source-guard/owner.json
  ```

---

## 6. Kiểm tra offline trong CI / Local

Chạy bộ test kiểm chứng hợp đồng DAG và runtime control plane:

```powershell
pytest airflow/tests/test_mli_02_build_dataset.py
```

Kiểm tra định dạng và trạng thái task index:

```powershell
& "C:\Program Files\Git\bin\sh.exe" scripts/check-task-status.sh
& "C:\Program Files\Git\bin\sh.exe" scripts/check-week-5-plan.sh
```
