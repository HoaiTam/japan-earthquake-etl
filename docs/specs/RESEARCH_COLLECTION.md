# DAT-03 — Mainshock collection v1

Version `dat03-mainshock-collection-v1`, opt-in preparation contract. Không đổi
ML schema/grain hoặc enum dataset lifecycle. Default MLD-05/MLI-02 exporter vẫn
cap128 mainshock/100k rows; collection không giả từng shard là full dataset.

## Identity và đối soát

- Pin một persisted BUILDING dataset, exact Published Gold snapshot, full
  225/63-month receipt-bound source coverage và audit/Mc trên **toàn split**.
- Một collection dùng chung parent dataset_id, Mc/config/hash, strict window
  exclusion report. Không estimate Mc theo batch/shard hoặc mượn split khác.
- Sắp mainshock IDs, next-fit deterministic vào shard <=128 mainshocks và
  <=100.000 feature rows. Mỗi mainshock xuất hiện đúng một shard; không chia
  window. Một window vượt budget thì fail, không truncate/split window.
- Layout limits cũng tham gia collection_id. Full-assignment proof giữ từng
  mainshock ID/count/shard, total rows và excluded IDs/reasons; verifier dựng
  lại plan từ exact upstream summaries để phát hiện omission/duplicate/reassign.
- Parent identity/Gold/coverage/Mc/strict split/layout/assignment đều được hash.
  Mỗi shard dùng cùng dataset_id và 29 feature columns, giữ grain toàn collection.

## Boundary và lifecycle

Collection preparation materialize/readback shards, immutable manifest/checksums
và external expected manifest SHA. Kết quả **RESEARCH_COLLECTION_PREPARED**,
research_ready=false, official_exported=false, parent BUILDING không đổi.
Không tạo EXPORTED giả, không đưa prepared collection vào TrustedDatasetResolver/
MLI-03. DAT-03 bước export chính thức sau full research rollout phải nối
collection-aware MLI-02/registry/loader/import gate bằng version riêng.

Production entrypoint chỉ nhận namespace/dataset ID/limits/output metadata;
không nhận events, Mc hoặc cờ completeness từ caller. Unit fixture API là
component QA, không source/Gold proof. Rerun verify existing exact bytes/pins;
thiếu/hỏng artifact fail, không tự tái tạo để che corruption. Không sửa Gold,
không commit Iceberg hoặc chuyển lifecycle trong preparation.

## Runtime bước 4

DAT-03 nghiệm thu bounded real-source ETL trong namespace mới riêng, giữ
shared lease toàn run, exact JAR/inventory/capture pins, resolver v2 và rerun.
Các tháng: 2000-01, 2011-04, 2016-04, 2023-12 (+h2024/USGS halo). Không chứng
nhận full collection/ML nếu chỉ có các tháng mẫu. Không đổi deployed files,
cron/DagRun, credential/IAM hoặc xóa dữ liệu. Ghi actual capacity/Trino evidence.
