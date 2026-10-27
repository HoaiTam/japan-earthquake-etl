# DAT-03 — Đo trước khi đổi verification

## Quy trình thay thế chạy thử dài

1. Bật Docker bằng thao tác vận hành riêng; test không build/pull/init/reset.
2. Đọc services, Java/DagRuns/Trino queries, session artifacts và own lease.
   Nếu run bị gián đoạn, không giả execution/acceptance hoặc release lease.
3. Đo read-only trên exact committed snapshots của batch hiện tại. Không
   re-ingest, replay Gold commit, publication hoặc mở batch tiếp.
4. So cùng một điều kiện chất lượng bằng whole-query và tổng đủ16bucket.
   Không sampling; mỗi kết quả phải là scalar integral không âm. Kết quả phải
   bằng nhau. Ghi thời gian từng query, commit SHA và capacity quan sát được.
5. Chỉ đưa check đã đo vào profile opt-in có version/giới hạn. Không thay
   đồng loạt mọi bucket hoặc dùng số đo một check làm ETA toàn pipeline.
6. Trước publication cần controller/recovery mới pin đúng bytecode/profile,
   xử lý interruption có evidence, kiểm quiescence/six pins/lease và đầy đủ
   acceptance. Benchmark không thay gate này.

## Profile component mới

`GoldPublisher.researchCatalogSinglePassPrimary` dùng
`dat03-primary-whole-other-buckets-v1`: chỉ primary_provenance chạy whole-query,
không thay SQL điều kiện. Bốn blocker còn lại vẫn16bucket, outside-scope vẫn
32full-row EXCEPT. Freshness, snapshot identity/schema/count, publication
readbacks và reuse cùng process giữ nguyên. Journal khác strategy bị từ chối.
Giới hạn mỗi fact <=1.300.000 rows; vượt ngưỡng fail closed trước SQL, không
suy capacity cho catalog288tháng. Factory cũ và daily không đổi.

**Chưa wiring vào pinned recovery/normal continuation.** Attempt V2 bị Docker
shutdown trước terminal receipt; không sửa JAR của attempt hoặc dùng benchmark
để thay execution thất lạc. DAT-03 vẫn In Progress.

## Chạy benchmark

Class `ie212.earthquake.spark.gold.ResearchVerificationBenchmark` nhận ba tham
số: đường dẫn commit JSON, tên blocker trong allowlist, đường dẫn report mới.
Dùng TRINO_HOST/TRINO_INTERNAL_PORT của môi trường deployment. Chỉ phát SELECT;
không dùng object-store writer hoặc release lease. Report có acceptance=false.
Mỗi query dùng research polling300s hiện có, không retry.

Lệnh actual27/10 (report phải chưa tồn tại, không chạy lại vào cùng output):

```bash
docker --context desktop-linux exec japan-earthquake-etl-airflow-scheduler-1 java -Xmx256m -XX:MaxDirectMemorySize=64m -cp /tmp/dat03-verification-benchmark-v2-20261027.jar ie212.earthquake.spark.gold.ResearchVerificationBenchmark /opt/pipeline/staging/dat03-catalog/b6db356fb30e46f9a26a419ebacb5393/months/2009-06/gold.json primary_provenance /tmp/dat03-primary-benchmark-20261027.json
```

Đây là artifact benchmark tạm, không runner recovery đã pin. Không dùng lệnh
này như startup hoặc E2E acceptance. [Số đo thật](../evidence/DAT-03_VERIFICATION_BENCHMARK_20261027.md).
