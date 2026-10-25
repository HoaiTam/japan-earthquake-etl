# DAT-03 — Phiên tạm dừng4service và khôi phục,25/10/2026

Branch `feat/dat-03-research-rollout`, owner HoaiTam/reviewer unassigned.
Owner duyệt rõ: stop Spark master/worker, Airflow API server/DAG processor để
đo rồi bật lại **cùng4containers**. Không build/pull/recreate, thay cron/DAG,
credential, lease hoặc dữ liệu. Đây là ngoại lệ operator, không đổi lifecycle
startup/test mặc định. [Cách vận hành](../runbooks/DAT-03_RESEARCH_ROLLOUT.md#5-phiên-giảm-tải-bốn-container--approval-riêng).

## Kết quả phiên thật

Deployment root là checkout `japan-earthquake-etl`; code ở checkout
`japan-earthquake-etl-mli-02`. Session:
`staging/dat03-light-session-28b1e2ae44ae476488b96dc5b1e508ac`.
Thời gian khoảng12:22–12:30 ngày25/10/2026, giờ Việt Nam UTC+7;
controller wall450,547s (~7m31s), dưới giới hạn2h/phiên.

**Stop/restore đạt, capacity chưa đạt.** Cuối phiên đã đối soát read-only:

- Cả9containers running/healthy; Spark master có1worker, không active app/driver.
- Bốn target giữ đúng container IDs/image/config/mounts; chỉ lifecycle/start
  time thay đổi do stop/start. Không recreate hoặc start container thay thế.
- Scheduler/PostgreSQL/MinIO/REST/Trino giữ IDs/config/mounts/start/restarts.
- Không active DagRun, Java trong scheduler, Trino query hoặc source lease.
- Không source/lake writes, publish, export, credential/IAM/DAG mutations.
  Trino dùng nguyên profile đã thử trước, không tăng RAM hoặc query timeout.
- Controller exit2 **vì hai probe chưa đạt**, không phải restore fail;
  failure_type=null, four_original_containers_restored_healthy=true,
  other_five_unchanged=true, full_capacity_accepted=false.

| Artifact trong session | SHA256 |
|---|---|
| before.json | 15313d05850ceec26654e53befa1a2ce289be8570fdd86e907d96d5eba9fcb1d |
| restored.json | d8868ad3eed452d433c3405611d6204f3efb25020b1a77651162fa331e265a9a |
| receipt.json | edd1e18c9facb5028f095cad2e60ab44ed13016766640d9ee7197647bce6137c |

Before/stop-intent/stopped/restored, host samples và từng probe receipt giữ
nguyên trong ignored staging. Không đưa full Docker inspect/Env/private logs
vào Git. Hash xác minh byte identity, không là chữ ký nguồn hoặc reviewer approval.

## Phép đo trên cùng pins/caps

Giữ namespace/pins của [bounded continuation](./DAT-03_CONTINUATION.md),
3.369.024 stress rows mỗi phía (105.282×32), đầy đủ hàng và symmetric EXCEPT.
Synthetic expansion không là research data. Query memory-per-node512MB,
heap1536MiB/direct512MiB/container3GiB, task.concurrency=1, query budget90s.

### Nguyên khối — fail memory gate

Attempt `staging/dat03-capacity/7ed6f7b2d4e84594b26d4bc7537eaef2`, query
`20261025_052316_00004_g4cap`: FAILED/EXCEEDED_LOCAL_MEMORY_LIMIT.
Server elapsed51,66s, CPU31,21s, peak530.106.829B; CLI54,133s.
CLI ghi Allocated505,49MB/Delta11,49MB, query limit512MB; top consumer là
HashAggregationOperator307,75MB. Đây là **engine query memory gate**, không
Docker OOM hoặc bằng chứng thiếu toàn bộ container RAM. Không nới cap để pass.
Receipt BLOCKED_MEMORY, postflight PASSED_UNCHANGED; receipt SHA
`bbdcb283c9e933242908cf1bc335a7da90e1249e0fbe48d0803af3e588493b72`.

### Buckets-v1 —3đạt, bucket thứ tư timeout

Attempt `staging/dat03-capacity/d9b910148dc548dc8f1f183e8f680d56`.
Giữ đủ16predicates trong immutable request, không sampling/hash equality.

| Bucket | Server query | Trạng thái | Server elapsed / CLI wall |
|---|---|---|---|
| 0 | 20261025_052411_00005_g4cap | FINISHED; changed_rows=0 | 23,83s /25,166s |
| 1 | 20261025_052437_00006_g4cap | FINISHED; changed_rows=0 | 22,69s /24,568s |
| 2 | 20261025_052501_00007_g4cap | FINISHED; changed_rows=0 | 39,55s /41,341s |
| 3 | 20261025_052550_00008_g4cap | FAILED/EXCEEDED_TIME_LIMIT | 1,93m /128,749s |
| 4–f | Chưa submit | Không được tính PASS | Không có số đo |

Receipt BLOCKED_TIME, completed4/required16: **3success +1failed +12chưa chạy**.
Whole probe221,047s; postflight PASSED_UNCHANGED/no active query. Không tiếp
tục hoặc resubmit bucket3. Limit cấu hình90s không phải cam kết hard wall90s:
server cleanup/enforcement có độ trễ; client có thêm40s. Giữ actual elapsed,
không thay115,8s thành90s trong evidence. Peak ba bucket đạt ~241–284MB;
bucket3 peak280.419.696B, CPU40s, không memory-limit error.
Receipt SHA `5fa614e95903c7596a5260e5c24a8073aac5c836d7cd3f700ae767aeee4479e5`.

## Host samples — không chứng minh causal improvement

Page16KiB, từng khoảng10s có counter deltas riêng, đơn vị MiB/s:

| Pha | Swap-in | Swap-out |
|---|---:|---:|
| Trước stop4 | 1,953 | 3,787 |
| Sau stop4/chờ ngắn, trước probes | 7,288 | 13,406 |
| Sau probes, trước restore | 37,892 | 61,744 |
| Sau restore | 50,599 | 17,998 |

Host8GiB/Docker VM5.685.809.152 bytes; host vẫn swap/compress mạnh. Không có
cơ sở khẳng định stop4 đã giải quyết memory pressure hoặc chỉ một ứng dụng
gây lỗi. RSS snapshots cũng không đại diện toàn compressed/VM footprint.
Không tự tắt ứng dụng khác, đổi Docker RAM hoặc dùng máy khác ngoài lựa chọn owner.

## Guard failure trước phép đo — không có container bị dừng

Attempt chuẩn bị cũ `staging/dat03-light-session-2bdc1994c18c4e919fe0e67f52dc6c83`
phát LIGHT_RUNTIME_OR_CONFIG_DRIFT do hash raw mounts array phụ thuộc order.
Không có stop-intent, không có stop/start; Docker status vẫn9healthy.
Artifact restoration-failed của prototype là guard error, **không** là bằng
chứng bốn container bị bỏ ở trạng thái stopped. Đã chứng minh old hash khớp
hoán vị của cùng6mounts ở scheduler/DAG processor; không config drift thật.
Giữ failed evidence, không sửa receipt thành PASS. Phiên mới dùng normalize
Destination và chỉ restore sau khi đã thực sự gửi stop; có regression riêng.

## Kiểm thử và phần còn thiếu

- `test_research_*.py`: **95 PASS**, gồm11 light-session mock tests. Mock output
  PASS không phải runtime capacity; runtime kết quả thật là hai gate fail trên.
- `make ci-docs-base`: **PASS/exit0** khi chạy lại sau phiên đo; docs244files/
  2.214links/0problems,77task statuses đồng bộ, contracts/mock guards, config/
  secret hygiene và static Compose checks đạt. CI không gọi Docker daemon,
  nguồn thật, stop/start hoặc stress query. Checkout code không có `.env`;
  check-config dùng `/dev/null` theo target CI, không chứng nhận deployment env.
- `git diff --check`: **PASS**. Các file runtime/capture/private logs giữ ngoài
  Git; không có commit/push/merge trong phiên này.
- Java source không đổi trong phiên này;31regression PASS của đợt trước giữ
  scope riêng, không coi là đã chạy lại hay full research acceptance.
- **DAT-03 — Catalog thật:** còn tối ưu/đo lại exact outside-scope verification,
  đủ16bucket/resource gates, full Spark/lake/export capacity và288monthly
  publications/checkpoints. Không lấy3bucket hoặc bốn tháng mẫu thay full kỳ.
- **DAT-03 — Official collection export:** versioned MLI-02 lifecycle/registry/
  loader/import và real full-period export chưa triển khai. Khối5 receipts ba
  máy còn riêng; EXP-02/03 official fits, MLI-04/MLQ-01 report/E2E chưa mở gate.
- SEC-01 local scope không đổi; không cloud/Colab/Drive/ACL và không approval
  khoa học. Task DAT-03 giữ In Progress, chưa commit/push/merge.

## Checkpoint sau phiên đo — Docker không kết nối được

Lúc12:43 ngày25/10/2026 (05:43UTC), kiểm tra read-only `docker ps` với quyền
truy cập socket trả về `Cannot connect to the Docker daemon`. Lần sandbox
trước đó bị permission denied; đây là hai kết quả khác nhau, không dùng lỗi
sandbox để kết luận daemon hỏng. Lần có quyền vẫn không kết nối được, nhưng
chưa xác định nguyên nhân hoặc ai đã dừng Docker.

Mốc9healthy bên trên là **postflight của phiên12:22–12:30**, không phải trạng
thái hiện tại đã xác minh. Không tự mở/restart Docker Desktop, stop/start lại
4containers, replay query hoặc chạy ETL để xử lý. Owner cần đưa Docker về trạng
thái truy cập được; sau đó fresh read-only kiểm exact IDs/config/health,
DagRun/Java/Trino/Spark và lease trước mọi phép đo mới. Không suy kết quả
restore cũ là bằng chứng đủ capacity; khối3/4 vẫn chưa hoàn tất.
