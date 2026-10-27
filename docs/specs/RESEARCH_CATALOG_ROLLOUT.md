# DAT-03 — Checkpointed catalog rollout v1

Version `dat03-catalog-rollout-v1`, explicit opt-in, không thay daily DAG hoặc
deployed runner. Java/Spark xử lý nguồn và dữ liệu; Python chỉ chuyển exact
bytes, kiểm metadata/pins, điều phối process/session và own lease.

## Phạm vi và input

- Một namespace mới `iceberg.dat03_catalog_<uuid>` cho đúng288UTC months:
  REPRODUCTION225 [2000-01,2018-10), EXTENSION63 [2018-10,2024-01).
- Stage25 original JMA archives2000–2024 và12 USGS monthly captures2023 đã
  verify. Giữ metadata/retrieved_at/release/bytes/SHA gốc, không download lại.
- Bootstrap37 Bronze pins vào resolver v2 một lần; các tháng sau tái dùng
  active input set đã receipt-bound. Không giới thiệu năm mới vào baseline
  đang chạy rồi vô tình làm thay lineage của các tháng đã đăng ký.
- Code/controller/support/inventories/plan/USGS summary và source cutoff được
  khóa trong request bất biến. Request không nhận rows, Mc, query tùy ý hoặc
  cờ completeness/research-ready từ caller.
- Hai capacity proofs cho event/bridge phải so đầy đủ16bucket trên exact
  bounded snapshots; mỗi phía ít nhất bằng profile JMA trong288tháng cộng
  tổng USGS response features (có halo duplicates, dùng như upper bound).
  Cả32 actual stdout integral changed_rows=0 và postflight unchanged.
  Proof này chỉ mở phép đo ETL rollout, không chứng nhận full Spark/ML capacity.

## State và phục hồi

Mỗi tháng chạy `readiness → bronze → silver → gold → verify → publish` bằng
RealEtlAdapter, FullMonthStateResolver v2 và isolated Gold publisher. Cả16
outside-scope buckets cho hai fact tables vẫn so toàn hàng bằng EXCEPT hai
chiều; SHA chỉ routing, không thay equality. Default publisher không đổi.

```text
request.json, runner.jar, controller.py, research-e2e.py
baseline.csv, boundary.csv, plan.json, usgs-summary.json
inputs/<source>.json + original raw bytes
capacity/<table>-request.json, <table>-receipt.json, actual stdout
bronze-pins.json
months/YYYY-MM/context.json, <phase>.json, record.json, checkpoint.json
sessions/<uuid>/intent.json, stdout.private, stderr.private,
                execution.json, result.json, acceptance.json
```

- Checkpoint chỉ sau Published; khóa request identity, tháng, record SHA,
  six snapshot IDs và SHA checkpoint trước. Chỉ nhận contiguous prefix của
  plan, không bỏ tháng hoặc reorder. Registry vẫn có quality/coverage thật.
- Local JSON canonical có thể đổi key order; `commit_journal_sha256` phải lấy
  từ **bytes gốc MinIO**, không reserialize local tree để tính bundle SHA.
  Trước tiếp tục, đối soát independent stored commit/publication của parent.
- Context tháng đã tạo là bất biến. Engine tái xác minh upstream/SHA/journals;
  Gold writer reconcile operation snapshots, không reappend committed table.
- Chỉ `resume` sau session có acceptance thành công, không active process/query
  hoặc lease. Session failed/timeout/partial-month không có acceptance: giữ
  artifacts và lease, chặn auto retry; cần chẩn đoán/recovery riêng.

## Session budget và lease

- Host run có deadline7200s, trừ thời gian preflight/staging và reserve600s trước
  launch. Default Java budget6000s, không hard-code một job chạy đủ288tháng.
- Legacy không bắt đầu tháng mới nếu còn<1800s; continuation batch v2 cần
  còn>=3600s trước batch mới. Không bắt đầu phase nếu còn<1200s.
  Nếu phải dừng giữa tháng, ghi PARTIAL_MONTH_REQUIRES_RECOVERY và giữ lease.
- Worker có timeout riêng, SIGTERM rồi SIGKILL **chỉ process group nó tạo**;
  timeout giữ lease/outcome unknown, không tự cancel Trino hoặc restart service.
  Không cam kết finally chống mất điện/SIGKILL/Docker failure.
- Chỉ release đúng owner sau child exit0, checkpoint/record chain đã verify,
  không pending month, no active Java/DagRun/Trino query và input pins không đổi.
  Session mới acquire lại khi lease trống; không expiration/foreign takeover.
- Host observations mới dùng [content-addressed manifests](./RESEARCH_HOST_OBSERVATIONS.md):
  frozen inputs giữ một bộ, stream missing payloads; mỗi observation vẫn khóa
  đầy đủ file-set/SHA và không overwrite acceptance/failed bytes. Legacy
  full-copy vẫn đọc được hoặc migrate explicit có proof; không còn full
  `docker cp` mỗi phiên. Private logs không vào Git hoặc kit nhóm.

## Gate còn lại

CATALOG_PUBLISHED chỉ có sau288checkpoint thật; official_exported và
research_ready vẫn false. **DAT-03 — official collection export** còn full-split
audit/Mc/strict exclusions, collection-aware lifecycle/registry/loader/import,
real export và rerun. **DAT-03 — handoff** còn verifier/receipt trên ba máy.
Không dùng checkpoint một phần, mock tests hoặc synthetic stress thay dataset.

## Continuation có version — batch/cache/verifier opt-in

`dat03-catalog-continuation-v1` không sửa original request/JAR/cutoff/inputs
hoặc checkpoint. Envelope mới pin exact original request SHA, original JAR,
continuation JAR/controller/support và compatibility proof. Host và Java đều
so từng original JAR entry, chặn parser/canonical/transform/dependency drift;
chỉ bảy class của catalog/proof-cache/opt-in verifier được đổi, ba class mới
được thêm. Không lấy cùng version string thay bằng chứng bytecode.

- Batch policy hiện tại `dat03-catalog-batches-v2`: next-fit theo exact contiguous
  month prefix, <=12months và <=75.000 profile rows/batch; không qua split
  2018-10. Current profile cho69batches nếu bắt đầu từ0, chỉ là plan, không ETA.
  Actual selected revisions vẫn bị resolver chặn100.000; không truncate hoặc
  dùng profile làm source proof. Tháng đơn vượt planning bound cần review.
  Profile Spark local[1]/heap768MiB/direct256MiB giữ nguyên, shuffle16 explicit
  trong continuation; legacy shuffle1 không đổi. Reserve3600s trước batch mới
  để không dùng thời gian còn lại quá ngắn cho verify/publication.
- Một batch có một Gold six-snapshot publication; từng tháng có checkpoint
  trỏ cùng record/pins và giữ chain theo tháng. Không nhân số SQL readbacks
  lên số tháng trong batch. Acceptance kiểm tất cả target months của record
  đều có checkpoint; lỗi ghi checkpoint giữ lease, không tự chứng nhận prefix
  dở của batch. Parent registry chứa coverage thực cho toàn affected scope.
- `JmaStructureCache` chỉ sống trong JVM, tối đa64entries, không giữ raw bytes,
  không nhận entries/flags từ caller. Cache admission bắt buộc chạy ZIP/CRC/
  record-width/count validator thật. Key là verified raw SHA + exact member;
  mỗi read vẫn fetch/re-hash bytes, kiểm manifest/inventory/count/cutoff.
  Đổi member/raw/count/manifest không bypass guard. Cache invalid/evicted phải
  scan lại; session/JAR mới bắt đầu empty. Default reader không cache.
- Verifier `dat03-exact-fact-blockers-v1` route uniqueness event bằng canonical
  ID, uniqueness bridge bằng **source_observation_id** để không che duplicate
  gán sang canonical khác. Bridge lineage/source counts/primary provenance
  route cả parent lẫn child bằng cùng canonical predicate; null cũng vào đúng
  một bucket. Mỗi blocker đủ16actual scalar SQL, derived sum ghi rõ kind,
  không tạo SQL giả. Stream/filter/dimension blockers giữ nguyên.
- `GoldPublisher.researchCatalog` chỉ opt-in isolated dat03_catalog namespace;
  legacy/researchRollout/daily/operational behavior giữ nguyên. Journal không
  được reuse bằng strategy khác. Outside-scope vẫn đủ16full-row EXCEPT hai chiều.
- Chỉ dùng continuation sau session cũ terminal, accepted, idle và lease trống.
  Không thay runner của process đang chạy. Trước tiên nghiệm thu một batch
  thật/resource rồi mới mở thêm; default/historical evidence không bị viết lại.

## Reuse proof cùng process — không bỏ fresh publication checks

`GoldPublisher.researchCatalogCheckpointed` chỉ reuse verify thành công trong
cùng instance/process, cùng commit bytes/run/namespace/identity/six pins, tối đa
300s. Failed verify xóa cache; đổi commit, instance, pin hoặc hết hạn phải chạy
verify đầy đủ. Fresh six current snapshot readbacks và status/serving checks
vẫn chạy thật trước/sau publication. Default publisher không đổi.

Publication ghi `dat03-same-process-verified-snapshots-v1`, source report SHA,
tuổi proof và số check được reuse; controller bind SHA vào phase verify receipt.
Không báo số check được sao lại thành số SQL đã thực thi lần nữa.

Các đợt commit tối đa khoảng3h chứa nhiều phiên <=2h; chỉ chốt ở checkpoint
Published/accepted, không cắt ngang phase để đủ giờ. Mốc tiến độ catalog là
72/144/216/288 tháng, không phải phần trăm Done của toàn DAT-03. Xem
[kế hoạch chạy batch](../runbooks/DAT-03_BATCH_EXECUTION.md).
