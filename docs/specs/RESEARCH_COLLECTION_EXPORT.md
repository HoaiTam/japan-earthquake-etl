# DAT-03 — Official collection export v1

Version `dat03-collection-export-v1`, execution protocol `mli-02-collection-v1`.
Đây là nhánh opt-in của MLI-02, không đổi exporter v1, grain 29 cột, dataset
lifecycle hoặc result-bundle schema. **Code/component tests không thay actual
full research acceptance.** DAT-03 vẫn In Progress cho đến khi đủ mọi gate.

## Identity và nguồn

`filter_config.collection_export` chứa đúng `version` và `layout_limits`.
Limits: <=128 mainshock, <=100.000 dòng mỗi shard, <=64 shard; tổng metadata
<=4096 mainshock. Cấu hình và strict-split policy tham gia dataset identity.
Protocol v1 không được âm thầm nhận layout collection; version/limit sai hoặc
boolean thay integer bị reject trước acquire/write. Không sample, truncate,
chia window hoặc estimate Mc theo shard.

Collection preparation vẫn là BUILDING/PREPARED. Official export chỉ resolve
persisted dataset và Published Gold pin, receipt-bound JMA coverage225/63 tháng,
full-split audit/Mc, selection/windows/features/assignment từ Java. Không nhận
rows, Mc hoặc cờ completeness do caller tự khai. Audit manifest đầy đủ đi cùng
global Mc proof; original prepared manifest/checksums có external receipt.

## Official lifecycle

1. Verify preparation, parent identity/Gold/cutoff/config và fresh source coverage.
2. Commit **toàn parent** mainshock/candidate Iceberg snapshots; shard chỉ là
   layout, không là dataset mới. Trino đọc exact pins/count/identity.
3. BUILDING → VALIDATED với full counts, audit/Mc/schema, two pins và export
   identity bind plan/prepared manifest + global sorted row fingerprints.
4. Copy exact verified logical Parquet shards sang attempt riêng, index/hash,
   readback tất cả shard/assignment/schema/features/identity, atomic install.
5. VALIDATED → EXPORTED, Trino verify registry snapshot; tạo trusted registry
   ngoài bundle, root manifest được ghi cuối và handoff receipt pin SHA ngoài.

Rerun kiểm exact pins và bytes, không tạo lại snapshot/artifact đã mất hoặc
ghi lại corrupted bundle. Unknown outcome/partial lifecycle giữ lease để
operator reconcile; không tự reset hay đổi REJECTED. Registry/trusted receipt
thiếu thì chưa hoàn tất handoff, dù portable directory đã được materialize.

## Layout portable và registry

    dataset_id=ds_<sha>/
      dataset_manifest.json
      collection-preparation.json
      export-checksums.sha256
      checksums.sha256
      shard_00000/
        features.parquet
        mainshocks.parquet
      shard_00001/...

Mỗi Parquet <=64MiB. Data index gồm preparation + shards, không gồm manifest
tự tham chiếu. Public index thêm dataset manifest và data index. Receipt bên
ngoài pin manifest/public-index SHA, không coi tự rehash là trusted origin.

Trusted registry giữ `ds_<sha>.json`, copy data index và một bounded lineage
JSON/shard (<=100k candidates, <=64MiB metadata), mỗi file có ref/SHA/counts.
Root manifest không collect/nhúng hàng triệu candidate IDs. Registry pin vẫn
là snapshot EXPORTED toàn parent; two feature/mainshock pins cũng toàn parent.

## Loader và result import

`notebooks/research_collection.py` nhận hai expected SHA từ release receipt.
Constructor hash **toàn file set**, kiểm exact paths/no symlink, full assignment
metadata/coverage/global Mc; `load_shard` đọc một shard sau footer row bound,
kiểm lại SHA/schema/types/finite/grain/roles/self/global Mc/versions/counts.
`verify_all` lần lượt đọc mọi shard rồi giải phóng, không concat toàn catalog
vào RAM. Không fit, generate feature, gọi mạng hay cần pipeline credential.

Shard trả về vẫn giữ manifest/counts **toàn parent**, kèm `collection_scope`
`WHOLE_WINDOW_SHARD`; không giả count shard là count dataset. Harness pin scope
trong experiment identity/config; retry không được đổi scope.

MLI-03 yêu cầu `collection_id` và `collection_shard_id` khi nhận registry version
này. Resolver chỉ đọc trusted bounded lineage của shard đã chọn, kiểm assignment,
count/grain/roles; caller không cung cấp candidates. Result contract phải có
đúng scope trong checked `experiment_config.json`; thiếu/khác scope bị chặn
trước ghi. Candidate report cũng ghi scope, config Iceberg giữ provenance này.

**Không coi một shard CANDIDATE là full-collection experiment.** DAT-03 sở hữu
dataset/export/loader/handoff; [EXP-02](../task/tasks/EXP-02.md),
[EXP-03](../task/tasks/EXP-03.md), [MLI-04](../task/tasks/MLI-04.md) và
[MLQ-01](../task/tasks/MLQ-01.md) cần kiểm đủ shard/config trước kết luận
khoa học hoặc báo cáo toàn kỳ. Scientific approval không được tự cấp.

## Runtime và an toàn

MLI-02 có thể chọn JAR đã pin riêng dưới
`/opt/pipeline/staging/dat03-ml-export/<uuid>/runner.jar`; mặc định deployed
JAR không đổi. Không nhận shell command/runner path từ DagRun. Các roots/code
pin được bind trong immutable context, lease bao toàn run, no build/pull/init.
Mỗi runtime session vẫn phải giữ budget <=2h và checkpoint. Controller opt-in
`scripts/research-ml-export.py`/`ResearchDatasetBuildJob` nối10 MLI-02 steps và
giữ lease qua clean checkpoint. Đây không phải scheduled DagRun và **chưa**
có actual long-run export acceptance. Xem
[runbook controller](../runbooks/DAT-03_RESEARCH_ML_EXPORT.md).

## Kiểm thử

`make test-research-collection` chạy fixture Spark/Parquet, identity/version,
trusted resolver/result scope và offline Python loader. Không Docker hoặc
source download; không fake actual research/three-machine receipt.

Sau khi có **release thật và pins từ external receipt**, từng người dùng:

```bash
python3 notebooks/research_collection.py --bundle "$DATASET_DIRECTORY" \
  --manifest-sha256 "$EXPECTED_MANIFEST_SHA256" \
  --checksums-sha256 "$EXPECTED_CHECKSUMS_SHA256" \
  --recipient HoaiTam --receipt hoaitam-research-receipt.json
```

ThanhTris/rosy179 đổi đúng recipient và receipt riêng, tự chạy trên máy mình.
Kết quả phải `COLLECTION_VERIFIED`, all_shards_verified=true và cùng dataset/
collection/SHA/counts. Không tạo hộ receipts bằng nhiều lần chạy máy owner.

Khi cả hai splits được nghiệm thu thật, [paired research kit](./RESEARCH_TEAM_KIT.md)
đóng gói hai bundles cùng Gold pin +offline verifier thành một ZIP bất biến.
Packer/component tests không thay actual export acceptance hoặc three-machine receipts.
