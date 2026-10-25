# DAT-03 — Scoped replay và strict-split, 24/10/2026

Tiếp tục trên [PR #99](https://github.com/HoaiTam/japan-earthquake-etl/pull/99),
branch `feat/dat-03-research-dataset-handoff`, HEAD tại đợt đầu `49d1784`. Thay đổi đợt này
chưa commit/push, reviewer unassigned. Task **In Progress**, không full research release.

**Cập nhật tiếp theo, HEAD6356a44:** [bước 1/2](./DAT-03_RESEARCH_PROFILE_ACCEPTANCE.md)
đã xử lý depth encoding h2016 bằng parser v1.2, profile25 archive và pin nguồn
biên h2024. Các mục “blocker mới/chưa chốt” dưới đây là phát hiện **trước sửa**,
giữ nguyên làm lịch sử; không dùng chúng thay tiến độ hiện tại. Capacity ETL,
collection/sharding/full catalog và ba máy handoff vẫn chưa đạt.

## Quyết định owner và implementation

Owner yêu cầu tiếp tục kỹ thuật, chưa có receipt ThanhTris/rosy179. Chốt **giữ
split nghiêm ngặt**, không mượn dữ liệu ngoài kỳ hoặc âm thầm cắt PRE/POST.

| Phần | Đã triển khai | Chưa được chứng nhận |
|---|---|---|
| Resolver `orc-06-v2` opt-in | Fresh verify mọi source pin, descriptor thay retained annual raws, scoped JMA replay, full USGS stable-key delta, old/new graph closure lặp | Full research runtime và capacity toàn kỳ |
| Release/lineage | Thay JMA release rebuild registered months dù bytes giữ nguyên; parent v1→v2 được, downgrade v2→v1 bị chặn | Year-wide revision lớn có thể vẫn vượt row cap |
| `dat03-strict-split-v1` | Exclude window vượt split, báo từng ID/interval/reason, reconciliation counts và report SHA trong export identity | Không là source completeness hoặc scientific approval |
| Consumers | MLD-03 report/readback, MLI-02 phases, MLD-05 bundle/trusted manifest dùng cùng policy | Không thay giới hạn mỗi export 128 mainshock/100k feature rows |

Không đổi default v1/legacy, lịch DAG, deployed JAR, environment/credentials,
source lease hoặc warehouse. Không build/pull/start/restart/init Docker, không
lake writes hoặc cloud upload. Java/Spark QA dùng fixture và local temp stores.

## Đo reader trên archive 2011 thật

[Metadata nguyên kết quả](./DAT-03_JMA_2011_REPLAY_PROFILE.json).
Exact archive SHA `30980319b7fb72101c508e16b44a9e1ad124f4a94a9c31b0260df1df6745729b`,
309.527 physical records. Guard giữ nguyên **100.000 selected unique revisions**.

- Tháng 3: 56.015 unique revisions, reader 2.489ms.
- Tháng 4: 57.044 unique revisions, reader 1.656ms. Cao hơn accepted-month
  count 57.043 trong profile cũ do reader chọn thêm temporal halo 16s; không
  phải duplicate hoặc thay source bytes.
- Gộp cả hai tháng: `BOUNDED_MONTHLY_ROW_LIMIT`, được ghi là blocked, không
  truncate để pass. Lệnh probe exit0 nghĩa **đo hoàn tất**, không resolver/Gold pass.
- Local wall time 6,36s, `-Xmx768m`, Java 21.0.11 target17. Không phải peak heap,
  benchmark Spark joins/S3/Trino hoặc thời gian full backfill.
- Runner thực tế lúc đo:
  `3998f07bf43155fe6c59ab7fe20d92bcaa7820801c28fee249f38ca3a8c72f8b`.
  Raw/parser/reader probe không tạo BronzeReady hoặc COMPLETE_MONTHS receipt.

Lặp lại sau khi build JAR mới (output JSON phải chưa tồn tại):

```bash
make profile-jma-replay JMA_PROFILE_ARCHIVE=staging/dat03-profile/h2011.zip JMA_PROFILE_MEMBER=h2011 JMA_PROFILE_SHA256=30980319b7fb72101c508e16b44a9e1ad124f4a94a9c31b0260df1df6745729b JMA_PROFILE_YEAR=2011 JMA_PROFILE_MONTHS=2011-03,2011-04 JMA_PROFILE_OUTPUT=staging/dat03-profile/h2011-reader-replay-repeat.json
```

## Phát hiện ban đầu từ archive 2016 thật — trước bản sửa v1.2

Tải exact URL đã có trong inventory; HTTP200, application/zip, Content-Length
8.221.979, Last-Modified `2021-04-01T07:44:08Z` khớp metadata đã pin. Raw chỉ
ở staging, không đưa vào Git hoặc ghi Bronze/Silver/Gold.

[Profile](./DAT-03_JMA_2016_PROFILE.json): 313.487 physical = 313.469 accepted
+ **18 rejected**, reason `INVALID_NUMBER`, parser `slv-03-v1.1`. Chạy lại với
runner SHA trên cho profile cùng bytes (`cmp` đạt). Aggregate reconciliation
đạt không có nghĩa quality gate/source coverage đạt.

Chẩn đoán bằng cùng parser và Java local: toàn bộ 18 failure ở depth cột 45–49,
precision code `1`, token có blank bên trong như `  0 4`, `  0 8`. Ví dụ:

| Physical line trong h2016 | Record SHA256 | Trường/token |
|---:|---|---|
| 48748 | 0b60a6e394e4eba3def8fc33ddfae5914c95343de8c039935afae9799f7a66e3 | depth 45–49, `  0 4` |
| 51517 | b7c9dfc208ac402fce2c49a1da5fa5c7490459404463da602fa2d9c1c921d0fc | depth 45–49, `  0 7` |
| 51547 | eb9e63431e2b5783e3da2aa7fcd44521311705fc8b673da52d1449e9044c6f5b | depth 45–49, `  0 8` |

[Đặc tả JMA](https://www.data.jma.go.jp/eqev/data/bulletin/data/format/hypfmt_e.html)
xác nhận depth ở 45–49 có dạng F5.2 với depth-free; precision `1` là depth-free.
Trang này không trực tiếp giải thích các token blank nội bộ đã thấy. **Chưa
chốt cách diễn giải**; không tự biến thành 0/0,04/0,4 hoặc bỏ 18 event để pass.
Full-month reader vẫn fail closed khi record được chọn bị parser reject.
DAT-03 phải chốt mapping có căn cứ/fixture/version và reprofile trước rollout.

## Test/check thực tế

- Đợt regression đầu: 54 test PASS (có 26 legacy window/runtime/regression
  tests); đây không là full Java suite.
- Nhóm mới/consumers: DatasetManifest 9, WindowCoveragePolicy 5, DatasetBuild
  4, FeatureExportBundle 10, RealEtlAdapter 8 đều PASS.
- Lượt tiếp theo FullMonthStateResolver **21 PASS**, JmaResearchReplayProfileJob
  **3 PASS**, 0 failures/errors/skips; package BUILD SUCCESS. Tổng **60 test**
  trong bảy nhóm kỹ thuật mới/consumer đã đạt qua các lượt cuối.
- Bổ sung readback Silver v2 qua **GoldInputReader** trong scoped replay test:
  rerun riêng method 1/1 PASS, count canonical khớp receipt. Không commit Gold
  và không tính lần chạy lặp thành một test mới trong tổng 60.
- Airflow real adapter **12 PASS**, CI policy cuối **41 PASS**; docs/status/secret/
  diff checks đạt. Không phải remote GitHub CI hoặc runtime acceptance.
- Có lượt lỗi trước sửa: fixture success có 2 observations, assertion mới
  nhầm 1 đã sửa thành đối chiếu count baseline; probe descriptor URL/hàm hash
  kéo dependency Parquet đã sửa. Không tính các lượt đó là PASS.
- Tool chẩn đoán local đầu bị sandbox loopback/classpath thiếu Spark; đã dùng
  Java local với đúng test classpath để đọc metadata. Không bypass Docker gates.

## Còn lại, đều thuộc DAT-03

Danh sách lịch sử dưới đây đã được cập nhật ở [bước 1/2](./DAT-03_RESEARCH_PROFILE_ACCEPTANCE.md):
depth fix/archive profiles/boundary capture đã đạt trong phạm vi engineering;
source runtime/coverage/ETL capacity và full research gates chưa đạt.

1. Xử lý depth token 2016 có căn cứ, mở rộng profile các năm còn lại và chốt
   capacity/rollout. Disk lúc preflight còn khoảng **10 GiB**, phải đo lại;
   không suy 6,36s reader probe là thời gian full ETL.
2. Inventory/capture nguồn biên đầu 2024 JST cho hết tháng 12/2023 UTC và halo;
   USGS overlap theo policy. Strict ML split không bỏ yêu cầu source coverage.
3. Collection/sharding có identity/full-mainshock assignment proof, giữ Mc
   của full pinned split; exporter hiện **chưa** giải quyết full-period scale.
4. Real 225+63 months → Gold quality/Trino → MLI-02 export/reuse/loader; chưa chạy.
5. Cả ba người verify research release trên máy riêng. Pilot receipt owner
   hiện có không thay research handoff hoặc receipt ThanhTris/rosy179.

[DAT-03](../task/tasks/DAT-03.md) vẫn In Progress. EXP-02/03 official science,
EXP-04/05 evaluation, MLI-04/MLQ-01 report/E2E và SEC-01 cloud không được coi
hoàn tất từ các test/profile này; không đổi status Done component cũ.
