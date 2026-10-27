# DAT-03 — Recovery verify batch 06–11/2009 (27/10/2026)

**Chỉ đạo mới nhất27/10:** chỉ hoàn tất batch06–11/2009 hiện tại rồi dừng.
Thu hồi quyền mở normal continuation/batch kế tiếp; không resume12/2009 kể
cả khi checkpoint119 đã accepted và còn budget. Follow-up chỉ nghiệm thu
recovery đang chạy, báo terminal result rồi tự xóa. Không dừng Docker services
hoặc xóa artifacts. Các approval chạy tiếp sau17:00 bên dưới là lịch sử đã
được giới hạn lại bởi yêu cầu này.

Branch `fix/dat-03-catalog-publication-recovery`; tiếp tục incident hiện tại,
không tạo catalog/source cutoff/namespace mới hoặc commit/push/merge.
Owner yêu cầu xử lý lỗi rồi tiếp tục. Ban đầu treo máy đến17:00
Asia/Ho_Chi_Minh; sau đó đã xác nhận **cho phép chạy tiếp sau17:00, tối đa2h
mỗi phiên**. Chỉ mở phiên kế từ accepted checkpoint sạch, còn >=4GiB host
physical reserve và đúng scope HoaiTam06/2008–07/2013 (đến163global months).

## Checkpoint đã nghiệm thu trước lỗi

- Session `5b45bd078c7e46bf8fce7575e0a0b660`: exit0, không timeout,
  4579,277s, own lease RELEASED và host CAS/reporter đạt.
- Accepted **113/288 tháng (39,236%)**, đến2009-05;
  1.200.455canonical/bridge. Phần HoaiTam06/2008–07/2013:12/62tháng.
- Observation `runtime-observation-2a66036be251481da7d6de4151e6654d`,
  manifest SHA `f100a2a2098c29b7dd47625d7c8cbb6dfc50618e971da644ab8191f9f2a655c1`.
- Acceptance SHA `a66f57a738e7449902924c5ea8e8120a265f9518c976247d2391d8ef1455d4fd`,
  result SHA `6a1cf89e7f8cf55ccd861646f6689cb68e7a5ae390ed7ddaf3e8761256081426`,
  execution SHA `ce881608e9df66b1f89fc0fb75e720a24c0bf26550ff34bc4a709dcba8b7c40f`.

## Failure giữ nguyên

Session `6af846903c964a46853536943d6e52ad`: exit1, timed_out=false,
4101,289s, Java peakRSS1.412.412KiB. Scope exact
`[2009-06-01T00:00:00Z,2009-12-01T00:00:00Z)`; readiness/bronze/silver/gold
đã có, Silver balanced; thiếu verify/publish/result/acceptance. Không đếm batch
này vào accepted prefix113.

- Client: `TRINO_QUERY_OUTCOME_UNKNOWN method=GET page=90`, query
  `20261027_080321_00473_7tnbe`, retry=disabled.
- Actual Trino query metadata: FAILED/ABANDONED_QUERY; createTime
  `2026-10-27T08:06:07.754007428Z`, lastHeartbeat
  `2026-10-27T08:08:08.507757192Z`, endTime
  `2026-10-27T08:13:08.630617497Z`. Query ID không dùng làm timestamp thay
  actual metadata. Khoảng heartbeat xấp xỉ2phút và kết thúc sau5phút không
  polling phù hợp với deadline2phút của client; không đủ chứng minh sleep,
  OOM hay một lỗi transport cụ thể vì client đã redacted exception cause.
- Query là SELECT/full-row EXCEPT theo bucket; peak user memory124.169.096B,
  physical input4.950.300rows/408.559.016B, CPU1,62phút,
  totalBlocked13,75phút (cộng theo tasks, không elapsed wall).
- Failed observation `runtime-observation-a55248ffc8844d0788354589fd794722`,
  manifest SHA `4a9b3b184250da32da7f81bfddec42eded9b9864c9d21b63ebd49e5dd7ff963f`;
  full manifest/frozen inputs đã kiểm. Original files và source lease giữ lại.

Host8GiB RAM; Docker còn có workload project khác. Chỉ ghi nhận tài nguyên
thực tế, không kết luận workload khác là root cause hoặc tự stop nó.

## Phục hồi V1 đã dừng (giữ evidence)

Tái dùng [publication-only recovery](../runbooks/DAT-03_CATALOG_PUBLICATION_RECOVERY.md).
Không tăng SQL timeout/memory cap, không thay Trino client/publisher/parser,
không replay ETL, không restart service hay release lease để gọi normal resume.
Recovery dùng Java512MiB, không SparkSession; không coi đây là chứng nhận đã
khắc phục mọi vấn đề capacity của các batch tiếp theo.

- Exact attempt `ebfd2e9c9df64175a04622b6174672c6` mới, failed session pin riêng;
  recovery session `d94bfeca4c964205860709a6ec102b66`.
- Runner SHA `8d839777c4f5c84ab0bafc1a323e1442097ced54a1762427686e2c17e18a39eb`;
  mọi original entry của continuation739e2e giữ nguyên bytes, chỉ thêm class
  recovery đã kiểm thử. Không sửa failed execution thành success.
- Preflight **PASS13/13**; own lease đúngrun, services/quiescence/input/parent/
  six pins/absence of later artifacts đã đạt. Python recovery regression:
  **8tests PASS**; không chạy Spark fixture song song runtime.
- `run` sau preflight đã dừng ở verify: exit1/no timeout/1686,953s, ordinal103;
  `GET page=63 query_id=20261027_091507_00589_7tnbe`, actual Trino
  FAILED/ABANDONED_QUERY. Query là `primary_provenance:2`, peak user memory
  32.215.600B; chưa có verify/publish/result/acceptance, không release lease.
  Observation `runtime-observation-e1cb0f4655a54a6c8cd7cbf4a7c32057` giữ cả hai
  failed sessions, không nâng latest accepted113 hoặc tự retry attempt này.

## Bản vá và actual preflight V2

- Thêm profile `dat03-select-poll-300s-v1`: chỉ research SELECT polling300s;
  default client/DDL/DML120s và từng HTTP90s giữ nguyên, không resubmit POST.
  Diagnostic mới phân biệt QUERY_DEADLINE/HTTP_TIMEOUT/TRANSPORT/INTERRUPTED,
  chỉ in method/page/safe queryID/budget, không rawSQL/URL/payload/exception cause.
- Recovery V2 được pin riêng, chỉ thay class TrinoSqlClient và thêm class
  recovery; publisher/SQL strings/parser/resolver/ML/source bytes giữ nguyên.
  Resolve primary failure và related failed recovery bằng actual acceptance
  mới, không sửa executions cũ hoặc bỏ qua failed session trong normal resume.
- Attempt `99124e6d7af8477aa2e4a6b64aea9bf8`, new session
  `1547dae95a0c4ff5a9a2b063b075441d`.
  Request SHA `396dd393f4a97a3e2c3181e84eea9872f97cc5b96bee1fd07b4343e3498be90d`;
  runner SHA `c2973204e54bcfffc3d64b7456d0c1ed3a4f6d937008856d1ddf4e1f5f990da8`;
  transport class SHA `9fbd918ca7b98d7230340169cc37fa226a197cef941db6c0f693a1167ad5852c`.
- Actual preflight **PASS13metadata SELECT +1slow-quality probe**:
  `primary_provenance:2` trả integral0 trong **58,637s**, profile mới ghi riêng
  và worker phải kiểm trước `run`. Probe không sửa data, không chứng minh mọi
  query sẽ dưới300s hoặc thay full-period capacity.
- `run` V2 đã mở sau preflight và đang **verify**. Accepted vẫn113/288; chỉ
  cộng119 khi verify/publish/final pins/result/own release/acceptance/hostreport
  đều đạt. Không tự gắn success từ local tests hoặc probe.
- Compatible continuation mới cho các batch sau:
  `ad0877a536e04062b069a8afa5257f5f`, runner SHA
  `9101cb5fdbb65f298dabc276fd0bb90536d4b135d6aea33bca054423c60d09a8`,
  controller SHA `706debb3dc9e21cb9c51489919e953751a6a83a3ecf58e4800213cfc1ab0a2c3`.
  So với runner739e2e, chỉ TrinoSqlClient/ResearchCatalogJob/compatibility gate
  khác bytes; không added class hoặc thay publisher/parser/resolver/ML.
  Chưa chạy continuation mới khi recovery đang active/lease giữ.

## Kiểm thử bản vá

- Java client/recovery13tests PASS; local offline Maven package PASS (JDK21,
  release17). Lần đầu mock HTTP bị sandbox chặn bind localhost; đã chạy lại
  với quyền localhost phù hợp và PASS, không sửa test để né lỗi.
- Java compatibility/publication/fact-buckets/outside-buckets/reuse23tests PASS,
  chỉ mocks/pure code, không SparkSession.
- Python catalog/progress/recovery34tests, storage28tests và ML export16tests
  PASS; recovery tests có legacy V1, narrow transport exception, hai failed
  sessions và probe missing/boolean/nonzero đều bị chặn.
- Không Spark fixture hoặc full project E2E song song phiên recovery.

## Gate còn lại

**DAT-03 — Catalog nghiên cứu thật và handoff:** batch recovery này chưa là
full288-month coverage hoặc official ML export/loader/rerun/receipts ba máy.
Giữ In Progress, research_ready=false, official_exported=false.
**SEC-01 — Security review:** recipient/identity acceptance trên Drive còn thiếu.
Bản Drive vẫn partial101, chưa upload các tháng mới.
