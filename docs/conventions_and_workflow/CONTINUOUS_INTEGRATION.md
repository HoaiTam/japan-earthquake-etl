# CI bằng GitHub Actions

## 1. Mục đích và phạm vi

Owner cấu hình ban đầu: **HoaiTam**. Reviewer: **unassigned**.
Branch triển khai: `ci/offline-pipeline-checks`, từ `main` commit `5373cb8`.
Đây là yêu cầu bổ sung CI, không đánh dấu các task E2E/security còn thiếu là Done.

Workflow [ci.yml](../../.github/workflows/ci.yml) tự động kiểm tra code,
contracts, fixture và control-plane khi nhóm cập nhật PR. Ba job độc lập giúp
biết phần nào hỏng mà không phải chờ một job khác thành công trước.
Các lệnh dùng chung nằm trong [Makefile](../../Makefile).

Không có deploy, trigger DAG, download USGS/JMA, backfill hoặc publish Gold
trong workflow. “Offline tests” nghĩa là không phụ thuộc nguồn dữ liệu/service
thật; runner vẫn cần Internet để tải JDK, Python, Maven dependencies và công cụ.

## 2. Khi nào chạy?

- PR hướng đến `main`: mở, mở lại hoặc push commit mới vào PR theo các sự kiện
  mặc định của `pull_request`. Kiểm tra mặc định trên merge ref để phát hiện
  vấn đề khi tích hợp với base branch, không chỉ riêng head của PR.
- Push vào `main`, kể cả commit merge: kiểm tra baseline đã nhập vào nhánh chung.
- Chạy thủ công ở tab Actions sau khi workflow có trên default branch.

Push nhánh feature chưa có PR không tự chạy. Không bật đồng thời push mọi
nhánh và PR, nên mỗi lần cập nhật PR không sinh hai bộ checks trùng nhau.
Không dùng path filter: PR chỉ sửa docs vẫn có đủ ba checks, tránh required
checks bị bỏ qua và chờ mãi. Run mới hủy run cũ của cùng PR/ref và loại sự kiện;
PR và push `main` không hủy lẫn nhau. Việc hủy chỉ tác động runner CI.

## 3. Các nhóm kiểm tra

| Check trên PR | Lệnh | Kiểm tra gì / dùng để làm gì |
|---|---|---|
| `docs-contracts` | `make ci-docs` + actionlint | Các contract/baseline/task-status checks hiện có, checksum fixture/catalog, Java Docker build inputs và regression, cấu hình mẫu/secret-pattern hygiene, Compose/network/volume/MinIO/query wiring tĩnh; YAML guardrails và cú pháp workflow/shell |
| `java-spark` | `make ci-java` | Maven Wrapper `clean verify`: toàn bộ Java tests hiện có, parser USGS/JMA, quality, revision/dedup/linking, bundle persistence, Gold transformation, ML contract; đóng gói JAR và bắt lỗi compile/dependency |
| `airflow` | `make ci-airflow` | Toàn bộ `airflow/tests/test_*.py`: SDK doubles/DAG graph, scheduling, readiness, retry, phase gates, logging, lease/recovery và QA control-plane; không cần cài Airflow trên host |

Runner: `ubuntu-24.04`; Java: Temurin **17**; Python: **3.13**.
Timeout lần lượt 15/30/10 phút. Java job cache dependency Maven, không cache
`spark/target` hoặc dữ liệu pipeline. Build vẫn chạy `clean verify` mỗi lần.
Java job đặt `SPARK_LOCAL_IP=127.0.0.1`, `SPARK_LOCAL_HOSTNAME=localhost`
cho local Spark tests để không phụ thuộc DNS/hostname của runner/container;
không thay cấu hình Spark standalone trong Compose.
Docker CLI/Compose plugin cần cho docs job, nhưng chỉ chạy `config`, không build
image, không start container và không cần kết nối daemon để kiểm tra Compose.

`ci-docs` cố ý gọi `check-config ENV_FILE=/dev/null` để chỉ kiểm tra example/repo,
không đọc `.env` riêng trên máy thành viên. Compose checks dùng `.env.example`
với placeholder, không dùng credential thật. Không copy hoặc ghi đè `.env`.
Shell syntax dùng đúng `sh`/`bash` theo shebang, bao gồm script readback có Bash arrays.
Docs job fetch đủ lịch sử và kiểm tra whitespace trên diff base → merge ref của
PR hoặc before → head của push. Không chỉ chạy `git diff --check` trên clean
checkout (vốn không thấy thay đổi đã commit). Manual/first push không có base
thì kiểm tra commit hiện tại bằng `git show --check`; SHA đi qua env được quote,
không nội suy dữ liệu PR trực tiếp vào shell script.

Các kiểm tra contract chủ yếu là rule/static checks đã có trong repo;
không phải trình kiểm tra toàn bộ link/format Markdown. Airflow checks bằng
double/mock không chứng minh scheduler thật hoặc mọi DAG import được trong
Airflow runtime thật. Java HTTP mock có thể bind loopback trên runner.

## 4. An toàn và khả năng review

- `GITHUB_TOKEN` chỉ có `contents: read`; checkout không persist credential.
- Không tham chiếu GitHub Secrets, không dùng `pull_request_target`, không chạy
  self-hosted runner nối với máy/lakehouse của thành viên.
- Các Actions chính thức được pin full commit SHA; comment ghi release để
  người review đối chiếu. Nâng cấp phải kiểm tra SHA ở repository nguồn và rerun checks.
- actionlint **1.7.12** được tải từ release chính thức, xác minh SHA256 trước chạy.
- Không dùng `continue-on-error` hay `|| true` để che lỗi test.
- Chỉ upload `spark/target/surefire-reports/*.xml`, tên `java-test-reports`, giữ
  7 ngày, kể cả khi test fail. Không upload JAR, `.env`, staging, Bronze/Silver hoặc lakehouse dump.
- YAML guardrails có regression cases cho duplicate key, quyền ghi, trigger
  nguy hiểm, path-filter, skipped/dependent jobs, mutable action reference,
  persisted credential và thay test bằng live smoke. Guardrails không thay thế
  code review: người có quyền sửa workflow cũng có thể sửa tests của nó.

Tham khảo chính thức:
[GitHub Actions workflows](https://docs.github.com/en/actions/concepts/workflows-and-actions/workflows),
[events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows),
[secure use](https://docs.github.com/en/actions/reference/security/secure-use),
[actionlint](https://github.com/rhysd/actionlint/releases/tag/v1.7.12).

## 5. Chạy tương đương trên máy local

Cần Git, GNU Make, **JDK 17**, Python 3.13, Bash/sh, `curl`/`wget`, `unzip`,
`jq`, `rg`, SHA256 tool và Docker CLI/Compose plugin. Không cần `make up`.
Nếu shell đang dùng JDK 21/25/26, chọn JDK 17 trước khi so sánh lỗi với CI.

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r tests/ci/requirements.txt
make ci-docs
make ci-java
make ci-airflow
# Hoặc ba nhóm tuần tự:
make ci
```

PyYAML chỉ phục vụ test cấu hình CI, không thêm dependency cho pipeline/Airflow.
`make test` hiện có vẫn dùng được, không yêu cầu PyYAML.
Nếu đã cài actionlint 1.7.12, có thể lint giống workflow:

```bash
actionlint .github/workflows/ci.yml
```

Lệnh `make ci-docs` chạy guardrails YAML; actionlint là bước riêng trong workflow.

## 6. Đọc kết quả và xử lý lỗi

Sau khi push branch và mở PR hướng đến `main`, vào **Checks** hoặc **Actions →
Project CI**. Chọn check đỏ và xem step lỗi; sửa code/test/config rồi push commit
mới vào cùng PR. Không tắt test để được merge.

Từ branch đang có PR:

```bash
gh pr checks
gh pr checks --watch
gh run list --workflow ci.yml --limit 10
```

- Vàng: đang chạy/chờ; đỏ: check thất bại; xanh: check đã pass trong phạm vi cấu hình.
- `cancelled`: thường do commit mới hủy run cũ; xem run của commit mới nhất.
- Java test fail: tải `java-test-reports` để xem suite/test cụ thể.
- Dependency/download timeout: xác nhận đây là lỗi hạ tầng rồi rerun failed jobs;
  không đổi assertion hoặc contract để xử lý lỗi mạng.
- Fork PR có thể chờ maintainer approve workflow trước khi runner chạy.

## 7. Required checks và giới hạn

Chưa thay đổi Settings/ruleset/branch protection của GitHub trong bản cấu hình
này. CI hiển thị kết quả **không tự động có nghĩa là chặn merge** và không tự merge PR.
Sau khi có lượt chạy trên GitHub, maintainer có thể cấu hình rule cho `main`:

1. **Settings → Rules → Rulesets** (hoặc **Branches → Branch protection**).
2. Yêu cầu PR trước merge; chọn status checks `docs-contracts`, `java-spark`,
   `airflow` từ lượt chạy thực tế, app GitHub Actions nếu UI hỗ trợ.
3. Có thể yêu cầu nhánh up-to-date và ít nhất một review độc lập theo quyết định nhóm.
4. Chặn force push/xóa `main`; không dùng admin bypass khi test thất bại.

Không bật required checks trước khi các tên check đã xuất hiện/chạy được.
Đổi tên job về sau phải đồng bộ rule để tránh PR bị chặn bởi check không còn tồn tại.
Xem [tài liệu branch protection](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches).
Phần Settings này chưa có task riêng trong backlog và cần maintainer cấu hình.

CI không thay các gate nghiệm thu thật:

- [QA-01 - Chạy E2E daily đa nguồn](../task/tasks/QA-01.md): thiếu trong CI một
  daily interval thật đi qua nguồn/Bronze/Silver/Gold/Trino với counts và `Published` evidence.
- [SEC-01 - Review secret và bề mặt truy cập](../task/tasks/SEC-01.md): secret-pattern
  scan hiện có không thay audit Git history, log/runtime, port và quyền bucket/prefix.

Không đổi trạng thái hai task này chỉ vì CI xanh. Không backfill/rebuild dữ liệu
khi thêm hoặc gỡ workflow. Rollback chỉ revert cấu hình CI; nếu đã bật required
checks thì maintainer cần cập nhật rule tương ứng trước khi gỡ job.

## 8. Evidence cục bộ

Cấu hình chưa được push/chạy trên GitHub trong lần triển khai này; không ghi
nhận GitHub checks xanh khi chưa có run thực tế. Kiểm tra ngày **2026-10-11**
(Asia/Ho_Chi_Minh):

- `make ci-docs`: PASS; các contract/fixture/task-status checks, 12 Java build-input
  regressions, CFG/Compose/MinIO/query static checks, shell syntax và YAML guardrails.
- `make ci-airflow`: **182 tests PASS** trên Python 3.13.3.
- `make test-ci-workflow`: **3 tests PASS**, gồm 10 mutation subcases; đã chạy
  với PyYAML **6.0.3** trong venv tạm.
- actionlint **1.7.12**: PASS; binary macOS ARM64 được xác minh checksum chính thức.
- Recipe Java của `ci-java` (`./mvnw --batch-mode --no-transfer-progress clean verify`)
  chạy trong image builder đã cache, Linux ARM64, Java **17.0.20.1**, Maven
  **3.9.16**, network none và Spark loopback: **253 tests, 0 failures/errors/skips;
  BUILD SUCCESS**, gồm package runner JAR. Image builder không có Make nên
  gọi trực tiếp recipe Maven, không gọi `make ci-java` bên trong container.
- Một lượt thử container không mạng trước đó lỗi hostname/DNS; đã xác định
  nguyên nhân, pin loopback cho CI/local target và chạy lại đủ suite, không bỏ test.
- `git diff --check`: PASS. GitHub Actions permissions read-only API cho thấy
  Actions đã enabled trong repo; không sửa Settings, protection hoặc GitHub Secrets.

Các xác minh này không phải một lượt Ubuntu x64/GitHub-hosted runner thực tế.
Push branch và mở PR để lấy evidence CI trên GitHub; không gọi kết quả local
là GitHub checks đã pass.
