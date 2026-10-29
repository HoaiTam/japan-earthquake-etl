# Tuần 7 — Setup pilot Drive → local gate → Google Colab

Áp dụng cho [phân công tuần 7](../task/WEEK_7_COLAB_PLAN.md), 02–08/11/2026.
Đây là **hướng dẫn để thực hiện**, chưa là bằng chứng đã chạy cloud. Không cần
Docker/Java/Airflow/MinIO hoặc `.env` để đọc pilot và fit các thuật toán.

## 1. Chọn đúng dữ liệu và code

| Thành phần | Giá trị khóa ban đầu |
|---|---|
| Code base đã merge PR #108 | `5cc9e95501a9963ddda65866a14f179ce8a63258` |
| Pilot ZIP | [jma-development-pilot-team.zip](https://drive.google.com/file/d/12eLvQJzEkBuT0WYaxHYvb9KZlk5MK_vb/view) |
| Folder pilot/README | [Release pilot](https://drive.google.com/drive/folders/1b7m-DE6f3OcM4Qm8wQf46U0T8O2whLQb) |
| Folder nhóm | [IE212](https://drive.google.com/drive/u/1/folders/17Wntw5LIRvFdzaU0RpnBRL60gzsRdFTB) |
| ZIP SHA256 | `f58291b72873e1b26791fef97818fdcf5e2b028a5dfa528885a666d8cdc1b870` |
| handoff.json SHA256 | `9121d6839f9ef8e5646a06cd0bf870fcde50b65da00c03e7b268760237cddc9a` |
| Feature SHA256 | `3baddf613f237d2117248ad9095230ec17e428ea47f73d499623035d3097d2e6` |
| checksums.sha256 SHA256 | `310b5f232ae34ca4f0a197b0181bff81efe6bee38897b6e151e9b58e8d5a6502` |
| pilot_id | `pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c` |
| Expected | 279 candidate rows/2 mainshocks, PARTIAL, DEVELOPMENT_ONLY, research_ready=false |

Pins từ [handoff](./DAT-03_DRIVE_DATA_HANDOFF.md) và
[local reproduction](../evidence/DOC-01-reproduction.json). Team ZIP có verifier/
README nên SHA khác ZIP năm file cũ của DOC-01; **feature/checksum-index bytes
phải giữ nguyên**. Không dùng ZIP partial101tháng, sources hYYYY, range-inputs
hoặc warehouse làm input feature cho notebook này.

Code base là điểm bắt đầu tái lập, không là pin mọi PR tương lai. Khi đổi
module/config, ghi full commit/config SHA mới và rerun local gate cho scope
đã đổi. Không sửa pin/hash/checklist để code khác vẫn được coi là bản cũ.

## 2. Tài khoản và thư mục Drive — mỗi người tự làm

1. Đăng nhập đúng Google account đã được cấp quyền vào folder nhóm. Mở link
   pilot và thử download. Không vào được: HoaiTam xác minh recipient/quyền,
   không mở `anyone with link` hoặc dùng OAuth token của người khác.
2. Thêm shortcut của **folder release pilot** vào My Drive để mount đọc được,
   hoặc tải ZIP rồi đặt bản sao giữ nguyên bytes trong My Drive của mình.
   Không giả định Shared with me tự xuất hiện dưới MyDrive hoặc đường dẫn của
   ba tài khoản giống nhau. Ghi đường dẫn thực tế vào biến, không commit nó.
3. Tạo một folder output cá nhân, ví dụ `IE212-Colab-Outputs/HoaiTam`,
   `IE212-Colab-Outputs/rosy179`, `IE212-Colab-Outputs/ThanhTris`. Chia sẻ đúng
   recipient cần review; không ghi vào release input hoặc dùng chung run folder.
4. Mỗi người tạo notebook riêng trên Colab; chọn Python runtime **CPU/None**.
   Dùng Python 3.11–3.13 tương thích model lock. Nếu runtime thực tế ngoài
   khoảng này, dừng và giao EXP-01 kiểm compatibility; không tự bỏ pins.

Mount Drive cấp quyền rộng cho code trong notebook, không giới hạn chỉ folder
pilot. Chỉ mount sau khi đọc/trust code; không đưa `.env`, service-account key,
OAuth client JSON/token hay MinIO credential vào runtime hoặc output. SEC-01
phải audit actual session/permissions trước khi công nhận cloud acceptance.

## 3. Local gate trước fit cloud — chạy một lần theo đúng code/input

Không cần dựng lại ETL. Cài Python 3.11–3.13, Git; tải ZIP pilot bằng browser.
Clone vào **thư mục mới**, không switch checkout đang chạy catalog/worker:

```bash
git clone https://github.com/HoaiTam/japan-earthquake-etl.git ie212-colab-local
cd ie212-colab-local
git checkout --detach 5cc9e95501a9963ddda65866a14f179ce8a63258
python3 -m venv .venv-colab
source .venv-colab/bin/activate
python -m pip install -r notebooks/requirements-lock.txt
python -m pip check
```

Windows PowerShell: dùng `python -m venv .venv-colab`, rồi gọi trực tiếp
`.venv-colab/Scripts/python.exe` thay `python` trong các lệnh Python dưới;
không cần đổi execution policy/admin. Cũng có thể activate venv theo chính
sách máy. Linux không có `python3-venv` hoặc compiler để build HDBSCAN: xử lý
prerequisite, không bỏ HDBSCAN khỏi gate. Nếu repo không public, đăng nhập Git
bằng công cụ chính thức trên máy; không paste PAT vào URL/notebook.

Đối chiếu ZIP pin trước khi giải nén: macOS/Linux `shasum -a 256` hoặc
`sha256sum`; Windows `Get-FileHash -Algorithm SHA256`. Dùng công cụ giải nén
vào folder mới ngoài repo, xác nhận không có member thoát thư mục.

Từ root clone, tạo thư mục evidence mới ngoài kit và chạy verifier. Các tên
đường dẫn dưới là tên người dùng tự chọn; thay đường dẫn ZIP đã giải nén và
recipient đúng người, không dùng lại file receipt đã tồn tại:

```bash
python -B scripts/verify-team-pilot.py --kit ../jma-pilot-kit --expected-handoff-sha256 9121d6839f9ef8e5646a06cd0bf870fcde50b65da00c03e7b268760237cddc9a --recipient HoaiTam --receipt ../HoaiTam-local-pilot-receipt.json
python -B scripts/inspect-exp02-notebook.py --pilot-bundle ../jma-pilot-kit/bundle --output-json ../HoaiTam-local-baseline-inspection.json
```

Tên kit `../jma-pilot-kit` là root chứa `handoff.json`, `bundle/`, `tools/`,
không phải thư mục cha nếu chương trình unzip tạo thêm một cấp. EXP-02 mặc định
ghi results dưới staging của clone mới; chỉ chạy lần đầu vào output chưa có.
Muốn rerun: đặt `EXP02_OUTPUT_DIR` sang folder mới trước lệnh Python bằng shell
của máy; không overwrite lần đã hoàn tất. macOS/Linux dùng `export`, PowerShell
dùng `$env:EXP02_OUTPUT_DIR`.

**Gate thật** là `staging/ml-experiments/exp02-baselines/local-smoke-report.json`
hoặc dưới EXP02_OUTPUT_DIR đã chọn, không chỉ inspection JSON hoặc exit=0.
Phải có `local_smoke_status=PASSED`, `fresh_kernel_acceptance=true`,
`colab_migration_allowed=true`, đúng pilot/feature/checksum-index pins và đủ ba
baseline. Nếu fallback hoặc tự dùng synthetic 2 rows/1 mainshock: chưa đạt.
Không lấy `DOC-01-reproduction.json` thay report schema local-smoke-v2.

Lưu commit bằng `git rev-parse HEAD`, config/package hashes/versions và report
SHA ngoài kit; chuyển **report thật** đó lên Drive để Colab đối chiếu. Có thể
dùng report từ owner nếu đúng pin/code/config và nguồn tin cậy, nhưng mỗi
người vẫn tự tạo receipt download/loader của mình. Không viết report PASSED tay.

ThanhTris chạy thêm adaptive fresh-kernel smoke trên pilot thật, output mới:

```bash
python -B scripts/inspect-exp03-notebook.py --pilot-bundle ../jma-pilot-kit/bundle --output-dir staging/exp03-local-week7 --output-json ../ThanhTris-local-adaptive-inspection.json
```

Expected `runner_mode=nbclient`, `fresh_kernel_acceptance=true`, không
`FALLBACK_EXEC_WARNING`; kiểm artifacts và counts. Dependency nghiên cứu
EXP-02 còn In Progress không chặn fixture/local adaptive, nhưng không dùng
smoke này thay official EXP-03 acceptance.

## 4. Colab cell 1 — Code pin và cài thư viện

Tạo notebook bootstrap mới, các cells dưới chạy theo thứ tự. Đây là code dùng
API đã có; EXP-01 sở hữu việc đóng gói thành notebook reusable và nghiệm thu
cloud. Không mở notebook nguồn rồi tự gán cờ fresh-kernel acceptance cho nó.

```python
import json, os, sys, subprocess
from pathlib import Path

assert (3, 11) <= sys.version_info[:2] <= (3, 13), sys.version
CODE_PIN = "5cc9e95501a9963ddda65866a14f179ce8a63258"
REPO = Path("/content/ie212-colab-code")
if not REPO.exists():
    subprocess.run(["git", "clone", "https://github.com/HoaiTam/japan-earthquake-etl.git", str(REPO)], check=True)
    subprocess.run(["git", "checkout", "--detach", CODE_PIN], cwd=REPO, check=True)
actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
assert actual == CODE_PIN, "Code pin khác; không tự reset checkout"
assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO, text=True).strip(), "Code checkout có thay đổi"
subprocess.run([sys.executable, "-m", "pip", "install", "-r", str(REPO / "notebooks/requirements-lock.txt")], check=True)
subprocess.run([sys.executable, "-m", "pip", "check"], check=True)
```

Nếu đã import numpy/scipy/pyarrow trước cài version: restart session rồi chạy
lại từ cell 1, không tiếp tục với binary đã load khác version. Lock hiện pin
direct dependencies, **chưa khóa toàn bộ transitive wheels/hashes** và không
có pandas/matplotlib. Cần plots: COL-03/EXP-01 bổ sung pin có kiểm chứng trong
PR, không cài bản latest im lặng. Nếu pip check fail do Colab preinstalled
packages, ghi issue EXP-01 và môi trường cụ thể; không bỏ check hoặc claim PASS.

## 5. Colab cell 2 — Mount, copy ZIP và verify trước thực thi

```python
from google.colab import drive
drive.mount("/content/drive")

import hashlib, shutil, stat, uuid, zipfile
RECIPIENT = input("GitHub name (HoaiTam / rosy179 / ThanhTris): ").strip()
assert RECIPIENT in {"HoaiTam", "rosy179", "ThanhTris"}
ZIP_SOURCE = Path(input("Đường dẫn tuyệt đối ZIP pilot trong Drive đã mount: ").strip())
assert ZIP_SOURCE.is_file() and ZIP_SOURCE.is_relative_to(Path("/content/drive"))
SESSION = uuid.uuid4().hex
WORK = Path("/content/ie212-week7") / RECIPIENT / SESSION
WORK.mkdir(parents=True, exist_ok=False)
ZIP_LOCAL = WORK / "pilot.zip"
shutil.copyfile(ZIP_SOURCE, ZIP_LOCAL)
ZIP_PIN = "f58291b72873e1b26791fef97818fdcf5e2b028a5dfa528885a666d8cdc1b870"
assert hashlib.sha256(ZIP_LOCAL.read_bytes()).hexdigest() == ZIP_PIN, "ZIP SHA mismatch: STOP"
KIT = WORK / "kit"
with zipfile.ZipFile(ZIP_LOCAL) as archive:
    entries = archive.infolist()
    assert len(entries) == len({entry.filename for entry in entries}), "Duplicate ZIP entry"
    assert sum(entry.file_size for entry in entries) < 2_000_000, "Unexpected pilot ZIP size"
    for entry in entries:
        relative = Path(entry.filename)
        assert not relative.is_absolute() and ".." not in relative.parts
        assert "\\" not in entry.filename and not stat.S_ISLNK(entry.external_attr >> 16)
    archive.extractall(KIT)
HANDOFF_PIN = "9121d6839f9ef8e5646a06cd0bf870fcde50b65da00c03e7b268760237cddc9a"
assert hashlib.sha256((KIT / "handoff.json").read_bytes()).hexdigest() == HANDOFF_PIN
subprocess.run([
    sys.executable, "-B", str(REPO / "scripts/verify-team-pilot.py"),
    "--kit", str(KIT), "--expected-handoff-sha256", HANDOFF_PIN,
    "--recipient", RECIPIENT, "--receipt", str(WORK / "colab-pilot-receipt.json"),
], check=True)
```

Expected `TEAM_PILOT_VERIFIED`, 279/2, PARTIAL/research_ready=false. Receipt
self-report trên **Colab VM**, không thay receipt local từ ba máy của DAT-03.
Không đặt receipt/notebook/cache/venv vào kit vì verifier kiểm exact file set.
Nếu không mount được: tải ZIP bằng browser rồi upload vào Colab VM bằng Files,
vẫn kiểm cùng SHA trước giải nén; ghi rõ phương thức khác trong evidence.

## 6. Colab cell 3 — Load thật và kiểm local gate

```python
sys.dont_write_bytecode = True
sys.path.insert(0, str(REPO / "notebooks"))
from experiment_harness import load_feature_bundle, MODE_DEVELOPMENT, ColabPathAdapter, get_runtime_metadata

PILOT_BUNDLE = KIT / "bundle"
bundle = load_feature_bundle(PILOT_BUNDLE, mode=MODE_DEVELOPMENT)
PILOT_ID = "pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c"
FEATURE_PIN = "3baddf613f237d2117248ad9095230ec17e428ea47f73d499623035d3097d2e6"
INDEX_PIN = "310b5f232ae34ca4f0a197b0181bff81efe6bee38897b6e151e9b58e8d5a6502"
assert bundle["bundle_id"] == PILOT_ID and bundle["is_official"] is False
assert bundle["features_table"].num_rows == 279 and bundle["mainshocks_table"].num_rows == 2
assert bundle["checksums"]["features-part-00000.parquet"] == FEATURE_PIN
assert hashlib.sha256((PILOT_BUNDLE / "checksums.sha256").read_bytes()).hexdigest() == INDEX_PIN
LOCAL_REPORT = Path(input("Đường dẫn local-smoke-report.json thật đã chuyển lên Drive: ").strip())
local_gate = ColabPathAdapter.verify_local_smoke_gate(LOCAL_REPORT)
assert local_gate["report_version"] == "exp-02-local-smoke-v2"
assert local_gate["fresh_kernel_acceptance"] is True
assert local_gate["colab_migration_allowed"] is True and local_gate["local_smoke_status"] == "PASSED"
assert local_gate["pilot_id"] == PILOT_ID and local_gate["bundle_checksum"] == FEATURE_PIN
assert local_gate["bundle_fingerprint"]["checksums_index_sha256"] == INDEX_PIN
print("PILOT_INPUT_AND_LOCAL_GATE_VERIFIED", get_runtime_metadata(code_version=CODE_PIN, random_seed=42))
```

EXP-01 cần bổ sung gate/package/config parity có test trước khi nhận cloud
acceptance; adapter hiện chỉ kiểm report schema/status, **không tự xác thực
report xuất xứ hoặc source code pin**. Đối chiếu code/config/package bằng
evidence local, không tự sửa report hoặc dùng synthetic fingerprint thay pilot.
EDA/verify thuần đọc không cần fit gate; thiếu report chỉ chặn các bước model.

## 7. Cell 4A — Khối 2 chạy ba default baselines

```python
import json
from baseline_models import run_baseline_experiment
baseline_results = {}
for algorithm, filename in [
    ("WINDOW", "window_baseline_v1.json"),
    ("DBSCAN", "dbscan_global_dev_v1.json"),
    ("HDBSCAN_GLOBAL", "hdbscan_global_dev_v1.json"),
]:
    config = json.loads((REPO / "config/ml/models" / filename).read_text())
    result = run_baseline_experiment(
        bundle=bundle, algorithm_name=algorithm, config=config,
        model_config_version=config["model_config_version"],
        output_dir=WORK / "results" / algorithm, mode=MODE_DEVELOPMENT,
        code_version=CODE_PIN, random_seed=42,
    )
    assert result["metrics"]["total_candidates"] == 279
    assert result["metrics"]["windows_processed"] == 2
    assert result["write_result"]["has_success_marker"] is False
    baseline_results[algorithm] = result
    print(algorithm, result["metrics"])
```

Default expected tham khảo từ local cùng pins: WINDOW có255 POST candidates;
DBSCAN có mainshock-noise window; HDBSCAN_GLOBAL có thể không có productive
sequence. **Không ép kết quả đẹp** hoặc đổi config để khớp số mà chưa điều tra
package/code difference. Parity so grain/roles/noise/membership, probabilities
theo tolerance công bố (đề xuất absolute1e-8/relative1e-6 nếu chưa có khác biệt
cần giải thích); không so run ID/timestamp/runtime byte-identical. Nếu lệch,
ghi FAILED và nguyên nhân trước khi sweep. Không sinh fake local-smoke từ Colab.

## 8. Cell 4B — Khối 3 chạy adaptive

Chỉ chạy sau local adaptive smoke thật của mục3 đúng code/input. Có thể dùng
cùng bootstrap nhưng output riêng; không chờ baseline artifacts của rosy179
để fit adaptive, chỉ chờ khi muốn lập bảng so sánh bốn thuật toán.

```python
from adaptive_hdbscan import run_adaptive_experiment
config = json.loads((REPO / "config/ml/models/hdbscan_adaptive_dev_v1.json").read_text())
adaptive = run_adaptive_experiment(
    bundle=bundle, config=config,
    model_config_version=config["model_config_version"],
    output_dir=WORK / "results" / "HDBSCAN_ADAPTIVE", mode=MODE_DEVELOPMENT,
    code_version=CODE_PIN, random_seed=42, subfolder_by_run_id=True,
)
assert adaptive["write_result"]["has_success_marker"] is False
print(adaptive["metrics"], adaptive["reconciliation"])
```

Ghi per-window resolved params, raw/resolved/ambiguous counts, nested
evidence; không bỏ raw membership sau resolve. Không tự gán EXP03 flags để
inspection fallback được gọi là fresh kernel. Nếu sửa module phải local rerun.

## 9. Lưu kết quả và kiểm rerun — cả ba người

Giữ input/ZIP/kit ngoài output. Mỗi session đã có WORK mới; chạy lại toàn bộ
setup tạo session mới, không ghi đè artifacts cũ. Không tự retry vào folder
completed để che failed attempt. Persist **chỉ metadata và results đã review**:

```python
from datetime import datetime, timezone
metadata = {
    "recipient": RECIPIENT, "runtime_kind": "GOOGLE_COLAB",
    "session_id": SESSION, "created_at_utc": datetime.now(timezone.utc).isoformat(),
    "code_commit": CODE_PIN, "pilot_id": PILOT_ID,
    "feature_sha256": FEATURE_PIN, "checksums_index_sha256": INDEX_PIN,
    "local_smoke_sha256": hashlib.sha256(LOCAL_REPORT.read_bytes()).hexdigest(),
    "allowed_usage": "DEVELOPMENT_ONLY", "research_ready": False,
    "environment": get_runtime_metadata(code_version=CODE_PIN, random_seed=42),
}
(WORK / "session-metadata.json").write_text(json.dumps(metadata, indent=2))
for marker in (WORK / "results").rglob("_SUCCESS.json"):
    raise RuntimeError("Pilot không được phát official marker: " + marker.name)
OUTPUT_PARENT = Path(input("Folder output cá nhân trong Drive đã mount: ").strip())
assert OUTPUT_PARENT.is_dir() and OUTPUT_PARENT.is_relative_to(Path("/content/drive"))
assert not OUTPUT_PARENT.resolve().is_relative_to(ZIP_SOURCE.parent.resolve()), "Không ghi output vào input release"
DEST = OUTPUT_PARENT / SESSION
DEST.mkdir(exist_ok=False)
for name in ("results", "colab-pilot-receipt.json", "session-metadata.json"):
    source = WORK / name
    if source.is_dir():
        shutil.copytree(source, DEST / name)
    elif source.is_file():
        shutil.copyfile(source, DEST / name)
for source in WORK.rglob("*"):
    relative = source.relative_to(WORK)
    if source.is_file() and relative.parts[0] in {"results", "colab-pilot-receipt.json", "session-metadata.json"}:
        restored = DEST / relative
        assert hashlib.sha256(source.read_bytes()).digest() == hashlib.sha256(restored.read_bytes()).digest()
print("OUTPUT_COPY_READBACK_VERIFIED", SESSION)
```

Đọc/review nội dung trước lưu lên Drive, không chỉ quét tên file. Colab receipt
không xác thực danh tính, không là scientific review; kèm account permission
check riêng ở SEC-01 mà không commit email/token. Không xóa local dữ liệu ETL,
không upload toàn WORK/code/cache hoặc gọi copy là backup warehouse.

Nếu mount bằng đường khác/sử dụng manual Files upload, điều chỉnh biến/path
trong notebook và ghi evidence; không đổi checksum pin. Kết thúc VM có thể mất
WORK nên kiểm remote readback trước đóng session. Không hứa tài nguyên Colab
luôn có hoặc tự chạy qua đêm không giới hạn.

## 10. Thành công / lỗi thường gặp / bàn giao

| Dấu hiệu | Ý nghĩa / hành động |
|---|---|
| TEAM_PILOT_VERIFIED +279/2 + đúng pins | Input thật hợp lệ; chưa fit model |
| PASSED nhưng2rows/1window hoặc fallback | Synthetic/fallback, không nghiệm thu pilot thật |
| ZIP/hash/file-set/schema mismatch | Dừng; tải lại đúng release; không sửa manifest |
| Colab Drive path missing | Kiểm đúng account/shortcut/MyDrive; không public ACL |
| pip conflict / incompatible Python | EXP-01 kiểm lock/runtime, không bỏ version check |
| Code pin hoặc config khác local | Rerun local cho scope mới, ghi pin mới; chưa parity |
| Noise/no productive sequence | Đọc reason, không tự coi là chương trình sai |
| DBCV/Omori không đủ mẫu | Null + reason, không nhập số giả hoặc loại window |
| OUTPUT_COPY_READBACK_VERIFIED | Copy output khớp bytes; chưa đủ scientific acceptance |

Mỗi PR: notebook source sạch outputs/execution counts, config/tests, evidence
metadata nhỏ, command/setup và expected outputs thật. Không commit data ZIP,
Parquet thật, credentials, mount authorization, private logs hoặc OAuth JSON.
Chưa có Make targets COL-01..04, không ghi lệnh chạy chưa triển khai. Tests có
sẵn: `make test-exp01`, `make test-exp02`, `make test-exp03` với model venv
đã activate (targets này gọi python3, không dùng PILOT_PYTHON); không gọi Docker.
Windows không có Make có thể dùng Python của venv với
`-m unittest discover -s tests -p 'test_baseline_algorithms.py'` hoặc thay
pattern bằng `test_experiment_harness.py`/`test_adaptive_hdbscan.py`.

- DAT-03 còn full catalog/official export và receipts ba máy.
- EXP-02/03 còn official dataset acceptance; cloud pilot không đủ Done toàn task.
- EXP-04 còn scientific evaluation/Omori/stability trên research release.
- MLI-04 còn official Trino views/readback; không import development artifacts.
- SEC-01 còn actual recipient/session audit, không chứng nhận từ docs này.

Tham khảo UI/runtime/sharing từ [Google Colab FAQ](https://research.google.com/colaboratory/faq.html)
(đọc29/10/2026): VM không chia sẻ cùng notebook và có thể bị thu hồi; notebook
được chia sẻ gồm code/output; nên copy ZIP vào VM để xử lý thay vì đọc nhiều
file trực tiếp qua Drive. Mọi cloud acceptance vẫn cần evidence chạy thật.
