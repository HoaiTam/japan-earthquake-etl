# DAT-03 — Dataset dùng chung cho ba thành viên

## Tình trạng 24/10/2026

DAT-03 **In Progress**, owner HoaiTam, reviewer unassigned. Hiện phát hành
**pilot JMA thật để dùng trước** theo lựa chọn owner; chưa có research release
toàn kỳ. Bộ synthetic EXPORTED của MLD-05/MLI-02 không được gửi như research data.

File gửi nhóm trên máy chuẩn bị:
`staging/shared-datasets/jma-development-pilot-team.zip` trong checkout DAT-03.
Đây là đường dẫn local, không phải URL public; người dùng gửi ZIP và pins qua
kênh nhóm đã được duyệt. Chưa upload/thay ACL Drive, không tự tạo cloud link.

| Thuộc tính | Bản pilot gửi trước |
|---|---|
| pilot_id | pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c |
| Số dòng / mainshock | 279 candidate rows / 2 mainshock, một window self-only |
| Quyền sử dụng | DEVELOPMENT_ONLY; PARTIAL; research_ready=false |
| Kích thước ZIP | 55.408 bytes |
| SHA256 ZIP | f58291b72873e1b26791fef97818fdcf5e2b028a5dfa528885a666d8cdc1b870 |
| SHA256 handoff.json | 9121d6839f9ef8e5646a06cd0bf870fcde50b65da00c03e7b268760237cddc9a |

Giữ các pins này **ngoài ZIP** khi gửi. Đối chiếu hash ZIP trước khi chạy code;
verifier kiểm hash handoff → exact file set → hash code/data → loader semantic.
Hash không thay xác thực người gửi nếu cả archive và pins ngoài đều bị thay.

## Người nhận làm gì?

Giải nén ZIP vào thư mục mới, mở terminal trong thư mục đó. Python 3.11–3.13,
venv **ngoài** kit; không Docker/Java/MinIO/.env. Ví dụ macOS/Linux:

```bash
python3 -m venv ../jma-pilot-venv
source ../jma-pilot-venv/bin/activate
python -m pip install -r requirements.txt
python verify_dataset.py --expected-handoff-sha256 9121d6839f9ef8e5646a06cd0bf870fcde50b65da00c03e7b268760237cddc9a --recipient HoaiTam --receipt ../HoaiTam-pilot-receipt.json
```

ThanhTris/rosy179 thay **cả hai** tên ở `--recipient` và `--receipt`. PowerShell
dùng `python -m venv ../jma-pilot-venv`, kích hoạt script phù hợp trên máy hoặc
gọi trực tiếp `../jma-pilot-venv/Scripts/python.exe`; không cần đổi execution
policy hoặc chạy quyền admin. Đối chiếu ZIP bằng `Get-FileHash -Algorithm SHA256`.

Thành công: TEAM_PILOT_VERIFIED, đúng pilot_id, feature_count=279,
mainshock_count=2, coverage_status=PARTIAL, research_ready=false. Receipt là
self-report; mỗi người tự chạy trên máy mình rồi gửi cho owner. Không tạo hộ
hai người còn lại hoặc ghi approval/reviewer chưa có.

| Người nhận | Xác nhận hiện có |
|---|---|
| HoaiTam | Đã verify kit giải nén ở thư mục độc lập trên máy chuẩn bị |
| ThanhTris | Chờ receipt từ máy thành viên |
| rosy179 | Chờ receipt từ máy thành viên |

Input đặt readonly; notebook/output/venv/receipt ngoài kit. `bundle/` dùng cho
EXP-01/02 local; README trong ZIP hướng dẫn import loader. Muốn fit model cần
requirements-lock của EXP-01/02, không chỉ pyarrow của verifier. Không fallback
synthetic hoặc coi local inspection là scientific approval/Colab parity.

## Operator đóng gói lại

```bash
make test-team-pilot PILOT_PYTHON=python3
make pack-team-pilot PILOT_BUNDLE="$PILOT_BUNDLE" PILOT_PYTHON=python3 TEAM_PILOT_ZIP="$TEAM_PILOT_ZIP"
```

PILOT_BUNDLE/TEAM_PILOT_ZIP là đường dẫn thật đã chọn. Chỉ đúng 5 pilot files
cộng README, requirements, verifier, loader và handoff; không repo snapshot,
.env, logs, warehouse, notebook output hoặc package cache. ZIP deterministic;
cùng bytes reuse, khác bytes fail, không overwrite release cũ. Khi thay code/
README/data, chọn đường dẫn release mới và công bố SHA mới, không sửa pin cũ.

## Roadmap catalog nghiên cứu thật

1. **Profile:** Java JmaResearchProfileJob đọc exact archive + expected SHA,
   reuse validator/parser; ghi aggregate counts, không ghi Bronze/Silver/Gold.
   Month có event/min-max không chứng minh complete; pre-Mc/mainshock counts
   không phải canonical/eligible counts cuối. Lệnh sau không tự build JAR:

```bash
make profile-jma-research JMA_PROFILE_ARCHIVE="$ARCHIVE" JMA_PROFILE_MEMBER="$MEMBER" JMA_PROFILE_SHA256="$ARCHIVE_SHA" JMA_PROFILE_OUTPUT="$REPORT"
```

2. **Resource plan:** đo thêm năm đông sự kiện và resource tại từng tầng trước
   khi mở rollout. ORC-06 có 100.000 selected revisions/operation, 64 active pins,
   128 MiB active raw và 32 MiB/raw; affected writes <=24 tháng. Cumulative registry
   hỗ trợ 480 tháng, không có nghĩa runtime xử lý được toàn catalog trong RAM.
   MLD-05 hiện 128 mainshock/100.000 feature rows/dataset. Không tăng limit mù hoặc
   chia nhỏ rồi âm thầm coi từng phần là dataset full-period; shard/identity/
   aggregate proof cần design version và regression tests trước thực thi.
3. **Coverage/boundaries:** reproduction 225 tháng [2000-01-01,2018-10-01),
   extension 63 tháng [2018-10-01,2024-01-01), UTC. Archive JMA theo JST cần cả
   input biên/halo16s; năm 2023 kết thúc 2023-12-31T15:00Z, nên extension UTC
   còn cần nguồn đầu 2024 JST (baseline/inventory hiện chỉ đến 2023, phải đổi
   contract có version trước capture). Window ML vượt split đang bị reject,
   không clip hoặc lén dùng extension để tune reproduction.
4. **Backfill:** nguồn/release/manifest pin → immutable Bronze → full-month
   Silver → Gold quality/commit/Trino; checkpoint/rerun không nhân đôi. Năm 2023
   cần USGS overlap theo CON-01. Daily current-month PROVISIONAL không eligible.
5. **Build:** pin một Gold publication đã đủ coverage → MLI-02 audit/Mc/window/
   features/VALIDATED/EXPORTED, exact snapshot readback và immutable bundle.
6. **Release:** local loader + checksum + rerun; cả ba người nhận cùng identity
   trên máy riêng; không tự bật research_ready/scientific_approval chỉ vì EXPORTED. Cloud cần
   mở lại SEC-01 trước migration. EXP-02/03 tiếp tục local pilot trong lúc chờ;
   không làm mất status Done component của MLD-05/MLI-02.

Đã profile h2000/h2011/h2023; inventory 24 ZIP năm 2000–2023 ước tính
120.587.487 bytes nén (~115 MiB), chưa tính archive biên 2024/USGS và warehouse.
Chưa ước lượng thời gian full backfill/fit từ ba archive profile; số đo vài
giây chỉ là parser scan local, không gồm download/Spark joins/Gold/Trino/export.
Disk còn khoảng 13 GiB ở mốc đầu phiên, cần đo lại trước batch; không xóa data
hoặc Docker volume để lấy chỗ. Estimate task 16h là ban đầu và phải replan.

## Evidence và giới hạn

- [DAT-03 progress](../evidence/DAT-03_PROGRESS.md), [task](../task/tasks/DAT-03.md).
- [DAT-02](./DAT-02_JUPYTER_PILOT.md) giữ nguyên 14-day pilot evidence/lifecycle.
- [MLI-02](./MLI-02_DATASET_BUILD.md) build kỹ thuật đã Done; không thay full research.
- [EXP-02](../task/tasks/EXP-02.md)/[EXP-03](../task/tasks/EXP-03.md) còn official fit/parity/reproduction.
- [SEC-01](../task/tasks/SEC-01.md) local-only; cloud acceptance không có trong task này.
