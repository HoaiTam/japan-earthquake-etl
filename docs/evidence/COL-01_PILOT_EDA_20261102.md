# COL-01 — nghiệm thu pilot EDA local và Colab, 02/11/2026

Trạng thái **PASSED** theo scope [COL-01](../task/tasks/COL-01.md).
Assignee HoaiTam; reviewer độc lập chưa chỉ định. Chi tiết identity, SHA và
lịch sử lỗi nằm trong [metadata](./COL-01_PILOT_EDA_20261102.json).

## Nghiệm thu thật

- [Colab session](https://colab.research.google.com/drive/10FAialkfontu8mJZQ6igLvPHzgC_vuA9):
  restart session rồi Run all; đủ bốn code cells với execution counts 1–4.
- Runtime Linux/Python 3.13.18; pyarrow 21.0.0, numpy 2.2.2, matplotlib 3.10.0
  trong venv riêng. Report ghi thời điểm `2026-11-02T11:43:17.887542+00:00`.
- Input là pilot JMA thật, không fixture/fallback; ZIP handoff 55.488 bytes đã
  verify SHA/exact allowlist trước thực thi. Không upload credential/đổi ACL.
- Tải `col01-output.zip` 22.995 bytes về máy; whole-ZIP SHA khớp Colab:
  `8da3256ffcb39a13ec800f29199614c6c0dea99659c64b1059359e85b45c6142`.
  Exact file-set gồm `pilot-eda.json`, `windows.csv`, `window-counts.png`,
  `output-checksums.json`; SHA cả ba artifact khớp checksum index. PNG đã kiểm
  trực quan: hai cột 1/278, nhãn và footer pins đọc được, không bị cắt.
- Identity/counts/windows/coverage/scaling và cả năm input hashes khớp bản vá
  chạy local; input không đổi bytes. Không yêu cầu PNG/JSON whole-file SHA
  bằng local vì runtime metadata/rendering có thể khác.

## Kết quả và cách hiểu

| Window | Feature rows | PRE | MAINSHOCK | POST | Self-only |
|---|---:|---:|---:|---:|---|
| evt_d47fcaaf451cc2e5c3dcc8f016a999be | 1 | 0 | 1 | 0 | Có |
| evt_f0300848e0b73b5ce9497a70a74a5905 | 278 | 22 | 1 | 255 | Không |

Tổng 279 feature rows, 2 mainshocks, 279 unique candidate IDs, 279 unique cặp
mainshock/candidate; trùng grain, null và nonfinite đều 0. Count unique được
tính riêng, không mặc định mỗi feature row là một event độc lập. Hai self
vectors có bốn thành phần bằng 0. GLOBAL scaling: space 50 km, depth 30 km,
time 720 giờ; scaled vectors không có đơn vị.

Available input scope chung là 14 ngày:
`[2000-01-21T14:21:08.760Z,2000-02-04T14:21:08.760Z)`.
Required windows dài hơn và thiếu cả hai đầu; report giữ nguyên missing ranges.
Self-only không chứng minh thực tế không có dư chấn. Không suy rộng kết quả
ra toàn Nhật Bản/toàn kỳ hoặc coi temporal coverage là catalog completeness.

## Kiểm thử và bản vá môi trường

- 11 EDA/guard/real-PNG regressions đạt, gồm fresh child process với inherited
  backend không tồn tại. 17 docs regressions đạt; task status/docs/config và
  whitespace checks đạt.
- Venv `--without-pip` dùng pip host `--python <venv python> check`.
- Colab kế thừa MPLBACKEND inline không tồn tại trong venv: buộc Agg trước
  import matplotlib và khôi phục biến môi trường sau import. Không cài tùy ý
  vào host hoặc bỏ pin để vượt lỗi. Hai lần lỗi cũ vẫn được giữ trong metadata.
- Notebook source sạch output/execution counts; xem [cách chạy lại](../runbooks/COL-01_PILOT_EDA.md).

## Ranh giới

Pilot vẫn **PARTIAL / DEVELOPMENT_ONLY / research_ready=false**. COL-01 chỉ EDA,
không fit/tune, scientific approval, EXPORTED hay ghi official marker.
[DAT-03 — bộ nghiên cứu và bàn giao](../task/tasks/DAT-03.md) còn acceptance
catalog/official export/receipts. [COL-03](../task/tasks/COL-03.md) phụ trách
baseline metrics/plots; [EXP-04](../task/tasks/EXP-04.md) phụ trách evaluation
và stability. COL-01 Done không thay acceptance các task này.
