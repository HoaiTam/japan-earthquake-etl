---
task_id: "MLI-04"
status: "Backlog"
week: 8
block: "I - ML Integration"
workstream: "ML serving and report"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["MLI-03", "EXP-04"]
---

# MLI-04 - Tạo Trino views và static report cho experiment

## Mục đích

Cung cấp kết quả so sánh có thể kiểm chứng bằng SQL và report tĩnh, không buộc
MVP phụ thuộc Power BI hoặc notebook output.

## Phạm vi công việc

- Tạo Trino views/query cho dataset/run/algorithm, membership, summary, metrics và failure windows.
- Đối soát aftershock count, multi-sequence resolution, noise, stability và snapshot/artifact lineage.
- Tạo static report/table/plot cho Window/DBSCAN/HDBSCAN global/adaptive và limitation.
- Report phải ghi dataset ID, experiment runs, code/config version và thời điểm snapshot.

## Deliverable

- Trino ML views, verification SQL/query smoke tests.
- Reproducible static report source và output nhỏ dùng cho demo/review.
- Data dictionary/report interpretation notes.

## Tiêu chí hoàn thành

- [ ] Query chỉ đọc experiment `CANDIDATE`/`APPROVED` theo filter rõ.
- [ ] Count/summary/metric trong report khớp SQL cùng dataset/run.
- [ ] Failure/noise/sample size không bị ẩn khỏi bảng so sánh.
- [ ] Report dùng từ candidate và nêu rõ không dự đoán/causal.

## Hard dependency

- [MLI-03](./MLI-03.md)
- [EXP-04](./EXP-04.md)

## Cách làm song song

SQL/report template có thể phát triển với fixture tables ngay khi `MLI-01`
khóa schema; evidence thật cần import snapshot từ `MLI-03`.

## Ranh giới

- Không yêu cầu Power BI để task đạt `Done`.
- Không chỉnh membership hoặc scientific metric ở serving layer.
- Không hard-code số minh họa như kết quả thật.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Trino SQL, reproducible reporting, data validation

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [ML logical data model](../../specs/ML_DATA_MODEL.md)
- [MLI-03](./MLI-03.md)
- [EXP-04](./EXP-04.md)
