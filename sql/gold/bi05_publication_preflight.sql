-- Run against the BI-01 local QA serving catalog. The control ID must be absent.
WITH cases (case_name, pinned_id, expected_eligible) AS (
    VALUES
        ('published_qa', 'pub-202302-001', 1),
        ('no_verified_publication', 'qa-bi05-unpublished-control', 0)
)
SELECT
    c.case_name,
    c.pinned_id,
    c.expected_eligible,
    count(p.publication_id) AS eligible_publications,
    max(p.bundle_sha256) AS bundle_sha256,
    max(p.serving_view) AS serving_view,
    max(p.published_at_utc) AS published_at_utc,
    max(p.max_event_time_utc) AS data_freshness_utc,
    max(p.event_count) AS event_count
FROM cases c
LEFT JOIN iceberg.gold.publication_status p
    ON p.publication_id = c.pinned_id AND p.verify_status = 'PASSED'
GROUP BY c.case_name, c.pinned_id, c.expected_eligible
ORDER BY c.case_name;
