-- Run against the BI-01/BI-02 local QA serving catalog.
-- The absent control checks a missing ID. The existing control is emitted
-- only when the catalog contains a publication that has not passed verification.
WITH catalog_non_passed AS (
    SELECT count(*) AS catalog_non_passed_publications,
           min(publication_id) AS control_id
    FROM iceberg.gold.publication_status
    WHERE verify_status IS NULL OR verify_status <> 'PASSED'
),
fixed_cases (case_name, pinned_id, expected_eligible) AS (
    VALUES
        ('published_qa', 'pub-202302-001', 1),
        ('absent_control_id', 'qa-bi05-absent-control', 0)
),
cases (case_name, pinned_id, expected_eligible) AS (
    SELECT case_name, pinned_id, expected_eligible FROM fixed_cases
    UNION ALL
    SELECT 'existing_non_passed_control', control_id, 0
    FROM catalog_non_passed
    WHERE control_id IS NOT NULL
)
SELECT
    c.case_name,
    c.pinned_id,
    c.expected_eligible,
    count(p.publication_id) AS matching_publication_rows,
    count(CASE WHEN p.verify_status = 'PASSED' THEN 1 END) AS eligible_publications,
    cat.catalog_non_passed_publications,
    max(CASE WHEN p.verify_status = 'PASSED' THEN p.bundle_sha256 END) AS bundle_sha256,
    max(CASE WHEN p.verify_status = 'PASSED' THEN p.serving_view END) AS serving_view,
    max(CASE WHEN p.verify_status = 'PASSED' THEN p.published_at_utc END) AS published_at_utc,
    max(CASE WHEN p.verify_status = 'PASSED' THEN p.max_event_time_utc END) AS data_freshness_utc,
    max(CASE WHEN p.verify_status = 'PASSED' THEN p.event_count END) AS event_count
FROM cases c
CROSS JOIN catalog_non_passed cat
LEFT JOIN iceberg.gold.publication_status p
    ON p.publication_id = c.pinned_id
GROUP BY c.case_name, c.pinned_id, c.expected_eligible, cat.catalog_non_passed_publications
ORDER BY c.case_name;
