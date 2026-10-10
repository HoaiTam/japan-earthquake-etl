# Offline integration fixture — không phải live Bronze

`usgs_2023_window.geojson` là GeoJSON 16-event được reformat để kiểm thử offline;
SHA-256 của bytes fixture là `c7d859c290bd10c62d26305d5f368a0854187d117f42164d9f93acb3d3ed5c48`.
Không dùng SHA của raw DAT-01 cho file này. Hai JMA records trong
`testOfflineReformattedUsgsAndSyntheticJmaGoldHandoff` là dữ liệu tạo bằng tay
để kiểm tra accepted-match; không đại diện cho archive JMA thật.

Live evidence chỉ đến từ `make smoke-silver-integration`, với exact Bronze
manifest/raw SHA trong `airflow/dags/fixtures/slv_09_bronze_inputs.json`.
