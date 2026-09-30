# Spark test fixtures

Đặt fixture nhỏ, xác định và chỉ dùng cho unit/integration test của Spark tại
đây. Ghi nguồn, schema kỳ vọng và case kiểm thử khi thêm fixture.

Fixture nguồn dùng xuyên module không copy vào thư mục này. Test Spark đọc từ
[`tests/fixtures`](../../../../../tests/fixtures/README.md) hoặc tạo một bản
copy tạm trong test resources khi build harness yêu cầu classpath resource.
