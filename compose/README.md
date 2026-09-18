# Compose assets

`compose.yaml` sẽ được đặt tại project root bởi `CMP-01`. Thư mục này chỉ dành
cho asset hỗ trợ service như script init hoặc healthcheck không thuộc module
khác.

Không đặt secret, data volume, warehouse hoặc file runtime trong thư mục này.
Mọi mount phải tuân theo
[repository mount contract](../docs/specs/REPOSITORY_LAYOUT.md#4-mount-contract).

