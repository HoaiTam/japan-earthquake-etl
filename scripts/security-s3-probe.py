"""SEC-01 exact-key S3 permission probes; executed inside the verified pipeline container."""
import hashlib
import json
import os
import uuid


def run(context):
    result = {"checks": [], "created_keys": [], "retained_keys": [], "passed": False}
    def check(name, passed):
        result["checks"].append({"check": name, "passed": bool(passed)})
    try:
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config
        from botocore.exceptions import ClientError
        common = dict(endpoint_url=context["endpoint"], region_name="us-east-1")
        client = boto3.client(
            "s3", **common, aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
            config=Config(signature_version="s3v4", connect_timeout=5, read_timeout=5,
                          retries={"total_max_attempts": 1}, s3={"addressing_style": "path"}),
        )
        anonymous = boto3.client(
            "s3", **common, aws_access_key_id="anonymous", aws_secret_access_key="anonymous",
            config=Config(signature_version=UNSIGNED, connect_timeout=5, read_timeout=5,
                          retries={"total_max_attempts": 1}, s3={"addressing_style": "path"}),
        )
        bucket = context["bucket"]
        suffix = "_security/sec-01-" + uuid.uuid4().hex + "/probe.txt"
        payload = b"SEC-01 harmless scoped permission probe\n"
        mutable = []
        def denied(name, operation, unexpected_write_key=None):
            try:
                response = operation()
                if isinstance(response, dict) and "Body" in response:
                    response["Body"].close()
                if unexpected_write_key is not None:
                    result["created_keys"].append(unexpected_write_key)
                    result["retained_keys"].append(unexpected_write_key)
                check(name, False)
            except ClientError as error:
                # Network/signature/not-found errors are not authorization-denial evidence.
                check(name, error.response.get("Error", {}).get("Code") == "AccessDenied"
                      and error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 403)
        try:
            for prefix in context["prefixes"]:
                key = prefix + "/" + suffix
                client.put_object(Bucket=bucket, Key=key, Body=payload)
                result["created_keys"].append(key)
                if prefix == context["prefixes"][0]:
                    result["retained_keys"].append(key)
                else:
                    mutable.append(key)
                body = client.get_object(Bucket=bucket, Key=key)["Body"]
                try:
                    check("pipeline_read_" + prefix, body.read(len(payload) + 1) == payload)
                finally:
                    body.close()
                listed = client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
                check("pipeline_list_" + prefix, any(row["Key"] == key for row in listed.get("Contents", [])))
            bronze_key = result["created_keys"][0]
            denied("bronze_delete_denied", lambda: client.delete_object(Bucket=bucket, Key=bronze_key))
            # A successful delete is a policy failure, never interpreted as deny evidence.
            body = client.get_object(Bucket=bucket, Key=bronze_key)["Body"]
            try:
                check("bronze_still_readable", body.read(len(payload) + 1) == payload)
            finally:
                body.close()
            denied("anonymous_read_denied", lambda: anonymous.get_object(Bucket=bucket, Key=bronze_key))
            denied("anonymous_list_denied", lambda: anonymous.list_objects_v2(Bucket=bucket, Prefix=bronze_key, MaxKeys=1))
            outside = "_security-denied/sec-01-" + uuid.uuid4().hex + "/probe.txt"
            denied("outside_prefix_read_denied", lambda: client.get_object(Bucket=bucket, Key=outside))
            denied("outside_prefix_list_denied", lambda: client.list_objects_v2(Bucket=bucket, Prefix=outside, MaxKeys=1))
            denied("outside_prefix_write_denied", lambda: client.put_object(Bucket=bucket, Key=outside, Body=payload), outside)
        finally:
            for key in mutable:
                try:
                    client.delete_object(Bucket=bucket, Key=key)
                    check("mutable_probe_cleanup", True)
                except Exception:
                    result["retained_keys"].append(key)
                    check("mutable_probe_cleanup", False)
        result["passed"] = bool(result["checks"]) and all(row["passed"] for row in result["checks"])
        result["payload_sha256"] = hashlib.sha256(payload).hexdigest()
    except Exception:
        result["reason"] = "PERMISSION_PROBE_FAILED_REDACTED"
    print(json.dumps(result, sort_keys=True))
