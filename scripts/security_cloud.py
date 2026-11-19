"""SEC-01 bounded cloud-byte/ACL inspection; no execution, uploads or IAM writes.

Scope results are not recipient identity verification, scientific approval,
or a replacement for the historical Docker/local acceptance.
"""
import hashlib
import io
import json
from pathlib import Path
import re
import stat
import zipfile

import security_handoff as h

ROSTER = {"HoaiTam", "ThanhTris", "rosy179"}
PILOT_ID = "pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c"
HANDOFF_SHA = "9121d6839f9ef8e5646a06cd0bf870fcde50b65da00c03e7b268760237cddc9a"
OAUTH = re.compile(r'''(?i)["'](?:access_token|refresh_token|client_secret|private_key)["']\s*:\s*["'][^"']+["']''')
BEARER = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9_.~-]{16,}")


def credential_fields(value, depth=0):
    h.require(depth <= 64)
    if isinstance(value, dict):
        if value.get("type") == "service_account":
            return True
        for key, item in value.items():
            if key.lower() in {"access_token", "refresh_token", "client_secret", "private_key"} and item:
                return True
            if credential_fields(item, depth + 1):
                return True
    elif isinstance(value, list):
        return any(credential_fields(item, depth + 1) for item in value)
    return False


def digest(data):
    return hashlib.sha256(data).hexdigest()


def scan_payload(name, data, scan_text, budget, findings, proofs, prefix=""):
    """Scan compressed members and decoded values; never echo names or values."""
    h.safe_name(name)
    budget[0] += len(data); budget[1] += 1
    h.require(len(data) <= h.FILE_LIMIT and budget[0] <= h.TOTAL_LIMIT
              and budget[1] <= h.MAX_FILES)
    location = prefix + name
    if name.endswith(".zip"):
        h.require(prefix.count("!") < 2)
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            entries = archive.infolist()
            h.require(0 < len(entries) <= h.MAX_FILES
                      and sum(e.file_size for e in entries) <= h.TOTAL_LIMIT)
            seen = set()
            for entry in entries:
                h.safe_name(entry.filename)
                h.require(entry.filename not in seen and not entry.is_dir()
                          and not entry.flag_bits & 1
                          and not stat.S_ISLNK(entry.external_attr >> 16)
                          and entry.file_size <= h.FILE_LIMIT)
                seen.add(entry.filename)
                scan_payload(entry.filename, archive.read(entry), scan_text, budget,
                             findings, proofs, location + "!")
        return
    raw = data.decode("utf-8", errors="ignore")
    if name.endswith(".png"):
        h.require(data.startswith(b"\x89PNG\r\n\x1a\n"))
        from PIL import Image
        with Image.open(io.BytesIO(data)) as image:
            text = raw + "\n" + "\n".join(h.strings(image.info))
            h.require(len(text.encode()) <= h.TOTAL_LIMIT)
            image.verify()  # Decode textual metadata, not OCR of rendered pixels.
    else:
        h.require(name.endswith((".json", ".ipynb", ".parquet", ".csv", ".txt", ".md", ".py", ".sha256")))
        text = h.decoded_text(name, data)
    categories = set(scan_text(raw, ml=True)) | set(scan_text(text, ml=True))
    if OAUTH.search(raw) or OAUTH.search(text) or BEARER.search(raw) or BEARER.search(text):
        categories.add("cloud_credential_payload")
    if Path(name).name.startswith((".env", "client_secret", "token.json")):
        categories.add("forbidden_credential_file")
    if name.endswith((".json", ".ipynb")):
        value = h.strict_json(data)
        if name.endswith(".ipynb"):
            h.require(isinstance(value, dict) and value.get("nbformat") == 4
                      and isinstance(value.get("cells"), list) and value["cells"])
        if credential_fields(value):
            categories.add("cloud_credential_payload")
    file_id = digest(location.encode())
    proofs[file_id] = digest(data)
    findings.extend({"file_id_sha256": file_id, "category": c} for c in sorted(categories))


def inspect_bytes(name, data, expected_sha256, scan_text):
    result = {"sha256": digest(data), "findings": [], "pending": [], "files": {}}
    try:
        h.require(re.fullmatch(r"[a-f0-9]{64}", expected_sha256 or "")
                  and result["sha256"] == expected_sha256)
        budget = [0, 0]
        scan_payload(name, data, scan_text, budget, result["findings"], result["files"])
        result.update(scanned_bytes=budget[0], scanned_items=budget[1])
    except Exception:
        result["pending"].append("invalid_pin_format_or_unsupported_bounded_payload")
    result["passed"] = not result["findings"] and not result["pending"]
    return result


def inspect_file(path, expected_sha256, scan_text):
    path = Path(path).absolute()
    try:
        h.require(not any(p.is_symlink() for p in (path, *path.parents))
                  and path.is_file() and path.stat().st_size <= h.FILE_LIMIT)
        return inspect_bytes(path.name, path.read_bytes(), expected_sha256, scan_text)
    except Exception:
        return {"passed": False, "findings": [], "pending": ["file_missing_unsafe_or_over_budget"]}


def permission_summary(meta, permissions, roster=None):
    """roster is an operator-supplied private email mapping, not inferred by name."""
    result = {"id": meta["id"], "mime_type": meta["mimeType"],
              "findings": [], "pending": [], "roles": {}, "principal_hashes": []}
    if not permissions:
        result["pending"].append("permissions_unavailable")
    for p in permissions:
        if p.get("deleted"):
            result["pending"].append("deleted_principal_requires_review")
        if p.get("type") != "user":
            result["findings"].append("non_user_sharing_requires_owner_review")
        address = p.get("emailAddress", "").strip().lower()
        if not address:
            result["pending"].append("principal_identity_unavailable")
        else:
            result["principal_hashes"].append(digest(address.encode()))
        role = p.get("role", "unknown")
        result["roles"][role] = result["roles"].get(role, 0) + 1
        if role not in {"owner", "organizer", "fileOrganizer", "writer", "commenter", "reader"}:
            result["pending"].append("permission_role_unknown")
    result["restricted_to_users"] = bool(permissions) and not result["findings"]
    if roster is None:
        result["pending"].append("private_roster_mapping_not_confirmed")
    else:
        h.require(set(roster) == ROSTER and len(set(roster.values())) == 3
                  and all(isinstance(v, str) and "@" in v for v in roster.values()))
        expected = {digest(v.strip().lower().encode()) for v in roster.values()}
        actual = set(result["principal_hashes"])
        result["roster_exact_match"] = actual == expected
        if actual != expected:
            result["pending"].append("scope_specific_recipient_roster_requires_review")
    # Personal notebook may intentionally be owner-only: pending, never auto-share.
    result["passed"] = not result["findings"] and not result["pending"]
    return result


def permissions_for(get_json, file_id):
    """Bounded pagination; an incomplete page is never a clean ACL."""
    h.require(re.fullmatch(r"[A-Za-z0-9_-]{10,128}", file_id))
    permissions = []; token = None; seen = set()
    for _ in range(10):
        params = {"fields": "nextPageToken,permissions(id,type,role,emailAddress,deleted,permissionDetails)",
                  "pageSize": 100, "supportsAllDrives": "true"}
        if token:
            params["pageToken"] = token
        page = get_json("files/" + file_id + "/permissions", params)
        h.require(isinstance(page.get("permissions"), list))
        permissions.extend(page["permissions"])
        token = page.get("nextPageToken")
        if not token:
            return permissions
        h.require(token not in seen); seen.add(token)
    raise ValueError("SEC_CLOUD_PERMISSION_PAGINATION_INCOMPLETE")


def receipt_summary(data, expected_handoff, expected_pilot):
    """Receipt is self-reported execution evidence, NOT authenticated identity."""
    receipt = h.strict_json(data)
    h.require(receipt.get("version") == "dat03-team-receipt-v1"
              and receipt.get("status") == "TEAM_PILOT_VERIFIED"
              and receipt.get("recipient") in ROSTER
              and receipt.get("recipient_self_reported") is True
              and receipt.get("handoff_sha256") == expected_handoff
              and receipt.get("pilot_id") == expected_pilot
              and receipt.get("feature_count") == 279 and receipt.get("mainshock_count") == 2
              and receipt.get("coverage_status") == "PARTIAL"
              and receipt.get("allowed_usage") == "DEVELOPMENT_ONLY"
              and receipt.get("research_ready") is False
              and receipt.get("scientific_approval") is False)
    return {"sha256": digest(data), "recipient": receipt["recipient"],
            "recipient_self_reported": True, "identity_authenticated": False,
            "local_vs_colab_not_proven_by_this_receipt_schema": True}
