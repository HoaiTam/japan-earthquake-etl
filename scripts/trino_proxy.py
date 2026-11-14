"""Trino ODBC Compatibility Proxy.

Transparent local HTTP proxy between Starburst ODBC Driver (V2/V3) and open-source Trino coordinators.
Translates JSON query envelopes ({"query": "..."}) into raw plain-text SQL statements expected by Trino.
Also rewrites nextUri/infoUri/partialCancelUri in responses so clients remain on the proxy port during pagination.

Default configuration:
    Listen port: 8084 (avoids project host ports: 8080 Airflow, 8081 Trino, 8082 Spark Master UI, 9001 MinIO)
    Upstream URL: http://127.0.0.1:8081 (standard TRINO_HOST_PORT from .env.example)

Usage:
    python scripts/trino_proxy.py [--listen-host 127.0.0.1] [--listen-port 8084] [--upstream http://127.0.0.1:8081]
"""
from __future__ import annotations

import argparse
import http.server
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_LISTEN_HOST: str = "127.0.0.1"
DEFAULT_LISTEN_PORT: int = 8084
DEFAULT_UPSTREAM_URL: str = "http://127.0.0.1:8081"
CONTROL_URI_KEYS: tuple[str, ...] = ("nextUri", "infoUri", "partialCancelUri")


def extract_query_from_body(body_bytes: bytes) -> bytes:
    """Extracts raw SQL text if the incoming body is a Starburst JSON query wrapper."""
    if not body_bytes:
        return b""
    try:
        parsed = json.loads(body_bytes.decode("utf-8"))
        if isinstance(parsed, dict) and "query" in parsed:
            return str(parsed["query"]).encode("utf-8")
    except Exception:
        pass
    return body_bytes


def rewrite_uri_origin(uri_str: str, upstream_base: str, proxy_base: str) -> str:
    """Rewrites the scheme and netloc of a URI if and only if its origin matches upstream_base."""
    if not isinstance(uri_str, str) or not uri_str.strip():
        return uri_str
    try:
        parsed_uri = urllib.parse.urlparse(uri_str)
        parsed_upstream = urllib.parse.urlparse(upstream_base)
        parsed_proxy = urllib.parse.urlparse(proxy_base)

        # Exact match of scheme and netloc (e.g. '127.0.0.1:8081' != '127.0.0.1:80810')
        if (
            parsed_uri.scheme
            and parsed_uri.scheme == parsed_upstream.scheme
            and parsed_uri.netloc == parsed_upstream.netloc
        ):
            rewritten = parsed_uri._replace(scheme=parsed_proxy.scheme, netloc=parsed_proxy.netloc)
            return urllib.parse.urlunparse(rewritten)
    except Exception:
        pass
    return uri_str


def rewrite_uris_in_json_response(response_bytes: bytes, upstream_base: str, proxy_base: str) -> bytes:
    """Rewrites Trino state URIs (nextUri, infoUri, partialCancelUri) to route back through the proxy.

    Guarantees:
    - Only top-level control URI fields ('nextUri', 'infoUri', 'partialCancelUri') are rewritten.
    - Exact origin comparison via urlparse: different ports/origins (e.g. 80810) are preserved untouched.
    - Data rows in 'data', schema in 'columns', error messages, and non-control fields remain 100% untouched.
    - Integers larger than 2^53 (e.g. snapshot IDs) retain exact precision without float casting.
    """
    if not response_bytes:
        return b""
    try:
        text = response_bytes.decode("utf-8")
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            return response_bytes

        # Check if any control URI key exists
        has_control_key = any(k in parsed for k in CONTROL_URI_KEYS)
        if not has_control_key:
            return response_bytes

        modified = False
        for key in CONTROL_URI_KEYS:
            if key in parsed and isinstance(parsed[key], str):
                new_uri = rewrite_uri_origin(parsed[key], upstream_base, proxy_base)
                if new_uri != parsed[key]:
                    parsed[key] = new_uri
                    modified = True

        if not modified:
            return response_bytes

        # Python's json encoder preserves exact int numbers
        return json.dumps(parsed).encode("utf-8")
    except Exception:
        return response_bytes


class TrinoProxyHandler(http.server.BaseHTTPRequestHandler):
    upstream_url: str = DEFAULT_UPSTREAM_URL
    listen_host: str = DEFAULT_LISTEN_HOST
    listen_port: int = DEFAULT_LISTEN_PORT

    def log_message(self, format: str, *args: object) -> None:
        pass  # Quiet logging

    @property
    def proxy_base_url(self) -> str:
        return f"http://{self.listen_host}:{self.listen_port}"

    def _forward_request(self, method: str) -> None:
        body = b""
        if method == "POST":
            length = int(self.headers.get("Content-Length", 0))
            raw_body = self.rfile.read(length) if length > 0 else b""
            if self.path.startswith("/v1/statement"):
                body = extract_query_from_body(raw_body)
            else:
                body = raw_body

        target_url = f"{self.upstream_url.rstrip('/')}{self.path}"
        headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "content-length")}
        if method == "POST":
            headers["Content-Length"] = str(len(body))

        req = urllib.request.Request(target_url, data=body if method == "POST" else None, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                resp_body = resp.read()
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    resp_body = rewrite_uris_in_json_response(resp_body, self.upstream_url, self.proxy_base_url)

                self.send_response(resp.status)
                for k, v in resp.headers.items():
                    if k.lower() not in ("transfer-encoding", "content-length"):
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(resp_body)))
                self.end_headers()
                self.wfile.write(resp_body)
        except urllib.error.HTTPError as e:
            resp_body = e.read()
            content_type = e.headers.get("Content-Type", "")
            if "application/json" in content_type:
                resp_body = rewrite_uris_in_json_response(resp_body, self.upstream_url, self.proxy_base_url)

            self.send_response(e.code)
            for k, v in e.headers.items():
                if k.lower() not in ("transfer-encoding", "content-length"):
                    self.send_header(k, v)
            self.send_header("Content-Length", str(len(resp_body)))
            self.end_headers()
            self.wfile.write(resp_body)
        except Exception as exc:
            err_msg = f'{{"error": "Proxy connection failed: {exc}"}}'.encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(err_msg)))
            self.end_headers()
            self.wfile.write(err_msg)

    def do_POST(self) -> None:
        self._forward_request("POST")

    def do_GET(self) -> None:
        self._forward_request("GET")

    def do_DELETE(self) -> None:
        self._forward_request("DELETE")


def make_proxy_handler(upstream_url: str, listen_host: str, listen_port: int) -> type[TrinoProxyHandler]:
    """Factory to create a handler class bound to configured upstream and listen addresses."""
    class ConfiguredHandler(TrinoProxyHandler):
        pass

    ConfiguredHandler.upstream_url = upstream_url
    ConfiguredHandler.listen_host = listen_host
    ConfiguredHandler.listen_port = listen_port
    return ConfiguredHandler


def main() -> None:
    parser = argparse.ArgumentParser(description="Trino ODBC compatibility proxy.")
    parser.add_argument(
        "--listen-host",
        default=os.getenv("TRINO_PROXY_LISTEN_HOST", DEFAULT_LISTEN_HOST),
        help=f"Host/IP to bind proxy listener (default: {DEFAULT_LISTEN_HOST})"
    )
    parser.add_argument(
        "--listen-port",
        type=int,
        default=int(os.getenv("TRINO_PROXY_LISTEN_PORT", DEFAULT_LISTEN_PORT)),
        help=f"Port to bind proxy listener (default: {DEFAULT_LISTEN_PORT})"
    )
    parser.add_argument(
        "--upstream",
        default=os.getenv("TRINO_PROXY_UPSTREAM", DEFAULT_UPSTREAM_URL),
        help=f"Upstream Trino coordinator URL (default: {DEFAULT_UPSTREAM_URL})"
    )
    args = parser.parse_args()

    handler_cls = make_proxy_handler(args.upstream, args.listen_host, args.listen_port)
    server = http.server.ThreadingHTTPServer((args.listen_host, args.listen_port), handler_cls)
    print(f"TRINO_PROXY_READY listen={args.listen_host}:{args.listen_port} upstream={args.upstream}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nTRINO_PROXY_STOPPED", flush=True)


if __name__ == "__main__":
    main()
