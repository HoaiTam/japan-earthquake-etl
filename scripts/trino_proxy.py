"""Trino ODBC Compatibility Proxy.

Transparent local HTTP proxy between Starburst ODBC Driver (V2/V3) and open-source Trino coordinators.
Translates JSON query envelopes ({"query": "..."}) into raw plain-text SQL statements expected by Trino.
Also rewrites nextUri/infoUri/partialCancelUri in responses so clients remain on the proxy port during pagination.

Usage:
    python scripts/trino_proxy.py [--listen-host 127.0.0.1] [--listen-port 8082] [--upstream http://127.0.0.1:8081]
"""
from __future__ import annotations

import argparse
import http.server
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request


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


def rewrite_uris_in_json_response(response_bytes: bytes, upstream_base: str, proxy_base: str) -> bytes:
    """Rewrites Trino state URIs (nextUri, infoUri, partialCancelUri) to route back through the proxy."""
    if not response_bytes:
        return b""
    try:
        upstream_clean = upstream_base.rstrip("/")
        proxy_clean = proxy_base.rstrip("/")
        # Regex replacement across any URI fields starting with upstream_clean
        text = response_bytes.decode("utf-8")
        rewritten = re.sub(re.escape(upstream_clean), proxy_clean, text)
        return rewritten.encode("utf-8")
    except Exception:
        return response_bytes


class TrinoProxyHandler(http.server.BaseHTTPRequestHandler):
    upstream_url: str = "http://127.0.0.1:8081"
    listen_host: str = "127.0.0.1"
    listen_port: int = 8082

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
        default=os.getenv("TRINO_PROXY_LISTEN_HOST", "127.0.0.1"),
        help="Host/IP to bind proxy listener (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--listen-port",
        type=int,
        default=int(os.getenv("TRINO_PROXY_LISTEN_PORT", "8082")),
        help="Port to bind proxy listener (default: 8082)"
    )
    parser.add_argument(
        "--upstream",
        default=os.getenv("TRINO_PROXY_UPSTREAM", "http://127.0.0.1:8081"),
        help="Upstream Trino coordinator URL (default: http://127.0.0.1:8081)"
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
