import http.server
import json
import urllib.request
import urllib.error
import sys

TARGET_HOST = "http://127.0.0.1:8083"

class TrinoProxyHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Quiet

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length > 0 else b""
        
        # Intercept JSON body on /v1/statement
        if self.path.startswith("/v1/statement"):
            try:
                parsed = json.loads(body.decode("utf-8"))
                if isinstance(parsed, dict) and "query" in parsed:
                    body = parsed["query"].encode("utf-8")
            except Exception:
                pass
        
        target_url = f"{TARGET_HOST}{self.path}"
        headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "content-length")}
        headers["Content-Length"] = str(len(body))
        
        req = urllib.request.Request(target_url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req) as resp:
                resp_body = resp.read()
                self.send_response(resp.status)
                for k, v in resp.headers.items():
                    if k.lower() not in ("transfer-encoding", "content-length"):
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(resp_body)))
                self.end_headers()
                self.wfile.write(resp_body)
        except urllib.error.HTTPError as e:
            resp_body = e.read()
            self.send_response(e.code)
            for k, v in e.headers.items():
                if k.lower() not in ("transfer-encoding", "content-length"):
                    self.send_header(k, v)
            self.send_header("Content-Length", str(len(resp_body)))
            self.end_headers()
            self.wfile.write(resp_body)

    def do_GET(self):
        target_url = f"{TARGET_HOST}{self.path}"
        headers = {k: v for k, v in self.headers.items() if k.lower() != "host"}
        req = urllib.request.Request(target_url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req) as resp:
                resp_body = resp.read()
                self.send_response(resp.status)
                for k, v in resp.headers.items():
                    if k.lower() not in ("transfer-encoding", "content-length"):
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(resp_body)))
                self.end_headers()
                self.wfile.write(resp_body)
        except urllib.error.HTTPError as e:
            resp_body = e.read()
            self.send_response(e.code)
            for k, v in e.headers.items():
                if k.lower() not in ("transfer-encoding", "content-length"):
                    self.send_header(k, v)
            self.send_header("Content-Length", str(len(resp_body)))
            self.end_headers()
            self.wfile.write(resp_body)

    def do_DELETE(self):
        target_url = f"{TARGET_HOST}{self.path}"
        headers = {k: v for k, v in self.headers.items() if k.lower() != "host"}
        req = urllib.request.Request(target_url, headers=headers, method="DELETE")
        try:
            with urllib.request.urlopen(req) as resp:
                resp_body = resp.read()
                self.send_response(resp.status)
                self.end_headers()
                self.wfile.write(resp_body)
        except urllib.error.HTTPError as e:
            self.send_response(e.code)
            self.end_headers()

server = http.server.ThreadingHTTPServer(("127.0.0.1", 8081), TrinoProxyHandler)
print("PROXY_STARTED_ON_8081", flush=True)
server.serve_forever()
