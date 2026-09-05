"""GET /api/state — returns the latest paper_state.json from Upstash Redis."""
import json
import os
from http.server import BaseHTTPRequestHandler
from upstash_redis import Redis


redis = Redis(
    url=os.environ["KV_REST_API_URL"],
    token=os.environ["KV_REST_API_TOKEN"],
)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            data = redis.get("paper_state")
            if data is None:
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error":"No state data yet. Is paper_trader sync running?"}')
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            # data is already a JSON string
            if isinstance(data, str):
                self.wfile.write(data.encode())
            else:
                self.wfile.write(json.dumps(data).encode())
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()
