"""POST /api/push — paper_trader pushes state here. Protected by API key."""
import json
import os
from http.server import BaseHTTPRequestHandler
from upstash_redis import Redis


redis = Redis(
    url=os.environ["KV_REST_API_URL"],
    token=os.environ["KV_REST_API_TOKEN"],
)

API_KEY = os.environ.get("DASHBOARD_API_KEY", "")


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        # Verify API key
        auth = self.headers.get("Authorization", "")
        if not API_KEY or auth != f"Bearer {API_KEY}":
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error":"Unauthorized"}')
            return

        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length).decode()

            # Validate it's valid JSON
            json.loads(body)

            # Store in Redis with 60s TTL (stale data = offline indicator)
            redis.set("paper_state", body, ex=60)

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        except json.JSONDecodeError:
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error":"Invalid JSON"}')
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()
