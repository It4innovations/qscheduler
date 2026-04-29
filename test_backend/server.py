#!/usr/bin/env python3
"""Mock backend server for testing start_task."""

import http.server
import re

PORT = 8080

ROUTE_TASK   = re.compile(r"^/task$")
ROUTE_START  = re.compile(r"^/task/(\d+)/start$")


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        if ROUTE_TASK.match(self.path):
            print(f"[task]  received {len(body)} bytes")
            self._reply(200)

        elif m := ROUTE_START.match(self.path):
            task_id = m.group(1)
            print(f"[start] task_id={task_id}")
            self._reply(200)

        else:
            print(f"[404]   {self.path}")
            self._reply(404)

    def _reply(self, status: int):
        self.send_response(status)
        self.end_headers()

    def log_message(self, *_):
        pass  # silence default access log; we print our own


if __name__ == "__main__":
    with http.server.HTTPServer(("", PORT), Handler) as srv:
        print(f"listening on http://localhost:{PORT}")
        srv.serve_forever()
