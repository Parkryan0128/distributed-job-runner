"""Exercise the production image through TLS and Caddy, without bypassing validation."""
import http.client
import json
import socket
import ssl
import sys
import time

HOST = "distributed-job-runner.ryanparkdev.com"
TLS = ssl.create_default_context(cafile=sys.argv[1]) if len(sys.argv) > 1 else ssl.create_default_context()
LOCAL = len(sys.argv) > 1

class Connection(http.client.HTTPSConnection):
    def connect(self):
        target = ("127.0.0.1", 8443) if LOCAL else (HOST, 443)
        self.sock = TLS.wrap_socket(socket.create_connection(target, timeout=10), server_hostname=HOST)

cookie = ""
def request(path, body=None, expected=200, visitor=None):
    connection = Connection(HOST, context=TLS, timeout=10)
    headers = {"Host": HOST, "Origin": "https://"+HOST, "Content-Type": "application/json", "Cookie": cookie if visitor is None else visitor}
    connection.request("GET" if body is None else "POST", path, None if body is None else json.dumps(body), headers)
    response = connection.getresponse()
    raw = response.read()
    assert response.status == expected, (path, response.status, raw[:200])
    result = json.loads(raw) if "json" in response.getheader("Content-Type", "") else raw.decode()
    cookies = response.getheader("Set-Cookie", "")
    connection.close()
    return result, cookies

for attempt in range(30):
    try:
        assert request("/readyz")[0]["status"] == "ready"
        break
    except (OSError, AssertionError):
        if attempt == 29: raise
        time.sleep(1)
assert "Queue monitor" in request("/")[0]
assert request("/app.js")[0] and request("/style.css")[0]
state, raw_cookie = request("/api/demo")
assert "Secure" in raw_cookie and "HttpOnly" in raw_cookie and "SameSite=Strict" in raw_cookie
cookie = raw_cookie.split(";")[0]
assert state["available"], "A visitor already controls the demo; do not interrupt them."
request("/api/demo", {"action":"claim"})
stream = Connection(HOST, context=TLS, timeout=10)
try:
    stream.request("GET", "/api/events", headers={"Host":HOST,"Cookie":cookie})
    response = stream.getresponse()
    assert response.status == 200 and "text/event-stream" in response.getheader("Content-Type", "")
    request("/api/demo", {"action":"start"}, expected=409, visitor="")
    job = request("/api/jobs", {"kind":"demo", "payload":{"work_ms":100,"fail_until":1}}, expected=201)[0]
    statuses = []
    deadline = time.monotonic()+20
    while time.monotonic() < deadline:
        raw = response.readline()
        assert raw, "Unexpected end of SSE"
        line = raw.decode().strip()
        if line.startswith("data: "):
            data = json.loads(line[6:])
            if isinstance(data, dict) and data.get("job", {}).get("id") == job["id"]:
                statuses.append(data["job"]["status"])
                if statuses[-1] == "succeeded": break
        # Frames have a blank separator; read it naturally in the next iteration.
    assert statuses == ["queued","running","queued","running","succeeded"], statuses
    detail = request("/api/jobs/"+job["id"])[0]
    assert len(detail["attempts"]) == 2
    workers = request("/api/workers")[0]
    assert len([w for w in workers if w["online"]]) == 2, workers
finally:
    stream.close()
    request("/api/demo", {"action":"stop"})
print("HTTPS, secure cookies, exclusive control, real retry, workers and unbuffered SSE passed.")
