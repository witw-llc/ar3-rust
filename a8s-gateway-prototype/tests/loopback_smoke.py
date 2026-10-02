"""Exercise a release binary using temporary synthetic state and loopback HTTP."""
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import urllib.request

binary = Path(sys.argv[1]).resolve()
with tempfile.TemporaryDirectory(prefix="ar3-loopback-") as work:
    root = Path(work)
    root.chmod(0o700)
    db = root / "state.sqlite"
    objects = root / "objects"
    subprocess.run([str(binary), "init", str(db), "00000000000000000000000000"], check=True, capture_output=True)

    def start():
        proc = subprocess.Popen([str(binary), "serve", str(db), str(objects), "127.0.0.1:0"], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        line = proc.stderr.readline()
        match = re.search(r"127\.0\.0\.1:(\d+)", line)
        if not match:
            proc.kill()
            raise AssertionError("Local server did not start")
        return proc, "http://127.0.0.1:" + match.group(1)

    def stop(proc):
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=5) == 0
        proc.stderr.close()

    proc, base = start()
    try:
        def request(method, path, seat, key=None, payload=None):
            body = None if payload is None else json.dumps(payload).encode()
            headers = {"Authorization": "Bearer " + seat + "-test", "Content-Type": "application/json"}
            if key:
                headers["Idempotency-Key"] = key
            req = urllib.request.Request(base + path, data=body, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=5) as response:
                assert response.headers["Cache-Control"] == "no-store"
                return json.load(response)

        ready = request("GET", "/health/ready", "analyst")
        idle_rss_kib = int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(proc.pid)], text=True))
        accepted = request("POST", "/v1/messages", "analyst", "smoke-send", {"recipient": "builder", "content": "synthetic λ $ `\n", "attachment_ids": []})
        op = accepted["operation_id"]
        assert request("POST", "/v1/operations/" + op + "/publish", "analyst", "smoke-publish", {})["state"] == "published"
        assert request("POST", "/v1/inboxes/builder/poll", "builder", "smoke-poll", {})["custody"] == 1
        c = request("POST", "/v1/inboxes/builder/claims", "builder", "smoke-claim", {"consumer_id": "builder-device", "max_messages": 1, "wait_seconds": 0})["claims"][0]
        disposition = {"lease_generation": c["lease_generation"], "lease_proof": c["lease_proof"], "outcome": "handled", "handling_reference": "smoke-result"}
        assert request("POST", "/v1/deliveries/" + c["delivery_id"] + "/disposition", "builder", "smoke-ack", disposition)["outcome"] == "handled"
        stop(proc)
        proc, base = start()
        assert request("GET", "/v1/inboxes/builder/messages", "builder")["messages"][0]["state"] == "handled"
        assert request("POST", "/v1/inboxes/builder/claims", "builder", "smoke-after-restart", {"consumer_id": "builder-device", "max_messages": 1, "wait_seconds": 0})["claims"] == []
        print(json.dumps({"loopback_smoke": "passed", "handled_after_restart": True, "graceful_sigterm": True, "sqlite": ready["sqlite"], "idle_rss_kib": idle_rss_kib, "binary_bytes": binary.stat().st_size, "network": "loopback only", "synthetic_messages": 1, "load_test": False}))
    finally:
        if proc.poll() is None:
            stop(proc)
