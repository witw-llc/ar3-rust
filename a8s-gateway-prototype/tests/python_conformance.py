"""Run the existing Python receiver with Rust-emitted synthetic wire objects only."""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(os.environ.get("AR3_REFERENCE_ROOT", Path(__file__).resolve().parents[3]))
ARTIFACTS = Path(sys.argv[1])

with tempfile.TemporaryDirectory(prefix="ar3-python-conformance-") as work:
    home = Path(work)
    os.environ["A8S_HOME"] = str(home / "a8s")
    sys.path[:0] = [str(ROOT / "apps/a8s"), str(ROOT / "lib")]
    from core import Participant, inbox_dir
    from mailbox import ensure_mailboxes
    from registry import save_registry, save_namespaces
    from network import receive_envelope
    from delivery_receipt import parse_delivery_receipt
    from transports.s3 import _mailbox_for
    import services.http_get

    def forbidden_http(*args, **kwargs):
        raise AssertionError("Conformance must not make any network request")

    services.http_get.http_get_url_to_path = forbidden_http
    agents = []
    for name in ("builder", "analyst"):
        directory = home / name
        directory.mkdir()
        p = Participant(name, directory)
        ensure_mailboxes(p)
        agents.append(p)
    save_registry({p.name: {"root": str(p.root)} for p in agents})
    save_namespaces({"team": "builder"})

    attachment = json.loads((ARTIFACTS / "attachment.json").read_bytes())
    capability = attachment["files"][0]["storage"][0]
    payload = (ARTIFACTS / "result.txt").read_bytes()

    class ExactStorage:
        id = "synthetic-exact-object"

        def retrieve(self, url, dest):
            assert url == capability, "No prefix guessing or arbitrary URL retrieval"
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(payload)
            return True

    controls = []
    # Captured synthetic receiver diagnostics are not emitted into logs.
    with contextlib.redirect_stdout(io.StringIO()):
        for filename in ("normal.json", "attachment.json"):
            raw = (ARTIFACTS / filename).read_bytes()
            original = json.loads(raw)
            assert receive_envelope(raw, agents, services=[ExactStorage()], publish_control=controls.append)
            final = inbox_dir("builder") / (original["id"] + ".json")
            delivered = json.loads(final.read_bytes())
            for field in ("id", "date", "from", "to", "content"):
                assert delivered[field] == original[field], field
            if delivered["files"]:
                assert "error" not in delivered["files"][0]
                copied = (agents[0].files_bundle_dir(original["id"]) / delivered["files"][0]["filename"]).read_bytes()
                assert copied == payload
            before = len(list(inbox_dir("builder").iterdir()))
            assert receive_envelope(raw, agents, services=[ExactStorage()], publish_control=controls.append)
            assert len(list(inbox_dir("builder").iterdir())) == before
        for stage in ("inbox_write", "attachment_fetched", "attachment_failed", "deferred", "expired", "no_local_recipient"):
            raw = (ARTIFACTS / (stage + ".json")).read_bytes()
            envelope = json.loads(raw)
            parsed = parse_delivery_receipt(envelope)
            assert parsed is not None and parsed.stage == stage
            assert _mailbox_for(envelope) == "analyst"
            assert receive_envelope(raw, agents)
    assert controls and all(parse_delivery_receipt(json.loads(c)) for c in controls)
    print(json.dumps({"python_receive": "passed", "normal_and_attachment": 2, "receipt_stages": 6, "dedup": "passed", "network_requests": 0, "attachment_sha256": hashlib.sha256(payload).hexdigest()}))
