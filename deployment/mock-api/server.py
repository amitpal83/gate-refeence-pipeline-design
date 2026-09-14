"""Minimal mock DOST PMS API for the GATES demo.

Both config/airbyte_manifest.yaml (the hand-rolled reader in
ingestion/ingest_api_airbyte.py) and ingestion/airbyte_cdk_connector/manifest.yaml
(the real Airbyte CDK connector) declare the same contract: GET
{base_url}/projects/monitoring returning {"data": [...records...]} — a flat
list under "data" (field_path: ["data"]). This server serves exactly that,
sourced from sample_data/project_monitoring_api.json (which nests its
records one level deeper, under data.records, for the fixture-file code
path — see ingestion/ingest_api_airbyte.py:run_from_records) so there is a
single source of truth for the demo's sample API records either way.

Stdlib-only — no need for a real web framework to serve two static records.
"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

FIXTURE_PATH = Path(__file__).parent / "project_monitoring_api.json"
PORT = 8899


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?")[0] != "/projects/monitoring":
            self.send_response(404)
            self.end_headers()
            return
        payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        records = payload["data"]["records"]
        body = json.dumps({"data": records, "total_records": len(records)}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):  # noqa: A002 — stdlib signature
        pass  # keep container logs quiet for the demo


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
