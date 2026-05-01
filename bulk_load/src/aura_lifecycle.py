"""
aura_lifecycle.py

Thin client for the Neo4j Aura public API. Used by recreate_instance.py to
spin a fresh Aura instance for benchmarking, on the principle that loading
into a brand-new instance is cleaner than resetting an existing one (no
stale page cache, no leftover schema fragments, no WAL history to replay).

Capabilities (verified against api.neo4j.io v1):
  - OAuth2 client-credentials authentication
  - List tenants / instances
  - Get full instance config (the source of truth when cloning)
  - Create instance (returns one-time password)
  - Delete instance
  - Poll until status == "running"

Custom-endpoint binding is intentionally not implemented: the OAuth keys
provisioned for ordinary tenant operations return 403 on the
custom-endpoints API surface, and rebinding a custom endpoint is a
single-click operation in the Aura console.

Auth file format (same shape as Neo4j-credentials-*.txt):
    CLIENT_ID=<oauth client id>
    CLIENT_SECRET=<oauth client secret>
    CLIENT_NAME=<descriptive label, optional>

Usage as a library:
    creds = AuraApiCredentials.from_file(Path("/path/to/api-creds.txt"))
    client = AuraClient(creds)
    inst = client.get_instance("27ad415a")
    new = client.create_instance(spec=InstanceSpec.from_existing(inst, name=...))
    client.wait_until_running(new["id"])
"""
from __future__ import annotations

import base64
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


AURA_API_BASE = "https://api.neo4j.io"
TOKEN_PATH = "/oauth/token"

# Statuses we treat as "settled" when polling. Anything else means the
# control plane is still working on the request.
TERMINAL_OK = {"running"}
TERMINAL_ERROR = {"creation_failed", "destroyed"}


@dataclass(frozen=True)
class AuraApiCredentials:
    client_id: str
    client_secret: str
    client_name: str = ""

    @classmethod
    def from_file(cls, path: Path) -> "AuraApiCredentials":
        kv: dict[str, str] = {}
        pat = re.compile(r"^([A-Z_][A-Z0-9_]*)=(.*)$")
        for line in path.read_text().splitlines():
            m = pat.match(line.strip())
            if m:
                kv[m.group(1)] = m.group(2).strip().strip('"').strip("'")
        try:
            return cls(
                client_id=kv["CLIENT_ID"],
                client_secret=kv["CLIENT_SECRET"],
                client_name=kv.get("CLIENT_NAME", ""),
            )
        except KeyError as e:
            raise ValueError(
                f"{path} missing required key {e}. Expected CLIENT_ID and CLIENT_SECRET."
            ) from None


@dataclass
class InstanceSpec:
    """The fields that POST /v1/instances accepts. Captured from an existing
    instance via from_existing() so a recreate is byte-identical except for
    the new instance ID and password."""
    name: str
    version: str
    region: str
    memory: str
    type: str
    tenant_id: str
    cloud_provider: str
    graph_analytics_plugin: bool = False
    vector_optimized: bool = True

    @classmethod
    def from_existing(cls, inst: dict[str, Any], name: str | None = None,
                      version: str = "5") -> "InstanceSpec":
        return cls(
            name=name or inst["name"],
            version=version,
            region=inst["region"],
            memory=inst["memory"],
            type=inst["type"],
            tenant_id=inst["tenant_id"],
            cloud_provider=inst["cloud_provider"],
            graph_analytics_plugin=bool(inst.get("graph_analytics_plugin", False)),
            vector_optimized=bool(inst.get("vector_optimized", True)),
        )

    def to_create_body(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "region": self.region,
            "memory": self.memory,
            "type": self.type,
            "tenant_id": self.tenant_id,
            "cloud_provider": self.cloud_provider,
            "graph_analytics_plugin": self.graph_analytics_plugin,
            "vector_optimized": self.vector_optimized,
        }


class AuraApiError(RuntimeError):
    def __init__(self, status: int, body: str, url: str):
        super().__init__(f"Aura API {url} returned {status}: {body[:500]}")
        self.status = status
        self.body = body
        self.url = url


@dataclass
class AuraClient:
    creds: AuraApiCredentials
    base_url: str = AURA_API_BASE
    timeout: float = 30.0
    _token: str | None = field(default=None, init=False, repr=False)
    _token_expires_at: float = field(default=0.0, init=False, repr=False)

    def _ensure_token(self) -> str:
        # 30-second skew so we refresh before edge expiry.
        if self._token and time.time() < self._token_expires_at - 30:
            return self._token

        basic = base64.b64encode(
            f"{self.creds.client_id}:{self.creds.client_secret}".encode()
        ).decode()
        body = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode()
        req = urllib.request.Request(
            self.base_url + TOKEN_PATH,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            payload = json.loads(resp.read().decode())
        self._token = payload["access_token"]
        self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))
        return self._token

    def _request(self, method: str, path: str,
                 body: dict[str, Any] | None = None) -> dict[str, Any]:
        url = self.base_url + path
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            url, data=data, method=method,
            headers={
                "Authorization": f"Bearer {self._ensure_token()}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            raise AuraApiError(e.code, e.read().decode(errors="replace"), url) from None

    def list_tenants(self) -> list[dict[str, Any]]:
        return self._request("GET", "/v1/tenants").get("data", [])

    def list_instances(self, tenant_id: str | None = None) -> list[dict[str, Any]]:
        path = "/v1/instances"
        if tenant_id:
            path = f"{path}?tenantId={urllib.parse.quote(tenant_id)}"
        return self._request("GET", path).get("data", [])

    def get_instance(self, instance_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/instances/{instance_id}").get("data", {})

    def create_instance(self, spec: InstanceSpec) -> dict[str, Any]:
        """Returns the full create response, including the one-time `password`
        and the `connection_url`. The password is shown only here, so callers
        must persist it before discarding the return value."""
        return self._request("POST", "/v1/instances", body=spec.to_create_body()).get("data", {})

    def delete_instance(self, instance_id: str) -> None:
        self._request("DELETE", f"/v1/instances/{instance_id}")

    def wait_until_running(self, instance_id: str, *,
                           poll_seconds: float = 10.0,
                           max_seconds: float = 900.0,
                           on_status=lambda s: None) -> dict[str, Any]:
        """Poll until status is in TERMINAL_OK, or raise if status hits
        TERMINAL_ERROR or we time out. Aura instance creation typically
        completes in 4-8 minutes; default ceiling is 15 minutes."""
        deadline = time.time() + max_seconds
        last_status: str | None = None
        while True:
            inst = self.get_instance(instance_id)
            status = inst.get("status", "unknown")
            if status != last_status:
                on_status(status)
                last_status = status
            if status in TERMINAL_OK:
                return inst
            if status in TERMINAL_ERROR:
                raise RuntimeError(
                    f"Instance {instance_id} reached terminal error status: {status}"
                )
            if time.time() >= deadline:
                raise TimeoutError(
                    f"Instance {instance_id} did not reach running within "
                    f"{max_seconds:.0f}s (last status: {status})"
                )
            time.sleep(poll_seconds)

    def wait_until_deleted(self, instance_id: str, *,
                           poll_seconds: float = 10.0,
                           max_seconds: float = 600.0,
                           on_status=lambda s: None) -> None:
        """Poll until GET /instances/{id} returns 404. Aura returns the
        instance with status="destroying" briefly before it disappears."""
        deadline = time.time() + max_seconds
        last_status: str | None = None
        while True:
            try:
                inst = self.get_instance(instance_id)
                status = inst.get("status", "unknown")
                if status != last_status:
                    on_status(status)
                    last_status = status
            except AuraApiError as e:
                if e.status == 404:
                    on_status("deleted")
                    return
                raise
            if time.time() >= deadline:
                raise TimeoutError(
                    f"Instance {instance_id} not deleted within {max_seconds:.0f}s"
                )
            time.sleep(poll_seconds)


def write_credentials_file(path: Path, *, uri: str, username: str, password: str,
                           database: str = "neo4j") -> None:
    """Write a Neo4j-credentials-style file the rest of this repo's scripts
    can consume. Format matches Neo4j-<id>-Created-<date>.txt exactly so
    reset_to_blank_neo4j_db.sh, run_load_benchmark.sh, etc. work unchanged."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"NEO4J_URI={uri}\n"
        f"NEO4J_USERNAME={username}\n"
        f"NEO4J_PASSWORD={password}\n"
        f"NEO4J_DATABASE={database}\n"
    )
    # Owner-readable only. The password is one-time-issued by Aura; if it
    # leaks the only remediation is to rotate via the console.
    path.chmod(0o600)
