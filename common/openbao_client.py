"""Minimal OpenBao client for pipeline tasks: AppRole login, Transit batch
calls (HMAC, encrypt, decrypt) and KV v2 reads, over plain HTTP.

Used by common/pii_protection.py. It has no dependency on the rest of the
pipeline, so any new pipeline can import it.

Fail closed: every error raises OpenBaoError. Nothing here falls back to
returning plaintext or a partial result.

Credentials come from an AppRole file with ROLE_ID= and SECRET_ID= lines
(see deployment/openbao/bootstrap.sh). The loader role may HMAC and encrypt.
Only the reader role may decrypt.
"""
from __future__ import annotations

import base64
import os
import re
import time
from pathlib import Path
from typing import Callable, Sequence

import requests

BATCH_SIZE = 500
DEFAULT_TIMEOUT_SECONDS = 10
TOKEN_RENEW_MARGIN_SECONDS = 60
CONNECTION_RETRIES = 2

_CIPHERTEXT_VERSION = re.compile(r"^vault:v(\d+):")


class OpenBaoError(RuntimeError):
    """Raised for any OpenBao failure. `status` is the HTTP status when there
    was one (403 means the role's policy denied the call)."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def parse_env_text(text: str) -> dict[str, str]:
    """Parse KEY=VALUE lines, ignoring blanks and comments."""
    values: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def ciphertext_key_version(ciphertext: str) -> int:
    """Key version embedded in a Transit ciphertext or HMAC, e.g. vault:v2:..."""
    match = _CIPHERTEXT_VERSION.match(ciphertext)
    if not match:
        raise ValueError("Value is not a Transit ciphertext (expected 'vault:vN:...')")
    return int(match.group(1))


def _b64_encode(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _b64_decode(text: str) -> str:
    return base64.b64decode(text).decode("utf-8")


class OpenBaoClient:
    def __init__(
        self,
        addr: str | None,
        role_id: str,
        secret_id: str,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        session: requests.Session | None = None,
    ) -> None:
        if not addr:
            raise OpenBaoError("OpenBao address is not set (OPENBAO_ADDR)")
        self.addr = addr.rstrip("/")
        self._role_id = role_id
        self._secret_id = secret_id
        self._timeout = timeout
        self._session = session or requests.Session()
        self._token: str | None = None
        self._token_expiry = 0.0

    @classmethod
    def from_approle_text(cls, text: str, addr: str | None = None, **kwargs) -> "OpenBaoClient":
        creds = parse_env_text(text)
        if "ROLE_ID" not in creds or "SECRET_ID" not in creds:
            raise OpenBaoError("AppRole credentials must contain ROLE_ID and SECRET_ID")
        return cls(addr or os.getenv("OPENBAO_ADDR"), creds["ROLE_ID"], creds["SECRET_ID"], **kwargs)

    @classmethod
    def from_env(cls, approle_file: str | None = None, addr: str | None = None, **kwargs) -> "OpenBaoClient":
        """Build a client from OPENBAO_ADDR and the file named by
        OPENBAO_APPROLE_FILE (or the arguments)."""
        path = approle_file or os.getenv("OPENBAO_APPROLE_FILE")
        if not path:
            raise OpenBaoError("AppRole file is not set (OPENBAO_APPROLE_FILE)")
        try:
            text = Path(path).read_text(encoding="utf-8")
        except OSError as exc:
            raise OpenBaoError(f"Cannot read AppRole file {path}: {exc}") from exc
        return cls.from_approle_text(text, addr=addr, **kwargs)

    # --- authentication -------------------------------------------------

    def login(self) -> None:
        body = self._send(
            "POST", "auth/approle/login",
            {"role_id": self._role_id, "secret_id": self._secret_id},
            authenticated=False,
        )
        auth = body.get("auth") or {}
        token = auth.get("client_token")
        if not token:
            raise OpenBaoError("AppRole login returned no token")
        self._token = token
        self._token_expiry = time.monotonic() + float(auth.get("lease_duration") or 0)

    def _valid_token(self) -> str:
        if self._token is None or time.monotonic() >= self._token_expiry - TOKEN_RENEW_MARGIN_SECONDS:
            self.login()
        assert self._token is not None
        return self._token

    # --- transit --------------------------------------------------------

    def hmac_batch(self, key: str, values: Sequence[str | None]) -> list[str | None]:
        """Deterministic pseudonyms. None and empty strings come back as None."""
        return self._transit_batch(
            "hmac", key, values, "input", "hmac",
            encode=_b64_encode, decode=lambda value: value,
            extra={"algorithm": "sha2-256"},
        )

    def encrypt_batch(self, key: str, values: Sequence[str | None]) -> list[str | None]:
        """Ciphertexts (vault:vN:...) in the same order. None and empty
        strings come back as None."""
        return self._transit_batch(
            "encrypt", key, values, "plaintext", "ciphertext",
            encode=_b64_encode, decode=lambda value: value,
        )

    def decrypt_batch(self, key: str, ciphertexts: Sequence[str | None]) -> list[str | None]:
        """Original values. Needs the reader role. Every call is audited."""
        return self._transit_batch(
            "decrypt", key, ciphertexts, "ciphertext", "plaintext",
            encode=lambda value: value, decode=_b64_decode,
        )

    def _transit_batch(
        self,
        operation: str,
        key: str,
        values: Sequence[str | None],
        field: str,
        result_field: str,
        encode: Callable[[str], str],
        decode: Callable[[str], str],
        extra: dict | None = None,
    ) -> list[str | None]:
        results: list[str | None] = [None] * len(values)
        positions = [i for i, value in enumerate(values) if value not in (None, "")]
        for start in range(0, len(positions), BATCH_SIZE):
            chunk = positions[start:start + BATCH_SIZE]
            payload = {
                "batch_input": [{field: encode(values[i])} for i in chunk],  # type: ignore[arg-type]
                **(extra or {}),
            }
            body = self._send("POST", f"transit/{operation}/{key}", payload)
            batch_results = (body.get("data") or {}).get("batch_results")
            if batch_results is None or len(batch_results) != len(chunk):
                raise OpenBaoError(f"transit/{operation}/{key} returned an unexpected batch result")
            for position, item in zip(chunk, batch_results):
                if item.get("error"):
                    raise OpenBaoError(f"transit/{operation}/{key} failed for an item: {item['error']}")
                results[position] = decode(item[result_field])
        return results

    # --- kv v2 ----------------------------------------------------------

    def read_secret(self, path: str) -> dict:
        """Read a KV v2 secret under the `kv` mount, e.g. read_secret('geo/postgis')."""
        body = self._send("GET", f"kv/data/{path.lstrip('/')}")
        return (body.get("data") or {}).get("data") or {}

    # --- http -----------------------------------------------------------

    def _send(self, method: str, path: str, payload: dict | None = None, authenticated: bool = True) -> dict:
        headers = {"X-Vault-Token": self._valid_token()} if authenticated else {}
        url = f"{self.addr}/v1/{path}"
        last_error: Exception | None = None
        for attempt in range(CONNECTION_RETRIES + 1):
            try:
                response = self._session.request(
                    method, url, json=payload, headers=headers, timeout=self._timeout,
                )
                break
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = exc
                if attempt < CONNECTION_RETRIES:
                    time.sleep(attempt + 1)
        else:
            raise OpenBaoError(f"OpenBao is unreachable at {self.addr}: {last_error}") from last_error

        if response.status_code >= 400:
            try:
                errors = response.json().get("errors")
            except ValueError:
                errors = response.text[:200]
            raise OpenBaoError(
                f"{method} {path} failed with HTTP {response.status_code}: {errors}",
                status=response.status_code,
            )
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError as exc:
            raise OpenBaoError(f"{method} {path} returned a response that is not JSON") from exc
