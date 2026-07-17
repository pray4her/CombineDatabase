from __future__ import annotations

import base64
import http.client
import json
import os
import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


class OpenSearchRequestError(RuntimeError):
    def __init__(self, message: str, status_code: int = 502) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class OpenSearchSettings:
    endpoint: str
    username: str
    password: str
    insecure: bool


ROOT_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT_DIR / ".env"


def load_dotenv_file(env_path: Path = ENV_PATH) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ[key] = value


def load_settings() -> OpenSearchSettings:
    load_dotenv_file()
    return OpenSearchSettings(
        endpoint=os.getenv("OPENSEARCH_ENDPOINT", "https://localhost:9200").rstrip("/"),
        username=os.getenv("OPENSEARCH_USERNAME", "admin"),
        password=os.getenv("OPENSEARCH_PASSWORD", os.getenv("OPENSEARCH_INITIAL_ADMIN_PASSWORD", "")),
        insecure=os.getenv("OPENSEARCH_INSECURE", "true").lower() in {"1", "true", "yes"},
    )


class OpenSearchClient:
    def __init__(self, settings: Optional[OpenSearchSettings] = None) -> None:
        self.settings = settings or load_settings()

    def _headers(self, content_type: str = "application/json") -> Dict[str, str]:
        headers = {"Content-Type": content_type}
        if self.settings.username and self.settings.password:
            token = f"{self.settings.username}:{self.settings.password}".encode("utf-8")
            headers["Authorization"] = "Basic " + base64.b64encode(token).decode("ascii")
        return headers

    def _context(self) -> Optional[ssl.SSLContext]:
        if self.settings.insecure:
            return ssl._create_unverified_context()
        return None

    def request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        content_type: str = "application/json",
    ) -> Dict[str, Any]:
        body = None
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            url=f"{self.settings.endpoint}{path}",
            method=method,
            headers=self._headers(content_type=content_type),
            data=body,
        )
        try:
            with urllib.request.urlopen(request, context=self._context(), timeout=60) as response:
                raw = response.read().decode("utf-8")
                if not raw:
                    return {}
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise OpenSearchRequestError(
                f"OpenSearch request failed: {method} {path} -> {exc.code} {error_body}",
                status_code=400 if 400 <= exc.code < 500 else 502,
            ) from exc
        except urllib.error.URLError as exc:
            raise OpenSearchRequestError(f"OpenSearch connection failed: {exc.reason}", status_code=502) from exc
        except (http.client.RemoteDisconnected, ConnectionResetError, TimeoutError, ssl.SSLError) as exc:
            raise OpenSearchRequestError(
                f"OpenSearch connection failed: {type(exc).__name__}: {exc}",
                status_code=502,
            ) from exc

    def search(self, index_name: str, body: Dict[str, Any]) -> Dict[str, Any]:
        return self.request("POST", f"/{index_name}/_search", payload=body)

    def count(self, index_name: str, query: Dict[str, Any]) -> Dict[str, Any]:
        return self.request("POST", f"/{index_name}/_count", payload={"query": query})

    def sql_explain(self, sql_request_body: Dict[str, Any]) -> Dict[str, Any]:
        return self.request("POST", "/_plugins/_sql/_explain", payload=sql_request_body)
