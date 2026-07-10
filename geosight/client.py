#!/usr/bin/env python3
"""
Minimal GeoSight API client for related-table workflows.
"""

from __future__ import annotations

import json
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class GeoSightClient:
    """Thin JSON client for the public GeoSight API."""

    def __init__(
        self,
        base_url: str,
        authorization: str,
        user_email: str | None = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_backoff_seconds: float = 2.0,
    ):
        if not base_url:
            raise ValueError("base_url is required")
        if not authorization:
            raise ValueError("authorization is required")

        self.base_url = base_url.rstrip("/")
        self.authorization = authorization
        self.user_email = user_email
        self.timeout = timeout
        # Only transient failures are retried (network errors, 5xx): a 401/403/404
        # will not succeed on retry, retrying those would just waste time and mask
        # the real problem, they raise immediately on the first attempt.
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds

    def _build_url(self, path: str, query: dict[str, Any] | None = None) -> str:
        api_path = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}/api/v1{api_path}"
        if query:
            clean_query = {k: v for k, v in query.items() if v is not None}
            if clean_query:
                url = f"{url}?{urlencode(clean_query, doseq=True)}"
        return url

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> Any:
        url = self._build_url(path, query=query)
        body = None
        headers = {
            "Accept": "application/json",
            "Authorization": self.authorization,
        }
        if self.user_email:
            headers["GeoSight-User-Key"] = self.user_email

        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = Request(url, data=body, headers=headers, method=method.upper())

        raw = None
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    raw = response.read().decode("utf-8")
                last_exc = None
                break
            except HTTPError as exc:
                error_body = exc.read().decode("utf-8", errors="replace")
                # 5xx is transient (server-side, may resolve on retry); 4xx (auth,
                # not-found, bad-request) will not succeed on retry, fail immediately.
                if exc.code < 500 or attempt == self.max_retries:
                    raise RuntimeError(
                        f"GeoSight API {method.upper()} {url} failed with "
                        f"{exc.code}: {error_body}"
                    ) from exc
                last_exc = exc
            except URLError as exc:
                if attempt == self.max_retries:
                    raise RuntimeError(f"GeoSight API {method.upper()} {url} failed: {exc}") from exc
                last_exc = exc
            except OSError as exc:
                # Catches TimeoutError/ConnectionResetError/socket.timeout and similar:
                # urlopen only wraps connection-establishment failures in URLError,
                # a timeout or reset while reading the response body (after the
                # connection was already made) raises these as bare OSError
                # subclasses instead, bypassing the HTTPError/URLError handlers
                # above entirely if not caught here too. Placed after HTTPError/
                # URLError (both of which are themselves OSError subclasses) so
                # this only catches genuinely different transient failures, not
                # ones already handled above.
                if attempt == self.max_retries:
                    raise RuntimeError(f"GeoSight API {method.upper()} {url} failed: {exc}") from exc
                last_exc = exc
            if last_exc is not None:
                time.sleep(self.retry_backoff_seconds * attempt)

        if not raw:
            return None

        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw

    def list_related_tables(self, page: int = 1, page_size: int = 100) -> Any:
        return self._request(
            "GET",
            "/related-tables/",
            query={"page": page, "page_size": page_size},
        )

    def iter_related_tables(self, page_size: int = 100):
        page = 1
        while True:
            data = self.list_related_tables(page=page, page_size=page_size)
            rows = data.get("results", []) if isinstance(data, dict) else data
            if not rows:
                break
            for row in rows:
                yield row

            if isinstance(data, dict):
                if not data.get("next"):
                    break
            elif len(rows) < page_size:
                break

            page += 1

    def get_related_table_by_name(self, table_name: str) -> dict[str, Any] | None:
        for table in self.iter_related_tables():
            if table.get("name") == table_name:
                return table
        return None

    def create_related_table(
        self,
        name: str,
        fields_definition: list[dict[str, str]],
        description: str = "",
    ) -> dict[str, Any]:
        payload = {
            "name": name,
            "description": description,
            "fields_definition": fields_definition,
        }
        return self._request("POST", "/related-tables/", payload=payload)

    def update_related_table(
        self,
        table_id: int | str,
        name: str,
        fields_definition: list[dict[str, str]],
        description: str = "",
    ) -> dict[str, Any]:
        payload = {
            "name": name,
            "description": description,
            "fields_definition": fields_definition,
        }
        return self._request("PUT", f"/related-tables/{table_id}/", payload=payload)

    def list_related_table_rows(
        self, table_id: int | str, page: int = 1, page_size: int = 100
    ) -> Any:
        return self._request(
            "GET",
            f"/related-tables/{table_id}/data/",
            query={"page": page, "page_size": page_size},
        )

    def iter_related_table_rows(self, table_id: int | str, page_size: int = 100):
        page = 1
        while True:
            data = self.list_related_table_rows(table_id, page=page, page_size=page_size)
            rows = data.get("results", []) if isinstance(data, dict) else data
            if not rows:
                break
            for row in rows:
                yield row

            if isinstance(data, dict):
                if not data.get("next"):
                    break
            elif len(rows) < page_size:
                break

            page += 1

    def create_related_table_row(
        self, table_id: int | str, properties: dict[str, Any]
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/related-tables/{table_id}/data/",
            payload=[{"properties": properties}],
        )

    def delete_related_table_row(self, table_id: int | str, row_id: int | str) -> None:
        self._request("DELETE", f"/related-tables/{table_id}/data/{row_id}/")

    def delete_all_related_table_rows(self, table_id: int | str) -> None:
        self._request("DELETE", f"/related-tables/{table_id}/data/")
