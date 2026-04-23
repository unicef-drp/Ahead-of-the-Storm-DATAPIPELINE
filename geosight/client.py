#!/usr/bin/env python3
"""
Minimal GeoSight API client for related-table workflows.
"""

from __future__ import annotations

import json
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
    ):
        if not base_url:
            raise ValueError("base_url is required")
        if not authorization:
            raise ValueError("authorization is required")

        self.base_url = base_url.rstrip("/")
        self.authorization = authorization
        self.user_email = user_email
        self.timeout = timeout

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

        try:
            with urlopen(request, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"GeoSight API {method.upper()} {url} failed with "
                f"{exc.code}: {error_body}"
            ) from exc
        except URLError as exc:
            raise RuntimeError(f"GeoSight API {method.upper()} {url} failed: {exc}") from exc

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
