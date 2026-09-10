from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    OpenerDirector,
    Request,
    build_opener,
)


class HttpBoundaryError(RuntimeError):
    def __init__(self, code: str, status: Optional[int] = None) -> None:
        super().__init__(code)
        self.code = code
        self.status = status


@dataclass(frozen=True)
class HttpJsonResult:
    data: Any
    status: int
    bytes_read: int
    elapsed_ms: int
    started_at_ms: int
    received_at_ms: int
    rate_limit: Mapping[str, Optional[str]]


class JsonTransport(Protocol):
    def get_json(
        self,
        url: str,
        headers: Optional[Mapping[str, str]] = None,
    ) -> HttpJsonResult:
        ...

    def post_json(
        self,
        url: str,
        payload: Any,
        headers: Optional[Mapping[str, str]] = None,
    ) -> HttpJsonResult:
        ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _without_duplicate_keys(pairs):
    output = {}
    for key, value in pairs:
        if key in output:
            raise ValueError("duplicate json key")
        output[key] = value
    return output


class BoundedJsonClient:
    """Small fail-closed HTTPS JSON client.

    Redirects are rejected, hosts are allowlisted, response bytes are counted
    while reading, and JSON duplicate keys are not accepted.
    """

    def __init__(
        self,
        *,
        allowed_hosts: Iterable[str],
        timeout_seconds: float = 10.0,
        max_bytes: int = 256 * 1024,
        opener: Optional[OpenerDirector] = None,
    ) -> None:
        hosts = frozenset(str(item).lower() for item in allowed_hosts)
        if not hosts:
            raise ValueError("allowed_hosts cannot be empty")
        if timeout_seconds <= 0 or max_bytes <= 0:
            raise ValueError("invalid HTTP boundary")
        self._allowed_hosts = hosts
        self._timeout_seconds = timeout_seconds
        self._max_bytes = max_bytes
        self._opener = opener or build_opener(_NoRedirect())

    def _validate_url(self, url: str) -> None:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.hostname.lower() not in self._allowed_hosts
            or parsed.port not in (None, 443)
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise HttpBoundaryError("URL_NOT_ALLOWED")

    def get_json(
        self,
        url: str,
        headers: Optional[Mapping[str, str]] = None,
    ) -> HttpJsonResult:
        return self._request_json("GET", url, headers=headers)

    def post_json(
        self,
        url: str,
        payload: Any,
        headers: Optional[Mapping[str, str]] = None,
    ) -> HttpJsonResult:
        try:
            body = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise HttpBoundaryError("INVALID_REQUEST_JSON") from exc
        if len(body) > self._max_bytes:
            raise HttpBoundaryError("REQUEST_TOO_LARGE")
        return self._request_json("POST", url, headers=headers, body=body)

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        body: Optional[bytes] = None,
    ) -> HttpJsonResult:
        self._validate_url(url)
        request_headers: Dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": "meme-radar/0.1",
        }
        if method == "POST":
            request_headers["Content-Type"] = "application/json"
        for key, value in (headers or {}).items():
            name = str(key)
            text = str(value)
            if name.lower() != "authorization":
                raise HttpBoundaryError("HEADER_NOT_ALLOWED")
            if not text or len(text) > 4096 or "\r" in text or "\n" in text:
                raise HttpBoundaryError("INVALID_HEADER")
            request_headers["Authorization"] = text
        request = Request(
            url,
            data=body,
            method=method,
            headers=request_headers,
        )
        started_at_ms = int(time.time() * 1000)
        started = time.monotonic()
        try:
            with self._opener.open(
                request,
                timeout=self._timeout_seconds,
            ) as response:
                status = int(response.getcode())
                if status != 200:
                    raise HttpBoundaryError("HTTP_STATUS", status)
                content_type = str(
                    response.headers.get("content-type", "")
                ).split(";", 1)[0].strip().lower()
                if not (
                    content_type == "application/json"
                    or content_type.endswith("+json")
                ):
                    raise HttpBoundaryError("CONTENT_TYPE")
                length = response.headers.get("content-length")
                if length is not None:
                    try:
                        parsed_length = int(length)
                        if parsed_length < 0:
                            raise HttpBoundaryError("INVALID_CONTENT_LENGTH")
                        if parsed_length > self._max_bytes:
                            raise HttpBoundaryError("BODY_TOO_LARGE")
                    except ValueError as exc:
                        raise HttpBoundaryError("INVALID_CONTENT_LENGTH") from exc
                body = bytearray()
                while True:
                    chunk = response.read(16 * 1024)
                    if not chunk:
                        break
                    body.extend(chunk)
                    if len(body) > self._max_bytes:
                        raise HttpBoundaryError("BODY_TOO_LARGE")
                try:
                    text = bytes(body).decode("utf-8", errors="strict")
                    data = json.loads(
                        text,
                        object_pairs_hook=_without_duplicate_keys,
                    )
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                    raise HttpBoundaryError("INVALID_JSON") from exc
                received_at_ms = int(time.time() * 1000)
                rate_limit = {
                    "limit": response.headers.get("x-ratelimit-limit"),
                    "remaining": response.headers.get("x-ratelimit-remaining"),
                    "window": response.headers.get("x-ratelimit-window"),
                    "retry_after": response.headers.get("retry-after"),
                }
                return HttpJsonResult(
                    data=data,
                    status=status,
                    bytes_read=len(body),
                    elapsed_ms=int((time.monotonic() - started) * 1000),
                    started_at_ms=started_at_ms,
                    received_at_ms=received_at_ms,
                    rate_limit=rate_limit,
                )
        except HttpBoundaryError:
            raise
        except HTTPError as exc:
            code = "REDIRECT_REJECTED" if 300 <= exc.code < 400 else "HTTP_STATUS"
            raise HttpBoundaryError(code, exc.code) from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise HttpBoundaryError("NETWORK_ERROR") from exc
