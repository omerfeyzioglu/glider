"""HTTP client for ``glider-server``. Standard library only."""

from __future__ import annotations

import http.client
import json
import math
import random
import secrets
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Union
from urllib.parse import urlsplit

MAX_WRITE_OPS = 100
MAX_GET_IDS = 1000
MAX_SCAN_LIMIT = 10000
# Stay below the server's 1 MiB limit on the encoded write request.
_SOFT_WRITE_BYTES = 900 * 1024

# Statuses meaning "the request was not processed, retry later".
_BUSY = 429
# Statuses meaning "the server (or a proxy in front of it) failed; for a write the
# outcome is uncertain". 500 is excluded: the server uses it for corruption.
_UNCERTAIN = (502, 503, 504)


class GliderError(Exception):
    """An error response from the server, or a failure that outlived all retries.

    ``status`` is the HTTP status, or ``None`` for transport failures.
    ``request_id`` is set when a write's outcome is uncertain; it can be
    resolved later with ``GET /v1/requests/{boundary}/{nonce}``.
    """

    def __init__(
        self,
        message: str,
        status: Optional[int] = None,
        request_id: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(f"HTTP {status}: {message}" if status else message)
        self.message = message
        self.status = status
        self.request_id = request_id


class _TransportError(Exception):
    """Connection failure or timeout: no usable response was received."""


@dataclass
class Point:
    """A stored point, or a query hit when ``distance`` is set.

    Fields the server did not return are ``None``.
    """

    id: int
    vector: Optional[List[float]] = None
    metadata: Optional[Dict[str, str]] = None
    distance: Optional[float] = None


Hit = Point

PointLike = Union[Point, Mapping[str, Any]]


def _encode_point(point: PointLike) -> Dict[str, Any]:
    if isinstance(point, Point):
        pid, vector, metadata = point.id, point.vector, point.metadata
    else:
        pid, vector, metadata = point["id"], point["vector"], point.get("metadata")
    if vector is None:
        raise ValueError(f"point {pid} has no vector")
    return {
        "id": _check_id(pid),
        "vector": _vector(vector),
        "metadata": dict(metadata or {}),
    }


def _check_id(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 2**64:
        raise ValueError(f"point id must be an unsigned 64-bit integer, got {value!r}")
    return value


def _vector(values: Iterable[Any]) -> List[float]:
    vector = [float(x) for x in values]
    if not all(math.isfinite(x) for x in vector):
        raise ValueError("vector components must be finite numbers")
    return vector


def _filter(value: Optional[Mapping[str, str]]) -> Dict[str, str]:
    return dict(value) if value else {}


class Client:
    """Client for one ``glider-server`` collection.

    Retry behaviour:

    * Reads retry 429, 502-504 and transport errors with exponential backoff
      and full jitter, at most ``max_retries`` times.
    * Writes carry a client-generated request ID, so retrying cannot apply a
      write twice. 429 (nothing committed) is resent with the same ID after
      backoff. After a 502/503/504 or a transport error the outcome is
      uncertain, so the ID is first resolved with ``/v1/requests``: a
      ``retained`` write is returned as done, an ``unknown`` one is resent
      with the same ID, and ``expired``/``ahead`` raise :class:`GliderError`
      (its ``request_id`` is set) because the outcome can no longer be known.
    * 400, 401, 404, 409, 422 and other statuses are raised immediately.
    """

    def __init__(
        self,
        url: str = "http://localhost:8080",
        token: Optional[str] = None,
        timeout: float = 30,
        max_retries: int = 5,
        backoff_base: float = 0.1,
        backoff_max: float = 5.0,
    ) -> None:
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise ValueError(f"url must be http(s)://host[:port], got {url!r}")
        self._scheme = parts.scheme
        self._netloc = parts.netloc
        self._prefix = parts.path.rstrip("/")
        self._token = token
        self._timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self._sleep = time.sleep

    # -- transport ---------------------------------------------------------

    def _request(self, method: str, path: str, body: Optional[Any] = None) -> Any:
        """One HTTP exchange. Returns parsed JSON (or ``None`` for an empty body)."""
        headers = {"Accept": "application/json"}
        payload = None
        if body is not None:
            payload = json.dumps(body, allow_nan=False).encode()
            headers["Content-Type"] = "application/json"
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        cls = (
            http.client.HTTPSConnection
            if self._scheme == "https"
            else http.client.HTTPConnection
        )
        conn = cls(self._netloc, timeout=self._timeout)
        try:
            try:
                conn.request(method, self._prefix + path, body=payload, headers=headers)
                response = conn.getresponse()
                status = response.status
                raw = response.read()
            except (OSError, http.client.HTTPException) as exc:
                raise _TransportError(f"{type(exc).__name__}: {exc}") from exc
        finally:
            conn.close()
        if 200 <= status < 300:
            if not raw:
                return None
            try:
                return json.loads(raw)
            except ValueError as exc:
                raise GliderError(f"invalid JSON in response: {exc}", status) from exc
        raise GliderError(_error_message(raw, status), status)

    def _delay(self, attempt: int) -> float:
        ceiling = min(self.backoff_max, self.backoff_base * (2**attempt))
        return random.uniform(0, ceiling)

    def _read(self, method: str, path: str, body: Optional[Any] = None) -> Any:
        """A request that is safe to repeat: retried on 429/502-504/transport errors."""
        attempt = 0
        while True:
            try:
                return self._request(method, path, body)
            except _TransportError as exc:
                if attempt >= self.max_retries:
                    raise GliderError(f"{method} {path} failed: {exc}") from exc
            except GliderError as exc:
                if (exc.status != _BUSY and exc.status not in _UNCERTAIN) or (
                    attempt >= self.max_retries
                ):
                    raise
            self._sleep(self._delay(attempt))
            attempt += 1

    # -- status ------------------------------------------------------------

    def status(self) -> Dict[str, Any]:
        """``GET /v1/status``: sequence, queue, cache and clustering state."""
        return self._read("GET", "/v1/status")

    def health(self) -> bool:
        """``GET /healthz``: True while the server accepts work."""
        try:
            self._request("GET", "/healthz")
        except (GliderError, _TransportError):
            return False
        return True

    # -- writes ------------------------------------------------------------

    def write(
        self,
        upsert: Iterable[PointLike] = (),
        delete: Iterable[int] = (),
    ) -> int:
        """Apply one atomic batch of upserts then deletes; return its commit sequence.

        At most 100 operations (``upsert`` plus ``delete``) per call. An upsert
        replaces the point's vector and its complete metadata. See the class
        docstring for retry semantics: the write is applied exactly once even
        if responses are lost.
        """
        ups = [_encode_point(p) for p in upsert]
        dels = [_check_id(i) for i in delete]
        ops = len(ups) + len(dels)
        if ops == 0:
            raise ValueError("a write needs at least one upsert or delete")
        if ops > MAX_WRITE_OPS:
            raise ValueError(f"at most {MAX_WRITE_OPS} operations per write, got {ops}")
        return self._send_write(ups, dels)

    def _send_write(self, ups: List[Dict[str, Any]], dels: List[int]) -> int:
        boundary = self.status()["sequence"]
        request_id = {"boundary": boundary, "nonce": secrets.token_hex(16)}
        body = {"upsert": ups, "delete": dels, "request_id": request_id}
        resolve_path = f"/v1/requests/{boundary}/{request_id['nonce']}"
        attempt = 0
        resolve_first = False
        last = ""
        while True:
            if resolve_first:
                try:
                    found = self._request("GET", resolve_path)
                except _TransportError as exc:
                    last = f"could not resolve request: {exc}"
                except GliderError as exc:
                    if exc.status != _BUSY and exc.status not in _UNCERTAIN:
                        raise
                    last = f"could not resolve request: {exc}"
                else:
                    state = found.get("state")
                    if state == "retained":
                        return found["outcome"]["sequence"]
                    if state != "unknown":
                        raise GliderError(
                            f"write outcome cannot be determined (request state "
                            f"{state!r}); read the affected points to decide",
                            request_id=request_id,
                        )
                    resolve_first = False  # not committed: resend the same body
            if not resolve_first:
                try:
                    return self._request("POST", "/v1/write", body)["sequence"]
                except _TransportError as exc:
                    resolve_first = True
                    last = str(exc)
                except GliderError as exc:
                    if exc.status in _UNCERTAIN:
                        resolve_first = True
                    elif exc.status != _BUSY:
                        raise
                    last = str(exc)
            if attempt >= self.max_retries:
                if resolve_first:
                    raise GliderError(
                        f"write outcome uncertain after {attempt + 1} attempts: {last}",
                        request_id=request_id,
                    )
                raise GliderError(
                    f"write not accepted after {attempt + 1} attempts: {last}",
                    request_id=request_id,
                )
            self._sleep(self._delay(attempt))
            attempt += 1

    def upsert(self, points: Iterable[PointLike]) -> int:
        """Atomically upsert up to 100 points; return the commit sequence."""
        return self.write(upsert=points)

    def delete(self, ids: Iterable[int]) -> int:
        """Atomically delete up to 100 IDs (absent IDs are allowed); return the sequence."""
        return self.write(delete=ids)

    def upsert_many(self, points: Iterable[PointLike], batch_size: int = MAX_WRITE_OPS) -> int:
        """Upsert any number of points in batches; return how many were written.

        Not atomic across batches: if a batch fails, earlier batches stay
        committed. Upserts are idempotent, so after an error the whole call can
        be repeated. A batch is also cut early to stay under the server's
        1 MiB request limit.
        """
        if not 1 <= batch_size <= MAX_WRITE_OPS:
            raise ValueError(f"batch_size must be between 1 and {MAX_WRITE_OPS}")
        batch: List[Dict[str, Any]] = []
        size = 0
        total = 0
        for point in points:
            encoded = _encode_point(point)
            cost = len(json.dumps(encoded, allow_nan=False))
            if batch and (len(batch) >= batch_size or size + cost > _SOFT_WRITE_BYTES):
                self._send_write(batch, [])
                total += len(batch)
                batch, size = [], 0
            batch.append(encoded)
            size += cost
        if batch:
            self._send_write(batch, [])
            total += len(batch)
        return total

    # -- reads -------------------------------------------------------------

    def query(
        self,
        vector: Sequence[float],
        k: int = 10,
        filter: Optional[Mapping[str, str]] = None,
        exact: bool = False,
        include_metadata: bool = False,
        include_vector: bool = False,
    ) -> List[Hit]:
        """Return the ``k`` nearest points, ordered by ascending distance.

        ``filter`` is an equality conjunction on metadata. ``exact=True``
        requests exhaustive exact search (needs a server that supports it); with
        a filter it returns ``min(k, matches)``. Otherwise the search is
        approximate and a filtered query may return fewer than ``k`` hits.
        """
        body: Dict[str, Any] = {
            "vector": _vector(vector),
            "k": k,
            "include_metadata": include_metadata,
            "include_vector": include_vector,
        }
        if filter:
            body["filter"] = dict(filter)
        if exact:
            body["exact"] = True
        response = self._read("POST", "/v1/query", body)
        return [
            Hit(
                id=r["id"],
                distance=r["distance"],
                metadata=r.get("metadata"),
                vector=r.get("vector"),
            )
            for r in response["results"]
        ]

    def get(self, id: int) -> Optional[Point]:
        """Return one point (vector and metadata), or ``None`` if absent."""
        try:
            r = self._read("GET", f"/v1/points/{_check_id(id)}")
        except GliderError as exc:
            if exc.status == 404:
                return None
            raise
        return Point(id=r["id"], vector=r.get("vector"), metadata=r.get("metadata"))

    def get_many(
        self,
        ids: Sequence[int],
        include_vector: bool = True,
        include_metadata: bool = True,
    ) -> List[Optional[Point]]:
        """Return one entry per requested ID, in order; ``None`` for absent points."""
        ids = [_check_id(i) for i in ids]
        found: Dict[int, Point] = {}
        for start in range(0, len(ids), MAX_GET_IDS):
            chunk = ids[start : start + MAX_GET_IDS]
            r = self._read(
                "POST",
                "/v1/points/get",
                {
                    "ids": chunk,
                    "include_vector": include_vector,
                    "include_metadata": include_metadata,
                },
            )
            for p in r["points"]:
                found[p["id"]] = Point(
                    id=p["id"], vector=p.get("vector"), metadata=p.get("metadata")
                )
        return [found.get(i) for i in ids]

    def _scan_page(
        self,
        filter: Optional[Mapping[str, str]],
        after: Optional[int],
        limit: int,
        include_metadata: bool,
    ) -> Dict[str, Any]:
        if not 1 <= limit <= MAX_SCAN_LIMIT:
            raise ValueError(f"page size must be between 1 and {MAX_SCAN_LIMIT}")
        body: Dict[str, Any] = {
            "filter": _filter(filter),
            "limit": limit,
            "include_metadata": include_metadata,
        }
        if after is not None:
            body["after"] = after
        return self._read("POST", "/v1/scan", body)

    def scan(
        self,
        filter: Optional[Mapping[str, str]] = None,
        include_metadata: bool = False,
        page_size: int = 1000,
    ) -> Iterator[Union[int, Point]]:
        """Iterate matching points in ascending ID order.

        Yields IDs, or :class:`Point` objects with ``metadata`` when
        ``include_metadata`` is true. Pages are fetched lazily by following the
        server's ``next`` cursor; each page is read at the then-current state,
        so concurrent writes can be reflected in part of the iteration.
        """
        after: Optional[int] = None
        while True:
            page = self._scan_page(filter, after, page_size, include_metadata)
            if include_metadata:
                for p in page["points"]:
                    yield Point(id=p["id"], metadata=p.get("metadata"))
            else:
                yield from page["ids"]
            after = page.get("next")
            if after is None:
                return

    def count(self, filter: Optional[Mapping[str, str]] = None) -> int:
        """Number of points matching ``filter`` (all points when omitted)."""
        return self._scan_page(filter, None, 1, False)["matched"]

    def delete_by_filter(self, filter: Mapping[str, str]) -> int:
        """Delete every point matching ``filter``; return how many were found.

        Scans the matching IDs, then deletes them in atomic batches of 100.
        The whole operation is not atomic: an error leaves earlier batches
        committed (repeat the call to finish), and points written concurrently
        after the scan are not deleted. An empty filter is refused; to remove
        everything, delete the IDs from :meth:`scan` explicitly.
        """
        if not filter:
            raise ValueError("delete_by_filter requires a non-empty filter")
        ids = list(self.scan(filter))
        for start in range(0, len(ids), MAX_WRITE_OPS):
            self.delete(ids[start : start + MAX_WRITE_OPS])
        return len(ids)


def _error_message(raw: bytes, status: int) -> str:
    try:
        decoded = json.loads(raw)
        if isinstance(decoded, dict) and isinstance(decoded.get("error"), str):
            return decoded["error"]
    except ValueError:
        pass
    text = raw.decode("utf-8", "replace").strip()
    return text or http.client.responses.get(status, "error")
