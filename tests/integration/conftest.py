"""Shared fixtures for prefect-couchbase integration tests.

These tests exercise the connector and credentials block against a **live**
Couchbase cluster (password authentication only; certificate authentication is
intentionally out of scope). Configure the target cluster through environment
variables; sensible defaults match a local Couchbase Server:

- ``COUCHBASE_CONNECTION_STRING`` (default ``couchbase://localhost``)
- ``COUCHBASE_USERNAME``          (default ``admin``)
- ``COUCHBASE_PASSWORD``          (default ``Password@12345``)
- ``COUCHBASE_BUCKET``            (default ``test_bucket``)
- ``COUCHBASE_SCOPE``             (default ``test_scope``)
- ``COUCHBASE_COLLECTION``        (default ``test_collection``)

When no cluster is reachable, the whole integration suite is skipped rather than
failing, so ``uv run pytest`` stays green in environments without Couchbase.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from collections.abc import Iterator
from datetime import timedelta

import pytest

from prefect_couchbase import CouchbaseConnector, CouchbaseCredentials

READY_TIMEOUT = timedelta(seconds=10)


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@pytest.fixture(scope="session")
def connection_settings() -> dict[str, str]:
    """Resolve live-cluster connection settings from the environment."""
    return {
        "connection_string": _env(
            "COUCHBASE_CONNECTION_STRING", "couchbase://localhost"
        ),
        "username": _env("COUCHBASE_USERNAME", "admin"),
        "password": _env("COUCHBASE_PASSWORD", "Password@12345"),
        "bucket": _env("COUCHBASE_BUCKET", "test_bucket"),
        "scope": _env("COUCHBASE_SCOPE", "test_scope"),
        "collection": _env("COUCHBASE_COLLECTION", "test_collection"),
    }


@pytest.fixture(scope="session")
def credentials(connection_settings: dict[str, str]) -> CouchbaseCredentials:
    """Build a password-authenticated credentials block for the live cluster."""
    return CouchbaseCredentials(
        connection_string=connection_settings["connection_string"],
        username=connection_settings["username"],
        password=connection_settings["password"],
    )


@pytest.fixture(scope="session")
def live_cluster(credentials: CouchbaseCredentials):
    """Yield a ready live cluster, or skip the suite if none is reachable."""
    try:
        cluster = credentials.get_cluster()
        cluster.wait_until_ready(READY_TIMEOUT)
    except Exception as exc:  # pragma: no cover - depends on external cluster
        pytest.skip(f"No live Couchbase cluster reachable: {exc}")
    yield cluster
    cluster.close()


@pytest.fixture
def connector(
    credentials: CouchbaseCredentials,
    connection_settings: dict[str, str],
    live_cluster,
) -> Iterator[CouchbaseConnector]:
    """Provide an open connector bound to the default scope/collection."""
    conn = credentials.get_connector(
        bucket=connection_settings["bucket"],
        wait_until_ready_timeout=READY_TIMEOUT,
    )
    with conn:
        yield conn


@pytest.fixture
def doc_key(connector: CouchbaseConnector) -> Iterator[str]:
    """Return a unique document key and remove it afterwards."""
    key = f"prefect-it::{uuid.uuid4()}"
    yield key
    with contextlib.suppress(Exception):  # cleanup is best effort
        connector.remove(key)
