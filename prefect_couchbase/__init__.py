"""Prefect collection for Couchbase."""

try:
    from . import _version

    __version__ = _version.__version__
except Exception:  # pragma: no cover - only used from an unpackaged source tree
    __version__ = "0.0.0"

from prefect_couchbase.connector import CouchbaseConnector
from prefect_couchbase.credentials import CouchbaseCredentials

__all__ = ["CouchbaseConnector", "CouchbaseCredentials", "__version__"]
