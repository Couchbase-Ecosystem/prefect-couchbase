"""High-level Couchbase connector for Prefect tasks and flows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Generic, TypeVar

from couchbase.cluster import Cluster

from prefect_couchbase.credentials import CouchbaseCredentials

ContentT = TypeVar("ContentT")


@dataclass
class CouchbaseConnector(Generic[ContentT]):
    """Convenience wrapper around the Couchbase Python SDK.

    `CouchbaseConnector` is intentionally thin: it manages the cluster lifecycle
    and resolves the default bucket, scope, and collection that most workflows
    use, while returning native Couchbase SDK results for writes and deletes.
    """

    credentials: CouchbaseCredentials
    bucket: str | None = None
    scope: str = "_default"
    collection: str = "_default"
    wait_until_ready_timeout: timedelta | None = None
    _cluster: Cluster | None = field(default=None, init=False, repr=False)

    def __enter__(self) -> CouchbaseConnector[ContentT]:
        """Open the connector for use as a context manager."""
        _cluster = self.cluster
        return self

    def __exit__(self, *_exc_info: object) -> None:
        """Close the cluster when leaving a context manager."""
        self.close()

    @property
    def cluster(self) -> Cluster:
        """Return a cached Couchbase SDK cluster, creating it on first use."""
        if self._cluster is None:
            self._cluster = self.credentials.get_cluster()
            if self.wait_until_ready_timeout is not None:
                self._cluster.wait_until_ready(self.wait_until_ready_timeout)
        return self._cluster

    def close(self) -> None:
        """Close the underlying cluster if it has been created."""
        if self._cluster is not None:
            self._cluster.close()
            self._cluster = None

    def get_bucket(self, bucket: str | None = None) -> Any:
        """Return a Couchbase SDK bucket object."""
        return self.cluster.bucket(self._resolve_bucket(bucket))

    def get_scope(self, bucket: str | None = None, scope: str | None = None) -> Any:
        """Return a Couchbase SDK scope object."""
        return self.get_bucket(bucket).scope(scope or self.scope)

    def get_collection(
        self,
        bucket: str | None = None,
        scope: str | None = None,
        collection: str | None = None,
    ) -> Any:
        """Return a Couchbase SDK collection object."""
        return self.get_scope(bucket, scope).collection(collection or self.collection)

    def upsert(
        self,
        key: str,
        value: Any,
        *,
        bucket: str | None = None,
        scope: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Insert or replace a document in Couchbase."""
        return self.get_collection(bucket, scope, collection).upsert(
            key, value, **kwargs
        )

    def get(
        self,
        key: str,
        *,
        content_as: type[ContentT] = dict,
        bucket: str | None = None,
        scope: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> ContentT:
        """Fetch a document and deserialize the content with `content_as`."""
        result = self.get_collection(bucket, scope, collection).get(key, **kwargs)
        return result.content_as[content_as]

    def remove(
        self,
        key: str,
        *,
        bucket: str | None = None,
        scope: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Remove a document from Couchbase."""
        return self.get_collection(bucket, scope, collection).remove(key, **kwargs)

    def query(self, statement: str, *args: Any, **kwargs: Any) -> list[Any]:
        """Run a SQL++ query and materialize all result rows."""
        return list(self.cluster.query(statement, *args, **kwargs))

    def ping(self, **kwargs: Any) -> Any:
        """Ping Couchbase services through the underlying cluster."""
        return self.cluster.ping(**kwargs)

    def _resolve_bucket(self, bucket: str | None) -> str:
        """Resolve a bucket override or the connector default."""
        resolved_bucket = bucket or self.bucket
        if not resolved_bucket:
            raise ValueError("bucket must be provided")
        return resolved_bucket
