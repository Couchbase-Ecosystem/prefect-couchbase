from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from prefect_couchbase import CouchbaseConnector, CouchbaseCredentials


@pytest.fixture
def credentials():
    return MagicMock(spec=CouchbaseCredentials)


def test_cluster_is_created_lazily_and_cached(credentials):
    cluster = MagicMock()
    credentials.get_cluster.return_value = cluster
    timeout = timedelta(seconds=10)
    connector = CouchbaseConnector(
        credentials=credentials, wait_until_ready_timeout=timeout
    )

    assert connector._cluster is None
    assert connector.cluster is cluster
    assert connector.cluster is cluster

    credentials.get_cluster.assert_called_once_with()
    cluster.wait_until_ready.assert_called_once_with(timeout)


def test_context_manager_opens_and_closes_cluster(credentials):
    cluster = MagicMock()
    credentials.get_cluster.return_value = cluster
    connector = CouchbaseConnector(credentials=credentials)

    with connector as opened:
        assert opened is connector
        assert connector._cluster is cluster

    cluster.close.assert_called_once_with()
    assert connector._cluster is None


def test_close_without_created_cluster_is_noop(credentials):
    connector = CouchbaseConnector(credentials=credentials)

    connector.close()

    credentials.get_cluster.assert_not_called()


def test_get_bucket_scope_and_collection_use_defaults(credentials):
    cluster = MagicMock()
    bucket = cluster.bucket.return_value
    scope = bucket.scope.return_value
    collection = scope.collection.return_value
    credentials.get_cluster.return_value = cluster
    connector = CouchbaseConnector(
        credentials=credentials,
        bucket="travel-sample",
        scope="inventory",
        collection="hotel",
    )

    assert connector.get_bucket() is bucket
    assert connector.get_scope() is scope
    assert connector.get_collection() is collection

    cluster.bucket.assert_called_with("travel-sample")
    bucket.scope.assert_called_with("inventory")
    scope.collection.assert_called_with("hotel")


def test_get_collection_accepts_bucket_scope_collection_overrides(credentials):
    cluster = MagicMock()
    bucket = cluster.bucket.return_value
    scope = bucket.scope.return_value
    collection = scope.collection.return_value
    credentials.get_cluster.return_value = cluster
    connector = CouchbaseConnector(credentials=credentials, bucket="default-bucket")

    assert (
        connector.get_collection(
            bucket="override-bucket",
            scope="override-scope",
            collection="override-collection",
        )
        is collection
    )

    cluster.bucket.assert_called_once_with("override-bucket")
    bucket.scope.assert_called_once_with("override-scope")
    scope.collection.assert_called_once_with("override-collection")


def test_bucket_is_required_when_no_default_is_configured(credentials):
    connector = CouchbaseConnector(credentials=credentials)

    with pytest.raises(ValueError, match="bucket must be provided"):
        connector.get_bucket()


def test_upsert_get_and_remove_delegate_to_collection(credentials):
    cluster = MagicMock()
    collection = cluster.bucket.return_value.scope.return_value.collection.return_value
    collection.get.return_value.content_as = {dict: {"name": "Hotel"}, str: "raw"}
    credentials.get_cluster.return_value = cluster
    connector = CouchbaseConnector(credentials=credentials, bucket="travel-sample")

    assert (
        connector.upsert("hotel::1", {"name": "Hotel"}, durability="majority")
        is collection.upsert.return_value
    )
    assert connector.get("hotel::1") == {"name": "Hotel"}
    assert connector.get("hotel::1", content_as=str, timeout=1) == "raw"
    assert connector.remove("hotel::1", cas="123") is collection.remove.return_value

    collection.upsert.assert_called_once_with(
        "hotel::1", {"name": "Hotel"}, durability="majority"
    )
    collection.get.assert_any_call("hotel::1")
    collection.get.assert_any_call("hotel::1", timeout=1)
    collection.remove.assert_called_once_with("hotel::1", cas="123")


def test_query_materializes_rows(credentials):
    cluster = MagicMock()
    cluster.query.return_value = iter([{"one": 1}, {"two": 2}])
    credentials.get_cluster.return_value = cluster
    connector = CouchbaseConnector(credentials=credentials)

    assert connector.query("SELECT 1", QueryOptions="ignored") == [
        {"one": 1},
        {"two": 2},
    ]
    cluster.query.assert_called_once_with("SELECT 1", QueryOptions="ignored")


def test_ping_delegates_to_cluster(credentials):
    cluster = MagicMock()
    credentials.get_cluster.return_value = cluster
    connector = CouchbaseConnector(credentials=credentials)

    assert connector.ping(service_types=["kv"]) is cluster.ping.return_value
    cluster.ping.assert_called_once_with(service_types=["kv"])
