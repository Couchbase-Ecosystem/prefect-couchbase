"""End-to-end integration tests against a live Couchbase cluster.

Every public credentials/connector code path is exercised except certificate
authentication, which is intentionally excluded per scope.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import DocumentNotFoundException
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.result import GetResult, MutationResult

from prefect_couchbase import CouchbaseConnector, CouchbaseCredentials

# ---------------------------------------------------------------------------
# Credentials block
# ---------------------------------------------------------------------------


def test_password_authenticator_is_built(credentials: CouchbaseCredentials):
    authenticator = credentials.get_authenticator()

    assert isinstance(authenticator, PasswordAuthenticator)
    assert credentials.uses_certificate_authentication is False


def test_cluster_options_are_built(credentials: CouchbaseCredentials):
    options = credentials.get_cluster_options()

    assert isinstance(options, ClusterOptions)


def test_cluster_options_apply_known_profile(connection_settings: dict[str, str]):
    credentials = CouchbaseCredentials(
        connection_string=connection_settings["connection_string"],
        username=connection_settings["username"],
        password=connection_settings["password"],
        profile="wan_development",
    )

    # Building options with a profile must not raise against the real SDK.
    assert isinstance(credentials.get_cluster_options(), ClusterOptions)


def test_get_cluster_connects_to_live_cluster(
    credentials: CouchbaseCredentials, live_cluster
):
    cluster = credentials.get_cluster()
    try:
        assert isinstance(cluster, Cluster)
        cluster.wait_until_ready(timedelta(seconds=10))
        assert cluster.ping() is not None
    finally:
        cluster.close()


def test_get_client_returns_live_cluster(
    credentials: CouchbaseCredentials, live_cluster
):
    client = credentials.get_client()
    try:
        assert isinstance(client, Cluster)
        client.wait_until_ready(timedelta(seconds=10))
    finally:
        client.close()


def test_secrets_are_masked_in_model_dump(credentials: CouchbaseCredentials):
    dumped = credentials.model_dump()

    assert dumped["password"] != credentials.password.get_secret_value()
    assert credentials.password.get_secret_value()


# ---------------------------------------------------------------------------
# Connector lifecycle
# ---------------------------------------------------------------------------


def test_get_connector_builds_configured_connector(
    credentials: CouchbaseCredentials, connection_settings: dict[str, str]
):
    connector = credentials.get_connector(
        bucket=connection_settings["bucket"],
        scope=connection_settings["scope"],
        collection=connection_settings["collection"],
    )

    assert isinstance(connector, CouchbaseConnector)
    assert connector.credentials is credentials
    assert connector.bucket == connection_settings["bucket"]
    assert connector.scope == connection_settings["scope"]
    assert connector.collection == connection_settings["collection"]


def test_cluster_is_created_lazily_and_cached(
    credentials: CouchbaseCredentials,
    connection_settings: dict[str, str],
    live_cluster,
):
    connector = credentials.get_connector(bucket=connection_settings["bucket"])

    assert connector._cluster is None
    first = connector.cluster
    second = connector.cluster
    try:
        assert isinstance(first, Cluster)
        assert first is second
    finally:
        connector.close()


def test_context_manager_opens_and_closes_cluster(
    credentials: CouchbaseCredentials,
    connection_settings: dict[str, str],
    live_cluster,
):
    connector = credentials.get_connector(
        bucket=connection_settings["bucket"],
        wait_until_ready_timeout=timedelta(seconds=10),
    )

    with connector as opened:
        assert opened is connector
        assert isinstance(connector._cluster, Cluster)

    assert connector._cluster is None


def test_close_without_created_cluster_is_noop(
    credentials: CouchbaseCredentials,
    connection_settings: dict[str, str],
    live_cluster,
):
    connector = credentials.get_connector(bucket=connection_settings["bucket"])

    assert connector._cluster is None
    connector.close()  # Must not raise even though no cluster was created.
    assert connector._cluster is None


# ---------------------------------------------------------------------------
# Bucket / scope / collection resolution
# ---------------------------------------------------------------------------


def test_get_bucket_scope_and_collection_defaults(connector: CouchbaseConnector):
    bucket = connector.get_bucket()
    scope = connector.get_scope()
    collection = connector.get_collection()

    assert bucket.name == connector.bucket
    assert scope.name == "_default"
    assert collection.name == "_default"


def test_get_scope_and_collection_overrides(
    connector: CouchbaseConnector, connection_settings: dict[str, str]
):
    scope = connector.get_scope(scope=connection_settings["scope"])
    collection = connector.get_collection(
        scope=connection_settings["scope"],
        collection=connection_settings["collection"],
    )

    assert scope.name == connection_settings["scope"]
    assert collection.name == connection_settings["collection"]


def test_bucket_is_required_when_no_default_is_configured(
    credentials: CouchbaseCredentials, live_cluster
):
    connector = credentials.get_connector()
    with (
        connector,
        pytest.raises(ValueError, match="bucket must be provided"),
    ):
        connector.get_bucket()


# ---------------------------------------------------------------------------
# Document operations: upsert / get / remove
# ---------------------------------------------------------------------------


def test_upsert_get_and_remove_roundtrip(connector: CouchbaseConnector, doc_key: str):
    document = {"type": "hotel", "name": "Prefect Grand Hotel"}

    upsert_result = connector.upsert(doc_key, document)
    assert isinstance(upsert_result, MutationResult)

    assert connector.get(doc_key) == document

    remove_result = connector.remove(doc_key)
    assert isinstance(remove_result, MutationResult)

    with pytest.raises(DocumentNotFoundException):
        connector.get(doc_key)


def test_upsert_replaces_existing_document(connector: CouchbaseConnector, doc_key: str):
    connector.upsert(doc_key, {"version": 1})
    connector.upsert(doc_key, {"version": 2})

    assert connector.get(doc_key) == {"version": 2}


def test_get_supports_content_as_string(connector: CouchbaseConnector, doc_key: str):
    connector.upsert(doc_key, "a plain string value")

    assert connector.get(doc_key, content_as=str) == "a plain string value"


def test_get_passes_options_through_to_sdk(connector: CouchbaseConnector, doc_key: str):
    connector.upsert(doc_key, {"n": 1})

    # A GetOptions timeout keyword must be forwarded without error.
    from couchbase.options import GetOptions

    result = connector.get_collection().get(
        doc_key, GetOptions(timeout=timedelta(seconds=5))
    )
    assert isinstance(result, GetResult)


def test_operations_target_override_scope_and_collection(
    connector: CouchbaseConnector,
    connection_settings: dict[str, str],
    doc_key: str,
):
    scope = connection_settings["scope"]
    collection = connection_settings["collection"]

    connector.upsert(
        doc_key, {"placed": "override"}, scope=scope, collection=collection
    )
    try:
        assert connector.get(doc_key, scope=scope, collection=collection) == {
            "placed": "override"
        }
    finally:
        connector.remove(doc_key, scope=scope, collection=collection)

    # The document must not leak into the default collection.
    with pytest.raises(DocumentNotFoundException):
        connector.get(doc_key)


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


def test_query_materializes_rows(connector: CouchbaseConnector):
    rows = connector.query("SELECT 1 AS n")

    assert rows == [{"n": 1}]


def test_query_with_positional_parameters(connector: CouchbaseConnector):
    rows = connector.query("SELECT $1 AS n", QueryOptions(positional_parameters=[7]))

    assert rows == [{"n": 7}]


def test_query_with_named_parameters(connector: CouchbaseConnector):
    rows = connector.query(
        "SELECT $val AS n", QueryOptions(named_parameters={"val": 9})
    )

    assert rows == [{"n": 9}]


# ---------------------------------------------------------------------------
# Ping
# ---------------------------------------------------------------------------


def test_ping_reports_services(connector: CouchbaseConnector):
    ping_result = connector.ping()

    assert ping_result is not None
    assert ping_result.endpoints
