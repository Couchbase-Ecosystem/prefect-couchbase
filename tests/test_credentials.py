from pathlib import Path
from unittest.mock import patch

import pytest
from couchbase.options import KnownConfigProfiles
from pydantic import ValidationError

from prefect_couchbase import CouchbaseConnector, CouchbaseCredentials


def test_password_authenticator_uses_username_password_and_optional_ca_cert():
    credentials = CouchbaseCredentials(
        connection_string="couchbases://example.com",
        username="user",
        password="secret",
        cert_path=Path("/tmp/ca.pem"),
    )

    with patch("prefect_couchbase.credentials.PasswordAuthenticator") as authenticator:
        assert credentials.get_authenticator() is authenticator.return_value

    authenticator.assert_called_once_with(
        username="user", password="secret", cert_path="/tmp/ca.pem"
    )
    assert credentials.uses_certificate_authentication is False


def test_password_authenticator_omits_ca_cert_when_not_configured():
    credentials = CouchbaseCredentials(username="user", password="secret")

    with patch("prefect_couchbase.credentials.PasswordAuthenticator") as authenticator:
        credentials.get_authenticator()

    authenticator.assert_called_once_with(
        username="user", password="secret", cert_path=None
    )


def test_certificate_authenticator_requires_all_certificate_paths():
    credentials = CouchbaseCredentials(
        cert_path=Path("/tmp/client.pem"),
        key_path=Path("/tmp/client.key"),
        trust_store_path=Path("/tmp/trust.pem"),
    )

    with patch(
        "prefect_couchbase.credentials.CertificateAuthenticator"
    ) as authenticator:
        assert credentials.get_authenticator() is authenticator.return_value

    authenticator.assert_called_once_with(
        cert_path="/tmp/client.pem",
        key_path="/tmp/client.key",
        trust_store_path="/tmp/trust.pem",
    )
    assert credentials.uses_certificate_authentication is True


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"connection_string": "   "}, "connection_string must not be blank"),
        ({}, "either username/password or certificate authentication paths"),
        ({"username": "user"}, "username and password must be provided together"),
        ({"password": "secret"}, "username and password must be provided together"),
        (
            {"cert_path": Path("/tmp/client.pem")},
            "cert_path, key_path, and trust_store_path are all required",
        ),
        (
            {
                "username": "user",
                "password": "secret",
                "key_path": Path("/tmp/client.key"),
            },
            "key_path and trust_store_path are only valid",
        ),
    ],
)
def test_invalid_authentication_configuration_is_rejected(kwargs, error):
    with pytest.raises(ValidationError, match=error):
        CouchbaseCredentials(**kwargs)


def test_cluster_options_pass_through_options_and_apply_known_profile():
    credentials = CouchbaseCredentials(
        username="user",
        password="secret",
        profile="wan_development",
        options={"show_queries": True},
    )

    with (
        patch("prefect_couchbase.credentials.ClusterOptions") as cluster_options,
        patch.object(credentials, "get_authenticator", return_value="authenticator"),
    ):
        assert credentials.get_cluster_options() is cluster_options.return_value

    cluster_options.assert_called_once_with(
        authenticator="authenticator", show_queries=True
    )
    cluster_options.return_value.apply_profile.assert_called_once_with(
        KnownConfigProfiles.WanDevelopment
    )


def test_cluster_options_does_not_apply_profile_when_profile_is_omitted():
    credentials = CouchbaseCredentials(username="user", password="secret")

    with patch("prefect_couchbase.credentials.ClusterOptions") as cluster_options:
        assert credentials.get_cluster_options() is cluster_options.return_value

    cluster_options.return_value.apply_profile.assert_not_called()


def test_known_profile_name_is_resolved_for_sdk_compatibility():
    assert (
        CouchbaseCredentials._resolve_profile("WanDevelopment")
        is KnownConfigProfiles.WanDevelopment
    )


def test_get_client_returns_cluster_for_prefect_credentials_contract():
    credentials = CouchbaseCredentials(username="user", password="secret")

    with patch.object(credentials, "get_cluster", return_value="cluster"):
        assert credentials.get_client() == "cluster"


def test_unknown_profile_is_passed_to_sdk_for_forward_compatibility():
    credentials = CouchbaseCredentials(
        username="user", password="secret", profile="future_profile"
    )

    with patch("prefect_couchbase.credentials.ClusterOptions") as cluster_options:
        credentials.get_cluster_options()

    cluster_options.return_value.apply_profile.assert_called_once_with("future_profile")


def test_get_cluster_uses_connection_string_and_cluster_options():
    credentials = CouchbaseCredentials(
        connection_string="couchbase://db", username="user", password="secret"
    )

    with (
        patch("prefect_couchbase.credentials.Cluster") as cluster,
        patch.object(credentials, "get_cluster_options", return_value="options"),
    ):
        assert credentials.get_cluster() is cluster.return_value

    cluster.assert_called_once_with("couchbase://db", "options")


def test_get_connector_builds_configured_connector():
    credentials = CouchbaseCredentials(username="user", password="secret")
    connector = credentials.get_connector(
        bucket="travel-sample",
        scope="inventory",
        collection="hotel",
        wait_until_ready_timeout=None,
    )

    assert isinstance(connector, CouchbaseConnector)
    assert connector.credentials is credentials
    assert connector.bucket == "travel-sample"
    assert connector.scope == "inventory"
    assert connector.collection == "hotel"


def test_block_secrets_are_masked_in_model_dump():
    credentials = CouchbaseCredentials(username="user", password="secret")

    assert credentials.password.get_secret_value() == "secret"
    assert credentials.model_dump()["password"] != "secret"
