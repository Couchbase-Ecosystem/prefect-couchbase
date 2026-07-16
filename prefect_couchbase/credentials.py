"""Credential block for connecting Prefect flows to Couchbase."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any, ClassVar

from couchbase.auth import CertificateAuthenticator, PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, KnownConfigProfiles
from prefect.blocks.abstract import CredentialsBlock
from pydantic import Field, SecretStr, field_validator, model_validator


class CouchbaseCredentials(CredentialsBlock):
    """A Prefect credentials block for Couchbase clusters.

    Store the connection string and authentication material once, then load the
    block from flows and tasks to create a Couchbase Python SDK `Cluster` or a
    higher-level `CouchbaseConnector`.

    Examples:
        Save password credentials:
        ```python
        from prefect_couchbase import CouchbaseCredentials

        credentials = CouchbaseCredentials(
            connection_string="couchbases://cb.example.cloud.couchbase.com",
            username="analytics_user",
            password="secret-password",
        )
        credentials.save("prod-couchbase", overwrite=True)
        ```
    """

    _block_type_name: ClassVar[str] = "Couchbase Credentials"
    _logo_url: ClassVar[str] = (
        "https://www.couchbase.com/wp-content/uploads/2023/05/CB-logo-2023.svg"
    )
    _documentation_url: ClassVar[str] = (
        "https://github.com/couchbase-examples/couchbase-prefect"
    )

    connection_string: str = Field(
        default="couchbase://localhost",
        description=(
            "Couchbase connection string, for example `couchbase://localhost` "
            "or `couchbases://cb.example.cloud.couchbase.com`."
        ),
        examples=[
            "couchbase://localhost",
            "couchbases://cb.example.cloud.couchbase.com",
        ],
    )
    username: SecretStr | None = Field(
        default=None, description="Username for password authentication."
    )
    password: SecretStr | None = Field(
        default=None, description="Password for password authentication."
    )
    cert_path: Path | None = Field(
        default=None,
        description=(
            "Path to a trusted CA certificate for password authentication, or "
            "to a client certificate for certificate authentication."
        ),
    )
    key_path: Path | None = Field(
        default=None,
        description="Path to a client private key for certificate authentication.",
    )
    trust_store_path: Path | None = Field(
        default=None,
        description="Path to a trust store for certificate authentication.",
    )
    profile: str | None = Field(
        default=None,
        description=(
            "Optional Couchbase SDK configuration profile, such as `wan_development`."
        ),
    )
    options: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Additional keyword arguments passed to `couchbase.options.ClusterOptions`."
        ),
    )

    @field_validator("connection_string")
    @classmethod
    def _connection_string_must_not_be_blank(cls, value: str) -> str:
        """Require a non-empty Couchbase connection string."""
        if not value.strip():
            raise ValueError("connection_string must not be blank")
        return value

    @model_validator(mode="after")
    def _validate_authentication(self) -> CouchbaseCredentials:
        """Validate password or certificate authentication configuration."""
        username_set = self.username is not None
        password_set = self.password is not None
        cert_auth_fields = [self.cert_path, self.key_path, self.trust_store_path]
        cert_auth_values = [field is not None for field in cert_auth_fields]

        if username_set != password_set:
            raise ValueError("username and password must be provided together")
        if not username_set and not any(cert_auth_values):
            raise ValueError(
                "either username/password or certificate authentication paths must "
                "be provided"
            )
        if any(cert_auth_values) and not all(cert_auth_values) and not username_set:
            raise ValueError(
                "cert_path, key_path, and trust_store_path are all required for "
                "certificate authentication"
            )
        if username_set and (
            self.key_path is not None or self.trust_store_path is not None
        ):
            raise ValueError(
                "key_path and trust_store_path are only valid for certificate "
                "authentication"
            )
        return self

    @property
    def uses_certificate_authentication(self) -> bool:
        """Whether the block will authenticate with client certificates."""
        return self.username is None

    def get_authenticator(self) -> CertificateAuthenticator | PasswordAuthenticator:
        """Build the Couchbase SDK authenticator for this block."""
        if self.uses_certificate_authentication:
            return CertificateAuthenticator(
                cert_path=str(self.cert_path),
                key_path=str(self.key_path),
                trust_store_path=str(self.trust_store_path),
            )
        return PasswordAuthenticator(
            username=self.username.get_secret_value(),
            password=self.password.get_secret_value(),
            cert_path=str(self.cert_path) if self.cert_path is not None else None,
        )

    def get_cluster_options(self) -> ClusterOptions:
        """Build `ClusterOptions`, including an optional SDK profile."""
        options = ClusterOptions(authenticator=self.get_authenticator(), **self.options)
        if self.profile is not None:
            options.apply_profile(self._resolve_profile(self.profile))
        return options

    def get_cluster(self) -> Cluster:
        """Create a Couchbase SDK cluster.

        The SDK opens network resources lazily as services are used. Call
        `Cluster.close()` directly or use `get_connector()` as a context manager
        when the flow is done with the connection.
        """
        return Cluster(self.connection_string, self.get_cluster_options())

    def get_client(self) -> Cluster:
        """Return a Couchbase SDK cluster for Prefect's credentials block contract."""
        return self.get_cluster()

    def get_connector(
        self,
        *,
        bucket: str | None = None,
        scope: str = "_default",
        collection: str = "_default",
        wait_until_ready_timeout: timedelta | None = None,
    ) -> Any:
        """Create a higher-level connector bound to this credentials block."""
        from prefect_couchbase.connector import CouchbaseConnector

        return CouchbaseConnector(
            credentials=self,
            bucket=bucket,
            scope=scope,
            collection=collection,
            wait_until_ready_timeout=wait_until_ready_timeout,
        )

    @staticmethod
    def _resolve_profile(profile: str) -> KnownConfigProfiles | str:
        """Resolve friendly SDK profile strings while preserving SDK compatibility."""
        for known_profile in KnownConfigProfiles:
            if profile in {known_profile.name, known_profile.value}:
                return known_profile
        return profile
