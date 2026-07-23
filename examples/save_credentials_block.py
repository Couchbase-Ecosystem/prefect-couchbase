"""Create and persist a Couchbase credentials block.

Run this once per environment to store your Couchbase connection details as a
Prefect block. Flows then load the block by name with
``CouchbaseCredentials.load(...)`` instead of hardcoding secrets in flow code.

Credentials are read from environment variables so they never live in source
control. The defaults target a local Couchbase Server for quick evaluation:

    export COUCHBASE_CONNECTION_STRING="couchbases://cb.example.cloud.couchbase.com"
    export COUCHBASE_USERNAME="analytics_user"
    export COUCHBASE_PASSWORD="super-secret-password"

Then run:

    python examples/save_credentials_block.py

The block is saved to whatever Prefect backend the current profile points at
(Prefect Cloud, a self-hosted server, or the local SQLite database). This step
does not connect to Couchbase; the connection is opened lazily when a flow uses
the block.
"""

from __future__ import annotations

import os

from prefect_couchbase import CouchbaseCredentials

BLOCK_NAME = "couchbase-credentials"


def save_credentials_block() -> str:
    """Build the credentials block from the environment and persist it."""
    credentials = CouchbaseCredentials(
        connection_string=os.getenv(
            "COUCHBASE_CONNECTION_STRING", "couchbase://localhost"
        ),
        username=os.getenv("COUCHBASE_USERNAME", "admin"),
        password=os.getenv("COUCHBASE_PASSWORD", "Password@12345"),
    )
    credentials.save(BLOCK_NAME, overwrite=True)
    return BLOCK_NAME


if __name__ == "__main__":
    name = save_credentials_block()
    print(f"Saved Couchbase credentials block as {name!r}.")
