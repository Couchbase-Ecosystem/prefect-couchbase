"""Minimal Prefect flow that uses a saved Couchbase credentials block.

This example shows the recommended pattern: store credentials once as a Prefect
block, then load the block from tasks instead of hardcoding secrets.

Prerequisites:

1. Save the credentials block once (see ``save_credentials_block.py``):

       python examples/save_credentials_block.py

2. Choose the bucket the flow should write to (defaults to ``test_bucket``):

       export COUCHBASE_BUCKET="travel-sample"

3. Run the flow:

       python examples/couchbase_flow.py

Expected output:

    Stored document: {'type': 'hotel', 'name': 'Prefect Grand Hotel'}
"""

from __future__ import annotations

import os

from prefect import flow, task

from prefect_couchbase import CouchbaseCredentials

BLOCK_NAME = "couchbase-credentials"


@task
def upsert_hotel(document_id: str, name: str) -> dict[str, str]:
    """Store a hotel document and return the persisted contents."""
    credentials = CouchbaseCredentials.load(BLOCK_NAME)
    bucket = os.getenv("COUCHBASE_BUCKET", "test_bucket")

    with credentials.get_connector(bucket=bucket) as couchbase:
        couchbase.upsert(document_id, {"type": "hotel", "name": name})
        return couchbase.get(document_id)


@flow(log_prints=True)
def couchbase_demo_flow() -> dict[str, str]:
    """Write a single hotel document and print the stored result."""
    document = upsert_hotel("prefect::hotel::1", "Prefect Grand Hotel")
    print(f"Stored document: {document}")
    return document


if __name__ == "__main__":
    couchbase_demo_flow()
