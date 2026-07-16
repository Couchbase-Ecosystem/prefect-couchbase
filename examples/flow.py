"""Minimal Prefect flow using the Couchbase connector.

Set these environment variables before running against a live Couchbase cluster:

- COUCHBASE_CONNECTION_STRING (default: couchbase://localhost)
- COUCHBASE_USERNAME (default: Administrator)
- COUCHBASE_PASSWORD (default: password)
- COUCHBASE_BUCKET (default: travel-sample)
"""

from __future__ import annotations

import os

from prefect import flow, task

from prefect_couchbase import CouchbaseCredentials


@task
def write_hotel_document(document_id: str, name: str) -> dict[str, str]:
    credentials = CouchbaseCredentials(
        connection_string=os.getenv(
            "COUCHBASE_CONNECTION_STRING", "couchbase://localhost"
        ),
        username=os.getenv("COUCHBASE_USERNAME", "Administrator"),
        password=os.getenv("COUCHBASE_PASSWORD", "password"),
    )
    bucket = os.getenv("COUCHBASE_BUCKET", "travel-sample")

    with credentials.get_connector(bucket=bucket) as couchbase:
        couchbase.upsert(document_id, {"type": "hotel", "name": name})
        return couchbase.get(document_id)


@flow(log_prints=True)
def couchbase_demo_flow() -> dict[str, str]:
    document = write_hotel_document("prefect::hotel::1", "Prefect Grand Hotel")
    print(f"Stored document: {document}")
    return document


if __name__ == "__main__":
    couchbase_demo_flow()
