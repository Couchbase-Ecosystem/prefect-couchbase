# prefect-couchbase

A Prefect collection for connecting Prefect flows and tasks to Couchbase through the official Couchbase Python SDK.

## What you can use it for

- Store Couchbase connection details as a Prefect block and load them safely from flows.
- Read, write, and delete documents from Couchbase buckets/scopes/collections in Prefect tasks.
- Run SQL++ queries from Prefect flows while reusing the same Couchbase SDK connection lifecycle.

This connector does **not** replace the Couchbase Python SDK. It provides Prefect-native credentials and a small convenience wrapper around the SDK so workflows can manage credentials, lifecycle, and common operations consistently.

## End-user quickstart

Install the package:

```bash
pip install prefect-couchbase
```

Create and save a credentials block:

```python
from prefect_couchbase import CouchbaseCredentials

credentials = CouchbaseCredentials(
    connection_string="couchbases://cb.example.cloud.couchbase.com",
    username="analytics_user",
    password="super-secret-password",
)
credentials.save("prod-couchbase", overwrite=True)
```

Use the block from a flow:

```python
from prefect import flow, task
from prefect_couchbase import CouchbaseCredentials


@task
def upsert_hotel(document_id: str, name: str) -> dict:
    credentials = CouchbaseCredentials.load("prod-couchbase")

    with credentials.get_connector(
        bucket="travel-sample", scope="_default", collection="_default"
    ) as couchbase:
        couchbase.upsert(document_id, {"type": "hotel", "name": name})
        return couchbase.get(document_id)


@flow
def demo() -> dict:
    return upsert_hotel("prefect::hotel::1", "Prefect Grand Hotel")


if __name__ == "__main__":
    print(demo())
```

Expected output:

```text
{'type': 'hotel', 'name': 'Prefect Grand Hotel'}
```

## Provision Couchbase

### Recommended: Couchbase Capella

Use Capella when Prefect Cloud workers or any hosted execution infrastructure need to reach Couchbase over the public internet.

1. Create a Capella cluster.
2. Create or choose a bucket, scope, and collection.
3. Add a database user with access to the bucket.
4. Add the worker's outbound IP address to Capella's allowed IP list.
5. Use the Capella SDK connection string, which starts with `couchbases://`.
6. Configure the Prefect block with the connection string, username, and password.

### Local or self-managed Couchbase

Use local/self-managed Couchbase when your Prefect worker runs on the same machine or private network.

1. Start Couchbase Server locally.
2. Create a bucket such as `travel-sample` or `prefect-demo`.
3. Create a user with bucket read/write permissions.
4. Use a connection string such as `couchbase://localhost`.

For a quick local evaluation, the Couchbase Python SDK examples and Docker documentation are the best reference for the currently supported server image and initialization flow.

## Configuration reference

`CouchbaseCredentials` fields:

| Field | Description |
| --- | --- |
| `connection_string` | Couchbase SDK connection string, such as `couchbase://localhost` or `couchbases://cb.example.cloud.couchbase.com`. |
| `username` / `password` | Password authentication credentials. Store these as Prefect secrets through the block. |
| `cert_path` | Optional trusted CA certificate path for password authentication; client certificate path for certificate authentication. |
| `key_path` / `trust_store_path` | Required with `cert_path` when using certificate authentication. |
| `profile` | Optional SDK config profile, for example `wan_development`. |
| `options` | Extra keyword arguments passed to `couchbase.options.ClusterOptions`. |

`CouchbaseConnector` methods:

- `get_bucket(bucket=None)`
- `get_scope(bucket=None, scope=None)`
- `get_collection(bucket=None, scope=None, collection=None)`
- `upsert(key, value, **kwargs)`
- `get(key, content_as=dict, **kwargs)`
- `remove(key, **kwargs)`
- `query(statement, *args, **kwargs)`
- `ping(**kwargs)`
- `close()`

## Tutorial: runnable local flow

1. Install dependencies:

   ```bash
   pip install prefect-couchbase
   ```

2. Export connection settings:

   ```bash
   export COUCHBASE_CONNECTION_STRING="couchbase://localhost"
   export COUCHBASE_USERNAME="Administrator"
   export COUCHBASE_PASSWORD="password"
   export COUCHBASE_BUCKET="travel-sample"
   ```

3. Run the example flow from this repository:

   ```bash
   python examples/flow.py
   ```

4. Validate by checking that the flow prints:

   ```text
   Stored document: {'type': 'hotel', 'name': 'Prefect Grand Hotel'}
   ```

## Troubleshooting

- `Authentication failed`: verify username/password and bucket permissions.
- `Unambiguous timeout`: verify the worker can reach the cluster and that Capella allowed IPs include the worker.
- `bucket must be provided`: pass `bucket="..."` to `get_connector()` or to the individual operation.
- TLS errors with Capella: use a `couchbases://` connection string and ensure the cluster endpoint is correct.

## Developer documentation

### Local development

```bash
git clone https://github.com/couchbase-examples/couchbase-prefect.git
cd couchbase-prefect
uv sync --extra dev
```

Run tests and coverage:

```bash
uv run pytest
```

Run linting:

```bash
uv run ruff check .
uv run ruff format --check .
```

Build the package:

```bash
uv run python -m build
```

Run the example smoke check without a live Couchbase cluster:

```bash
uv run python -m py_compile examples/flow.py
```

Run the example against a live cluster by setting the environment variables from the tutorial and executing `uv run python examples/flow.py`.

### Release and publishing path

1. Ensure `uv run pytest`, `uv run ruff check .`, and `uv run python -m build` pass.
2. Update the version tag according to the repository release policy.
3. Build distributions with `uv run python -m build`.
4. Publish to PyPI or the chosen private package index with `twine upload dist/*` after configuring maintainer credentials.
5. Register the collection with Prefect by installing the package in the worker environment. The package exposes the `prefect.collections` entry point `prefect_couchbase = prefect_couchbase`.

## Research and design notes

This implementation follows Prefect's collection pattern from official integrations such as `prefect-redis`, `prefect-snowflake`, and `prefect-sqlalchemy`: typed Prefect blocks hold credentials and user code imports a small package-specific public API. It also mirrors the Couchbase authentication and collection access shape used by the Couchbase Airflow provider: password authentication uses `PasswordAuthenticator`, certificate authentication uses `CertificateAuthenticator`, and helper methods resolve bucket/scope/collection objects.

## Known limitations

- The unit test suite uses SDK mocks so it can run without a live Couchbase service. Live connectivity should be validated in the environment where the Prefect worker runs.
- The connector intentionally exposes a minimal wrapper. Advanced SDK features should be accessed from `connector.cluster` or native SDK objects returned by `get_bucket`, `get_scope`, and `get_collection`.
