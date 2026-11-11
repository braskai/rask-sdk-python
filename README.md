# Rask Python SDK

The official Python client for the [Rask API v3](https://docs.api.rask.ai): upload media, localize it, edit transcripts,
dub, lip-sync and export. Sync and async, typed, with retries and idempotency built in. Version `3.0.0a0` (alpha, like
the API).

## Install

Python 3.11 or newer.

```sh
pip install rask-sdk
```

Before the first release is on PyPI, install from a checkout of this repository instead, with `pip install .`.

## Key and base URL

`Rask()` reads `RASK_API_KEY` and `RASK_BASE_URL`, and an argument wins over its variable.

```python
from rask_sdk import AsyncRask, Rask

client = Rask(api_key="raskai_live_key_...")  # https://api.rask.ai/v3 by default
client = Rask(api_key="...", base_url="https://gw.example.com/rask/v3")  # a gateway: the path prefix is kept
client = AsyncRask()  # the same API with await
```

The base URL applies to every operation and helper, and a trailing slash makes no difference. The key goes only to URLs
under it: presigned PUTs go without `Authorization`, a GET redirected elsewhere follows without the key, and a write
redirected elsewhere is not followed (it raises `RaskError` with the 3xx status).

## Worked example

Upload a file, localize it, wait for the dub, download the result.

<!-- worked example -->
```python
import httpx

from rask_sdk import Rask

client = Rask()

# Upload, then wait for the import. job.result holds the media id.
job = client.uploads.upload_file("talk.mp4", on_progress=lambda sent, total: print(f"{sent}/{total} bytes"))
media_id = client.jobs.wait(job.id).result.media_id

# Ask the price, then start. The cap is what you agree to pay; auto-dub follows the translation.
estimate = client.projects.estimate_project(media_id=media_id, target_language="es-es")
job = client.projects.create_project(
    media_id=media_id, target_language="es-es", expected_cost_minutes=estimate.total_minutes
)
job = client.jobs.wait(job.id)
project_id = job.result.project_id
if job.result.auto_dub.dub_job_id:
    client.jobs.wait(job.result.auto_dub.dub_job_id)

# Download. Output URLs are presigned and take no API key.
for output in client.exports.list_project_outputs(project_id).data:
    if output.kind == "dubbed_video" and output.available:
        with httpx.stream("GET", output.url) as response, open("talk.es.mp4", "wb") as file:
            for chunk in response.iter_bytes():
                file.write(chunk)
```

## Using it

Operations are grouped by the contract's tags and named after their `operationId`: `createFolder` is
`client.projects.create_folder(name=...)`, `listVoices` is `client.voices.list_voices(target_language="ru-ru")`.

- **Lists:** `for project in client.projects.list_projects(limit=50)` walks every page; `page.data`, `page.next_cursor` and
  `page.next_page()` give page-level access. With `AsyncRask`: `async for project in await client.projects.list_projects()`.
- **Status and headers:** `result.http.status`, `.headers`, `.etag`, `.location`, `.request_id`, `.idempotency_key` and
  `.idempotent_replayed`, with no second call. A list has them on the page, and a 204 returns a `NoContent` that has them too.
- **Edits:** `client.segments.update_segment(project_id, segment_id, if_match=segment.etag, translation_text="Hola")`.
  A stale ETag raises `PreconditionFailedError`, whose `current_etag` holds the current one. `""` clears a clearable field;
  `None` means "leave it out", and requests never send null.
- **Errors:** `RaskError` and one subclass per status (`InsufficientBalanceError`, `RateLimitError`, ...) carry every Problem
  member as an attribute, plus `headers`, `request_id` and `idempotency_key`. Unknown codes are kept.
- **Retries:** 429, 502, 503, 504, connection errors and timeouts, for up to 120 s, honouring `Retry-After`. Writes keep their
  `Idempotency-Key` across retries; pass `idempotency_key=` to choose it. `max_retries=0` turns retries off.
- **Forward compatibility:** an enum value, a union variant (`rask_sdk.UnknownVariant`) or a field this version does not
  know stays readable on the result and never fails the call. `extra_body=`, `extra_query=` and `extra_headers=` send what
  it does not know yet.
- **Debugging:** `RASK_LOG=debug` logs traffic to stderr (or to your `rask_sdk` logger), without the key or presigned
  signatures.

## The API v2 client

The previous client talks to API v2 and stays available beside this one while you migrate, unchanged but for its import:

```python
# pip install "rask-sdk[v2]"
from rask_sdk.v2 import RaskSDKClient, enums, schemas
```

`from rask_sdk import clients, enums, schemas` no longer works. To stay on the old layout, pin the last commit that had it,
`bc87b59`, with `pip install "git+ssh://git@github.com/braskai/rask-sdk.git@bc87b59"`.

## Development

[uv](https://docs.astral.sh/uv/) and, for the acceptance tests, Node 22 or newer. `make help` lists everything.

```sh
make bootstrap   # uv sync
make generate    # rewrite rask_sdk/_generated from contract/openapi.json
make lint        # ruff, mypy, and a check that the generated code matches the contract
make test        # unit and acceptance tests; RUN_SLOW=1 adds the two slow scenarios
make build       # sdist and wheel in dist/
```

The acceptance tests start [rask-sdk-acceptance](https://github.com/braskai/rask-sdk-acceptance), found next to this checkout or through
`RASK_SDK_ACCEPTANCE_DIR`. `rask_sdk/_generated` is generated by `tools/generate.py`: never edit it by hand. `examples/smoke.py` checks a
real environment with `RASK_API_KEY` and `RASK_BASE_URL` set.

## License

MIT. See [LICENSE](LICENSE).
