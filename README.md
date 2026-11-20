# Cohort Insights API

A FastAPI service that stores documents in MongoDB and processes them with a
separate worker. Redis tracks active counts and caches completed results.
Processing and tags are simulated; no AI service is used.

## Project files in simple terms

| File | What it does |
| --- | --- |
| `app/main.py` | Starts FastAPI and defines the health checks. |
| `app/auth.py` | Checks the API key and identifies the user. |
| `app/database.py` | Creates and closes the MongoDB and Redis clients. |
| `app/routers/documents.py` | Defines the document API endpoints. |
| `app/schemas/documents.py` | Checks that incoming document data is valid. |
| `app/services/documents.py` | Saves, reads, updates and lists documents. |
| `app/worker.py` | Processes queued documents in the background. |
| `tests/` | Checks that the API and its important behaviors work. |
| `docker-compose.yml` | Describes the API, worker, MongoDB and Redis containers. |

For example, when a document is submitted, the router checks the caller and
the schema validates the data. The service saves it in MongoDB. The worker
finds the queued document and later saves the summary and tags. Redis tracks
active counts and stores a temporary result for identical content.

## Run locally (PowerShell)

Start Docker Desktop and run these commands from the project folder:

```powershell
docker compose up -d --wait mongo redis
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pytest -q
```

In one terminal, configure example development keys and start the API:

```powershell
$env:API_KEYS = '{"user-123":"local-demo-key","user-456":"another-demo-key"}'
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

In a second terminal, start the worker:

```powershell
.\.venv\Scripts\python.exe -m app.worker
```

The demo keys are not secure and must not be used in production. Real keys must
be long, random, unique per user, and supplied through a secret manager or
environment configuration. Never commit real keys.

Check `http://127.0.0.1:8000/health`,
`http://127.0.0.1:8000/health/mongodb`, and
`http://127.0.0.1:8000/health/redis`. Each should return `{"status":"ok"}`.
The general endpoint is only a liveness check; the other two ping the services.
Swagger UI is at `http://127.0.0.1:8000/docs`.

To submit from a third PowerShell terminal:

```powershell
$headers = @{ "X-API-Key" = "local-demo-key" }
$body = @{
    user_id = "user-123"
    title = "Learning Python"
    content = "Students practice Python every week."
    client_doc_ref = "cms-123"
} | ConvertTo-Json
$submitted = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8000/documents" -Headers $headers -ContentType "application/json" -Body $body
$submitted
Invoke-RestMethod -Uri "http://127.0.0.1:8000/documents/$($submitted.document_id)" -Headers $headers
```

Poll again after processing. It takes 10–20 seconds for the summary and 5–15
seconds for tags. Each stage has a 10% simulated failure probability. If a
stage fails, use `POST /documents/{document_id}/retry`. The retry resumes the
failed stage: an enrichment failure retains the existing summary.

To inspect stored documents:

```powershell
docker compose exec mongo mongosh
```

Inside `mongosh`:

```javascript
use cohort_insights
db.documents.find().pretty()
exit
```

Redis keys can be inspected with `docker compose exec redis redis-cli --scan`.
Keys beginning with `cohort:active:` track counts and `cohort:cache:` contain
cached results. Do not expose these development databases to the public.

## Docker Compose

**Verification status:** On the development computer, MongoDB and Redis were
run successfully in Docker, while FastAPI and the worker were run locally.
The full Compose setup has not been verified: the API image build failed
because pip could not verify the package-download certificate
(`CERTIFICATE_VERIFY_FAILED`). The cause of the certificate issue has not
been confirmed.

Copy `.env.example` to `.env` and replace the example API keys:

```powershell
Copy-Item .env.example .env
docker compose config --quiet
docker compose up --build -d --wait
docker compose ps
```

The Compose configuration is intended to start the API, worker, MongoDB and
Redis. The API container connects using `mongodb://mongo:27017` and
`redis://redis:6379/0`; the host defaults
are `mongodb://127.0.0.1:27017` and `redis://127.0.0.1:6379/0`. Optional
`MONGODB_URI`, `REDIS_URL`, and `MONGODB_DATABASE` environment variables
configure the local API and worker. `API_KEYS` is a JSON object mapping user
IDs to keys; missing, malformed, empty or duplicate-key configurations are
rejected. The API uses the `X-API-Key` header and checks the caller's identity
against the submitted user ID and every owner-scoped query.

The development MongoDB and Redis services are unauthenticated and bound to
localhost. MongoDB data persists in the `mongo_data` volume; Redis is
ephemeral. Use `docker compose down` to stop without deleting the volume.
If a network certificate is not trusted inside Docker, ask your administrator
about the approved CA configuration; do not disable TLS verification. The
local API and worker setup above is a development alternative, not a substitute
for verifying the final Compose build.

## API behavior

| Method and path | Behavior |
| --- | --- |
| `POST /documents` | Validate input; create a queued document or return an immediate cached result. |
| `GET /documents/{id}` | Return the owner's document, version, status and stage results. |
| `PATCH /documents/{id}` | Replace content, increment its version and queue both stages again. |
| `POST /documents/{id}/retry` | Resume the failed stage. |
| `GET /documents/by-ref/{ref}` | Find an owner's document by partner reference. |
| `GET /users/{user_id}/documents` | List the owner's documents, newest first; accepts `page`, `page_size` and `status`. |

Use the API key for the submitted `user_id`. Missing or invalid keys return
401; another user's document or list returns 404. Invalid input returns 422.
An active-job limit returns 429. MongoDB or Redis failure during admission
returns 503. If Redis becomes unavailable after a MongoDB write, that write
remains committed; the count/cache refresh error is logged and a later
admission reconciles the count. Clients must account for uncertain outcomes
when network or storage errors happen around a write.

`client_doc_ref` is globally unique when present. Any duplicate submission,
even with identical content, returns 409; a changed document must be updated
with PATCH. This avoids an out-of-order retry silently overwriting content.
MongoDB has a unique partial index `client_doc_ref_1` for string references;
the by-ref lookup explicitly uses it. Other indexes cover user listing, status,
content hashes, the worker queue and active slots.

## Schema & Staleness Design

Each MongoDB document has a `version`, `content_hash`, stage states and one
`result` object containing its own `version`, summary and tags. Stage 1 stores
the summary with `tags: null`; stage 2 replaces the whole result with a
summary and tags from that same version. A failed enrichment preserves that
partial result.

PATCH atomically increments the document version, clears the result and stage
states, and queues the new content. A worker claims a job with a lease ID and
records the version it read. Every worker write requires both that version and
lease ID to still match. If PATCH occurs while an old worker is running, its
conditional write matches nothing. The reader also only exposes a result whose
version equals the current document version. MongoDB single-document atomic
updates ensure readers see either the old document before PATCH or the new
document after it—not a mixture. Concurrent PATCH requests against the same
starting version yield one success and one 409.

Cache entries are keyed by user and SHA-256 content hash, never document ID.
A cache hit is copied into a new result labeled with the new document's
version. The cache expires after 24 hours; a previously completed matching
MongoDB document can refill it. The worker's lease expires after 120 seconds
so another worker can recover abandoned jobs. A superseded worker cannot
publish after a new lease is claimed.

## Rate limit and recovery

An active document owns one of three numbered slots. A unique partial MongoDB
index on `(user_id, active_slot)` makes it impossible for two active documents
to take the same slot, even under concurrent API requests. Redis stores the
active count per user with a 24-hour TTL. A Redis lock serializes count refreshes,
which recount MongoDB before admission and after status changes. MongoDB slots
are the final enforcement guard: a lost or stale Redis counter cannot allow a
fourth job. Redis is required for new submissions, updates and retries, so
these operations fail closed when it is unavailable. Reads remain available
if MongoDB is reachable. Cache and counter updates after a committed write
are best effort with error logging, and counts are reconciled on the next
admission. Worker restarts may leave a lease until it expires.

## Testing

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

The integration tests use local MongoDB and Redis, create a uniquely named
temporary database, and clean it up. They skip if those services are not
available. They cover ownership, duplicate references and indexed lookup,
concurrent limits, cache reuse, stage failure and retry, and an update racing
an old worker. Their simulated stage delays are patched out, so the suite
doesn't wait 15–35 seconds per document.

## At 100×

A user with 500,000 documents will have a large contiguous range in an index
beginning with `user_id`. The compound listing index avoids scanning other
users, but the user's range and associated records can exceed available
memory, causing more disk reads. The `(user_id, status, created_at)` index helps
filtered listings, but each additional index consumes disk and makes writes
more expensive. I would inspect real query plans and access patterns before
adding further indexes.

Sharding only by `user_id` is attractive for owner-scoped reads, but a very
large or very active user can become a hot or oversized partition. I would
consider a compound key containing a stable user hash plus a document-derived
bucket. This spreads one user's documents across buckets, while reads for a
single user must query and merge those buckets. Crosswalk uniqueness becomes
more difficult across shards, so I would use a separately partitioned reference
mapping or a service providing global uniqueness. The final choice depends on
the relative rate of writes, per-user lists and reference lookups.

At 100× submit QPS, recounting MongoDB and taking a Redis lock on each admission
can become a bottleneck, especially for one hot user. The three MongoDB slots
still enforce the hard limit, but the Redis count is a reconciled view rather
than a cross-database transaction. I would measure lock contention and move
admission and reservations to a purpose-built atomic Redis script, with
idempotent job IDs, leases and durable reconciliation for Redis restarts.
Keeping MongoDB as the authority still requires a carefully designed recovery
protocol for failures between the two systems; simply adding replicas does
not solve that consistency gap.

Skip/limit pagination becomes slower for deep pages because MongoDB must walk
past the skipped entries. I would use cursor pagination based on the last
`(created_at, _id)` pair and a matching compound index. The ID breaks ties
when timestamps match, and the cursor avoids scanning every earlier page.
This changes navigation from arbitrary numbered pages to efficient next-page
navigation and needs a documented cursor format.

## Further improvements

Use a real identity provider, TLS and authenticated database connections,
structured logs and metrics, configurable worker concurrency, automated
backoff, durable queue monitoring, and a production-grade recovery protocol.
Integration tests should run in CI against isolated services. A secret manager
and key rotation are needed before deployment.
