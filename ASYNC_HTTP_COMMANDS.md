# Asynchronous HTTP Commands

The monitoring API has an opt-in asynchronous command admission path inspired
by ClickHouse asynchronous inserts. It lets a client submit a journaled write,
receive a bounded admission response, and poll a completion record instead of
waiting for the command's durable group commit in the request.

This is an HTTP admission feature, not a replication protocol. It is disabled
by default and leaves ordinary synchronous `POST /api/commands` behavior
unchanged.

## Enable It

Async HTTP commands require all of the following:

- a command journal;
- journal idempotency enabled with a capacity greater than zero; and
- journal group commit with a maximum batch greater than one.

The normal monitoring authentication controls still apply. For a local test
or service deployment:

```sh
MONITORING_ASYNC_COMMANDS=true \
MONITORING_ASYNC_COMMAND_STATUS_CAPACITY=1024 \
JOURNAL_PATH=data/commands.journal \
JOURNAL_IDEMPOTENCY_CAPACITY=4096 \
JOURNAL_GROUP_COMMIT_MAX_BATCH=64 \
MONITORING_AUTH_TOKEN=operator-secret \
make monitoring-server
```

The status registry defaults to 1,024 entries and is capped at 65,536. It
evicts completed entries before rejecting new work. Successful idempotency
responses can also be recovered from the journal after a handler restart.

Async HTTP admission is rejected when leader-write enforcement or a
replicator is configured. The direct journal callback cannot atomically carry
the leader and replication outbox side effects, so rejecting this combination
preserves the existing replication correctness contract.

## Submit And Poll

Send a journalable public write with a unique, retry-stable idempotency key and
either `X-Hatrie-Async: true` or the standard `Prefer: respond-async` header:

```sh
curl --fail-with-body \
  -H 'Authorization: Bearer operator-secret' \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -H 'X-Hatrie-Async: true' \
  --data '{"command":"SET","key":"session:42","value":"ready","idempotency_key":"session-42-write-1"}' \
  http://127.0.0.1:8080/api/commands
```

A newly admitted command returns `202`:

```json
{"accepted":true,"status":"pending","idempotency_key":"session-42-write-1"}
```

Poll the status endpoint with the same key:

```sh
curl --fail-with-body \
  -H 'Authorization: Bearer operator-secret' \
  'http://127.0.0.1:8080/api/commands/status?idempotency_key=session-42-write-1'
```

While the journal worker is processing the command, the response is:

```json
{"idempotency_key":"session-42-write-1","status":"pending"}
```

After the command is durable and applied, `status` is `completed` and
`response` contains the normal `CacheCommandResponse`:

```json
{"idempotency_key":"session-42-write-1","status":"completed","response":{"ok":true}}
```

The status record reports command completion, including a completed command
response with `ok:false` when the command itself was rejected after admission.
The idempotency key must be treated as a secret-bearing identifier when keys
contain sensitive business context because it is included in the status URL.

## Retries And Errors

Retry the same request with the same idempotency key after a transport failure.
The server returns the existing `pending` record or the original completed
response without applying the mutation twice. Reusing a key with a different
canonical command returns `409 Conflict`.

| HTTP status | Meaning |
| ---: | --- |
| `200` | The key already completed; the response is replayed. |
| `202` | The command was admitted and is pending durable application. |
| `400` | The status request has a missing or invalid idempotency key. |
| `401` | Monitoring authentication failed. |
| `409` | Async mode is disabled, prerequisites are missing, the command is not a journalable public write, or the key conflicts. |
| `429` | The journal queue or bounded status registry is full. Retry with backoff. |
| `503` | The journal is closed or does not support async submission. |

Unknown status after a crash means the command was not recoverable from the
retained journal idempotency records. Keep the journal and its retained
segments with backups when retry suppression must survive restore. Snapshot-only
backups do not contain the complete journal idempotency map.

## Compatibility And Security

The feature accepts public journalable writes, including batches composed only
of such writes. Reads, internal replication commands, leader-enforced writes,
and replicated handlers stay on their existing paths. The async header does
not silently turn a default-off handler on; it receives `409` until the
feature is explicitly configured.

Use TLS and a monitoring bearer token for remote access. Configure RBAC and
write protection according to the existing monitoring API policy. Do not use
the replication authentication token as a normal command credential.

The measured caller-side benefit and its completion boundary are recorded in
[BENCHMARK.md](BENCHMARK.md#asynchronous-http-command-admission).
