# T238: Ordered Compact Binary Request Batches

`hatPeer.CompactPeerSession.CallBatch` is an opt-in transport API for sending
multiple commands in one compact binary request frame.

## API

```go
responses, err := session.CallBatch(ctx, []hatPeer.CompactBatchRequest{
	{Command: []byte("GET"), Payload: []byte("orders:1")},
	{Command: []byte("SETSTR"), Payload: []byte(`{"key":"orders:1","value":"ready"}`)},
})
```

The returned `[]hatPeer.CompactBatchResponse` has the same order as the input.
Each response contains `Kind`, `Command`, `ResponseSchemaID`, and `Payload`.
`CompactResponse` is a successful item; `CompactError` is an item-level failure.

Servers adapt an existing handler with `NewCompactBatchHandler`:

```go
handler, err := hatPeer.NewCompactBatchHandler(func(
	ctx context.Context,
	frame hatPeer.CompactFrame,
) (hatPeer.CompactFrame, error) {
	return handleCommand(ctx, frame)
})
```

Use `NewCompactBatchHandlerWithProtocol` when the server session uses custom
frame, command, or payload limits. The adapter executes items sequentially and
continues after a handler error, so one bad item does not hide later results.

## Wire and limits

The outer command is `BATCH`. Its bounded payload contains a version, item
count, and length-prefixed command/payload pairs. Responses use the same
versioned envelope with a response kind, schema ID, command, and payload for
each item.

- The default batch bound is `DefaultCompactBatchMaxItems` (`256`).
- Existing compact protocol command, payload, and frame limits apply to the
  batch envelope and every item.
- Empty batches, malformed lengths, trailing bytes, invalid response kinds,
  oversized commands, and oversized payloads are rejected before handler work.
- Normal `Call` frames and the default protocol remain unchanged.
- Payload compression is not duplicated: compressed sessions use the existing
  `MarshalBatch` plus `Call` path, while uncompressed sessions encode directly
  into the reusable session write buffer.

Deploy the batch handler on the receiving side before sending `BATCH` frames.
Older peers that do not register the adapter do not gain batch support; callers
should fall back to ordinary `Call` requests when mixed-version compatibility
is required.

## Cancellation and failures

`CallBatch` follows normal compact-session context behavior. A canceled context
removes the local pending request and sends the existing best-effort request
cancellation when cancellation is enabled. Transport failures fail the session
as they do for ordinary calls. Item handler failures stay inside the ordered
response list and do not become transport failures.

## Measurement

The paired benchmark encodes 32 representative `SETSTR` commands. It compares
32 individual compact frames with one batch frame; it does not include network
latency or application handler time. The direct encoder is the production path
for uncompressed sessions and reuses the session's bounded write buffer.

See [BENCHMARK.md](BENCHMARK.md#t238-ordered-compact-binary-request-batches) for
raw samples and the tradeoff table.
