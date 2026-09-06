# Deterministic Replica Replay Checks

`hatReplication.DigestReplayRecords` computes a canonical SHA-256 digest for
an ordered stream of opaque replay payloads. It frames each sequence number and
payload length before hashing, so payload boundaries cannot be ambiguous.

```go
expected := []hatReplication.ReplayRecord{
	{Sequence: 1, Payload: []byte("SET a 1")},
	{Sequence: 2, Payload: []byte("SET b 2")},
}
actual := append([]hatReplication.ReplayRecord(nil), expected...)
if err := hatReplication.VerifyReplayRecords(expected, actual); err != nil {
	panic(err)
}
```

Sequences must be positive and strictly increasing. A reordered, missing, or
changed record returns `hatReplication.ErrReplayMismatch`; malformed sequence
order returns `hatReplication.ErrReplaySequenceInvalid`. The helper is pure,
does not mutate payloads, and leaves command decoding to the caller so it can
be used by different journal or transport adapters.
