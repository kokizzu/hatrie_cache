# WAL Encryption And Key Rotation

`hatJournal` supports optional authenticated encryption for command-journal
records. The default remains the legacy plaintext format for compatibility and
zero crypto overhead.

## Configuration

Use a current AES key and a stable key ID when opening a writable journal:

```go
options := hatCache.CommandJournalOptions{
	Format:              hatCache.CommandJournalFormatBinary,
	GroupCommitWindow:   2 * time.Millisecond,
	GroupCommitMaxBatch: 64,
	Encryption: hatJournal.EncryptionOptions{
		KeyID: "wal-2026-09",
		Key:   currentKeyFromSecretStore,
	},
}
journal, err := hatCache.OpenCommandJournalWithOptions(path, options)
```

`Key` must be 16, 24, or 32 bytes. Keep key material in a secret manager or
protected process environment; do not put it in a journal, repository, or
backup manifest. `KeyID` is metadata and is intentionally stored in cleartext
inside each frame so readers can select the right key.

Reading during rotation keeps the old key in the keyring:

```go
Encryption: hatJournal.EncryptionOptions{
	KeyID: "wal-2026-10",
	Key:   newKey,
	Keyring: map[string][]byte{
		"wal-2026-09": oldKey,
	},
},
```

The current key is used for new records. Keyring entries are read-only keys for
existing records. A writable journal must have a current key; a reader may be
configured with only a keyring.

## Rotation Procedure

1. Generate a new AES key and assign a new key ID.
2. Deploy readers and writers with the new key as `Key` and the previous key
   in `Keyring`.
3. Keep every key needed by retained journal segments and backups.
4. Compact or checkpoint the journal so old plaintext or old-key segments are
   removed before retiring their keys.
5. Remove the retired key only after the retention and restore windows expire.

Existing plaintext records are readable when encryption is enabled, and only
newly appended records are encrypted. This allows an online transition but
does not retroactively protect old bytes. The encrypted reader recognizes
frames at binary-record or JSON-line boundaries, so a legacy value containing
the frame marker remains ordinary data.

For offline maintenance, use
`InstallCommandJournalCheckpointWithOptions` so a checkpoint replacement keeps
the configured encryption. The older
`InstallCommandJournalCheckpoint` function intentionally retains its legacy
behavior.

## Format And Failure Behavior

Each encrypted record contains a versioned `HJE1` marker, key ID, random
12-byte nonce, ciphertext length, and AES-GCM ciphertext plus tag. The header
and key ID are authenticated as associated data. A missing key returns
`hatJournal.ErrEncryptionKey`; modified ciphertext or headers return
`hatJournal.ErrEncryptionAuthentication`. A truncated final frame is treated
as an incomplete tail by journal inspection and recovery.

This protects journal record confidentiality and integrity. It does not
encrypt snapshots, local backup bundles, object-store backup payloads, or
application logs; protect those through their respective backup and secret
management controls.

## Measured Cost

The isolated benchmark uses a 256-byte record on an AMD Ryzen 9 5950X and
compares a plaintext memory copy with AES-GCM frame encoding. The control is
not a complete disk or group-commit benchmark.

| Encoding | Median ns/op | B/op | Allocs/op | Wire size |
| --- | ---: | ---: | ---: | ---: |
| Legacy copy | 5.534 | 0 | 0 | 256 bytes |
| AES-GCM frame | 450.0 | 704 | 5 | 299 bytes |

For the `current` key ID used by the benchmark, encryption adds 43 bytes per
256-byte record (16.8%). It is therefore a security feature with measurable
CPU, allocation, and bandwidth cost, not a throughput optimization. Run
`make benchmark-tr008-journal-encryption` on the deployment hardware before
enabling it for a latency-sensitive workload.
