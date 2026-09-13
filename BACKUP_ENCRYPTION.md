# Encrypted Object-Store Backups

Hatrie Cache now supports opt-in authenticated encryption for the public
`hatBackup.ObjectStoreTarget`. The historical `NewObjectStoreTarget` constructor
and all existing unencrypted bundles remain readable and unchanged. The
separate local `hatCache.CreateBackupBundle` tar/Pebble bundle path is outside
this feature and remains unencrypted unless it is protected by the surrounding
filesystem or storage service.

## Configuration

Use a 32-byte AES key from a secret manager. Keep the active key and retired
keys in the target keyring during the restore retention period:

```go
target, err := hatBackup.NewObjectStoreTargetWithOptions(store, "backups/node-a", hatBackup.ObjectStoreTargetOptions{
	EncryptionKeys: []hatBackup.ObjectStoreEncryptionKey{
		{ID: "backup-2026-01", Key: currentKey},
		{ID: "backup-2025-12", Key: retiredKey},
	},
	ActiveEncryptionKeyID: "backup-2026-01",
})
```

An empty `ObjectStoreTargetOptions` value disables encryption. A keyring with
no active key, duplicate IDs, non-32-byte keys, or an active ID absent from the
keyring is rejected at construction time.

## Format And Rotation

Payload objects use `AES-256-GCM-CHUNKED-V1` with a random per-object nonce and
64 KiB authenticated frames. Each frame binds the object-relative path as
additional authenticated data. The manifest is an authenticated envelope that
contains the key ID in its envelope header so restore can select a historical
key; the manifest body also records the algorithm, key ID, and frame size.

The key itself is never written to the manifest or object bytes. Object-store
object names remain visible to the storage provider, while manifest contents
and payload contents are encrypted. The manifest's SHA-256 values and sizes
continue to describe plaintext, so `Verify` and `Restore` validate the same
logical backup after decryption.

To rotate keys, add the new key to the keyring, set it as
`ActiveEncryptionKeyID`, and write new backups. Keep the old key available for
all retained backups. Remove it only after those backups and any incremental
chain that depends on them have expired. A missing or wrong key, altered
manifest, altered frame, truncated object, or trailing object bytes fails
before restore publication.

## Compatibility

`NewObjectStoreTarget(store, prefix)` is the default compatibility path. It
writes and restores the existing plaintext manifest and payload format. An
encryption-enabled target can restore both encrypted bundles for keys in its
keyring and legacy unencrypted bundles. An encrypted bundle cannot be restored
without its keyring.

## Measured Tradeoff

The benchmark uses the in-memory object-store test double, one 10,240-byte
payload file, and five benchmark samples on an AMD Ryzen 9 5950X. `B/op` is
transient Go allocation accounting for the whole test-double operation; it is
not resident cache memory.

| Operation | Unencrypted median | Encrypted median | Relative cost | Stored bytes |
| --- | ---: | ---: | ---: | ---: |
| Backup | 53,477 ns/op; 40,263 B; 76 allocs | 57,976 ns/op; 77,828 B; 94 allocs | 1.08x CPU; 1.93x transient bytes; 1.24x allocs | 10,240 -> 10,280 |
| Restore | 1,672,541 ns/op; 52,900 B; 122 allocs | 1,728,067 ns/op; 78,397 B; 139 allocs | 1.03x CPU; 1.48x transient bytes; 1.14x allocs | 10,240 -> 10,280 |

The security feature has an intentional CPU and transient-allocation cost. The
wire/storage expansion for this fixture is 40 bytes, or 0.39%, because the
manifest is stored separately and each payload gets only a fixed envelope plus
one 16-byte authentication tag per frame.

Raw benchmark samples:

```text
BenchmarkObjectStoreTargetBackup: 53818, 54687, 53477, 52950, 53221 ns/op; 40262-40268 B/op; 76 allocs/op
BenchmarkObjectStoreTargetEncryptedBackup: 59129, 57345, 59559, 57582, 57976 ns/op; 77826-77834 B/op; 94 allocs/op
BenchmarkObjectStoreTargetRestore: 1679218, 1640175, 1899331, 1657313, 1672541 ns/op; 52853-52920 B/op; 122 allocs/op
BenchmarkObjectStoreTargetEncryptedRestore: 1913820, 2112816, 1718782, 1698816, 1728067 ns/op; 78329-78485 B/op; 139 allocs/op
```

## Verification Commands

```text
make test-ch049
make race-ch049
make vet-ch049
make check-ch049
make benchmark-ch049-before
make benchmark-ch049-after
```
