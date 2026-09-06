# Remote Parts

`hat/hatStorage.RemotePartReference` records the relationship between an
immutable remote object and local metadata. It does not fetch, write, or
delete remote data.

```go
reference, err := hatStorage.NewRemotePartReference(
	"s3://bucket/parts/part-001.bin?versionId=7",
	"parts/part-001.json",
	"sha256:...",
	4096,
)
metadataPath, err := reference.ResolveMetadataPath("/var/lib/hatrie/metadata")
```

Supported URI schemes are `s3`, `gs`, `az`, `http`, and `https`. The URI must
have a host and object path, and userinfo and fragments are rejected. Query
parameters are preserved for signed URLs. The checksum is caller-supplied and
the size may be zero for an empty part.

Metadata paths are relative, normalized, and rejected if they contain parent
directory traversal. `ResolveMetadataPath` confines the result to the supplied
root, but deliberately does not follow symlinks. Callers must apply their own
filesystem trust policy before opening metadata and must implement remote fetch
and checksum verification around this reference.
