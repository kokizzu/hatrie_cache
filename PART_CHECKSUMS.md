# Immutable Part Checksums

`hatMerkle.PartChecksum` provides an opt-in integrity record for immutable
backup or replication parts. It stores the exact byte length and a SHA-256
digest, and its canonical wire form is 54 characters of unpadded Base64 for a
40-byte payload.

```go
checksum := hatMerkle.ChecksumPart(part)
wireValue := checksum.Encode()

received, err := hatMerkle.DecodePartChecksum(wireValue)
if err != nil {
	return err
}
if !hatMerkle.VerifyPartChecksum(receivedPart, received) {
	return errors.New("immutable part failed integrity verification")
}
```

Verification rejects both changed bytes and changed length. Decoding rejects
wrong-length, invalid, padded, and non-canonical Base64 values. The checksum is
an integrity check, not authentication: untrusted peers still require the
existing transport authentication and authorization controls.

This primitive is not automatically added to the replication or storage path,
so existing backups, wire formats, and latency are unchanged. Hashing costs one
SHA-256 pass over the part. The reference benchmark uses a 1 MiB payload and
reports roughly `0 B/op` and `0 allocs/op`; use it at immutable part boundaries,
where that pass is preferable to accepting silent transfer corruption.
