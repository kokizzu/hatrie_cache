# Reusable Cursor-Token Encoding

`hatDataStructure.CursorTokenCodec.EncodeInto` is an opt-in byte-oriented
encoder for HTTP or RPC callers that can write a token byte slice directly.
It produces exactly the same authenticated URL-safe token as `Encode`.

```go
var scratch []byte
for id := uint64(1); id <= 100; id++ {
	token, err := codec.EncodeInto(scratch[:0], "orders_by_id", 7, key, id)
	if err != nil {
		return err
	}
	if _, err := writer.Write(token); err != nil {
		return err
	}
	scratch = token[:0]
}
```

The returned slice contains the base64 token at the front and retains the raw
signed payload in its backing-array tail. That tail allows subsequent calls to
reuse both working storage and output capacity. A first call with insufficient
capacity grows once; `Encode` remains unchanged and still returns an owned
string.

`make benchmark-cursor-token` and
`make benchmark-cursor-token-encode-into` used ten `-benchmem` samples with a
small index name and timestamp key:

| Path | Median ns/op | B/op | Allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| `Encode` | 664 | 864 | 9 | baseline |
| reused `EncodeInto` | 571 | 480 | 5 | 1.16x faster |

The signature remains HMAC-SHA256 authenticated and maximum index/key/token
limits are unchanged.
