# Bit-Packed Numeric Codec

`hat/hatCodec` provides `EncodeBitPackedUint64` and
`DecodeBitPackedUint64` for numeric columns whose values fit in fewer than 64
bits.

```go
encoded, err := hatCodec.EncodeBitPackedUint64(values)
if err != nil {
    return err
}
values, err = hatCodec.DecodeBitPackedUint64(encoded)
```

The format stores a varint element count, the minimum bit width required by
the maximum value, and a packed little-endian bit stream. An all-zero column
uses width zero; a full-range column uses width 64. The count and exact
payload-size checks reject truncation and trailing bytes, while padding bits
and invalid widths are rejected as non-canonical input.

This helper is opt-in and does not change existing storage or wire defaults.
It is most useful for bounded integer columns; high-range values receive less
benefit than raw fixed-width storage. The decoder allocates the result slice,
while encoding allocates only the output payload.
