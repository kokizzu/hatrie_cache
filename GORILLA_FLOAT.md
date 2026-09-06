# Gorilla-Style Float Codec

`hat/hatCodec` provides `EncodeGorillaFloat64` and
`DecodeGorillaFloat64` for repeated or slowly changing floating-point
sequences.

```go
encoded, err := hatCodec.EncodeGorillaFloat64(values)
if err != nil {
    return err
}
values, err = hatCodec.DecodeGorillaFloat64(encoded)
```

The format stores a varint element count and the first IEEE-754 bit pattern,
then XORs each following pattern against its predecessor. Unchanged values
cost one control byte. Changed values store the XOR's leading-zero count,
significant-bit count, and only the required little-endian payload bytes.
Round trips preserve all bits, including `NaN` payloads and signed zero.

The count and exact-consumption checks reject truncation, overflow, invalid
windows, non-zero padding, and trailing bytes. This codec is opt-in and does
not change existing storage or wire defaults. High-entropy values can be
larger than raw eight-byte values because control and window metadata is
included; repeated values are the best case.
