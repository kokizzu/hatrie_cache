# Entropy-Based Codec Selection

`hat/hatCodec` provides a small advisory selector for choosing between raw
bytes and the existing compressed-block representation. It is useful when a
caller has a representative sample of a column or value batch and wants to
avoid compressing data that is already high entropy.

```go
sample := payload
if len(sample) > 4096 {
	sample = sample[:4096]
}

choice, entropy, err := hatCodec.SelectCodecFromSample(sample)
if err != nil {
	return err
}
switch choice {
case hatCodec.CodecChoiceRaw:
	// Store or send payload without compression.
case hatCodec.CodecChoiceCompressedBlocks:
	// Use the existing compressed-block codec.
}
_ = entropy
```

`EstimateByteEntropy` uses a stack-resident 256-bin histogram and does not
retain the input. `SelectCodecFromSample` examines exactly the bytes supplied;
callers should pass a bounded prefix when sampling a large column. An empty
sample has zero estimated entropy and recommends compressed blocks.

The recommendation is deliberately conservative and does not compress data,
change a configured codec, or guarantee that compression will be smaller. The
default threshold is `hatCodec.RawEntropyThreshold` (`7.0` bits per byte):
samples at or above it recommend raw bytes, while lower-entropy samples
recommend compressed blocks. Use `SelectCodecFromEntropy` when the caller has
already computed an estimate, and treat `ErrCodecSelectionInvalid` as invalid
metadata rather than silently choosing a representation.

Tests cover repeated data, a uniform byte distribution, empty input, and
invalid entropy values. The selector is advisory, so callers remain free to
override it when storage or wire compatibility requires a specific codec.
