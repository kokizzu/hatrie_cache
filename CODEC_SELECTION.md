# Entropy-Based Codec Selection

`hat/hatCodec` exposes an advisory entropy estimator and codec selector for
callers choosing a representation from a sampled column.

```go
choice, entropy, err := hatCodec.SelectCodecFromSample(sample)
if err != nil {
    return err
}
fmt.Println(choice, entropy)
```

`EstimateByteEntropy` returns a Shannon estimate in bits per byte using a
256-bin histogram. `SelectCodecFromEntropy` recommends
`CodecChoiceCompressedBlocks` below 7 bits per byte and `CodecChoiceRaw` at or
above that threshold. Empty samples are treated as zero entropy.

This is a recommendation, not an automatic format change. Real compression
ratio, CPU cost, block size, and downstream compatibility should still be
measured before persisting the choice. High-entropy data may expand under
compression. The estimator does not retain sample data and the selector does
not perform I/O or mutate codec configuration.
