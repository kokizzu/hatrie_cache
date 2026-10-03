# TT-039 Live Credential Rotation

\`hatAuth\` now provides an atomic live token rotator for listeners that must
change credentials without rebuilding an HTTP or gRPC provider chain.

\`\`\`go
rotator := hatAuth.NewTokenRotator("current-token", "", time.Time{})
provider := hatAuth.RotatingTokenIdentity{Rotator: rotator}

if err := rotator.Rotate(
	"next-token",
	"current-token",
	time.Now().Add(5*time.Minute),
); err != nil {
	return err
}
\`\`\`

The rotator publishes immutable \`TokenSet\` snapshots through
\`atomic.Pointer\`. Authentication reads do not take a lock and do not allocate.
During an overlap, both the current and previous token are accepted; the
previous token stops matching at its expiry. \`Rotate\` rejects an empty snapshot
and rejects an overlap token without an expiry.

The existing immutable \`TokenSet\` and \`LocalTokenIdentity\` APIs are unchanged.
Tokens remain in memory only, are normalized before comparison, and continue to
use constant-time comparison. Callers should avoid logging token arguments and
should keep the overlap interval no longer than required for rollout.

## Measured Tradeoff

Measured with:

\`\`\`text
go test -run '^$' -bench '^BenchmarkTT039' -benchmem -count=5 ./hat/hatAuth
\`\`\`

Environment: Linux amd64, AMD Ryzen 9 5950X 16-Core Processor. Values below are
medians of five samples.

| Operation | Median CPU | Heap bytes/op | Allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Immutable \`TokenSet.Matches\` | 12.82 ns | 0 B | 0 | baseline |
| \`TokenRotator.Matches\` | 14.77 ns | 0 B | 0 | 1.15x CPU, +1.95 ns |
| \`TokenRotator.Rotate\` | 93.35 ns | 64 B | 1 | infrequent control-plane update |

Raw samples:

\`\`\`text
immutable match: 12.92 12.82 12.73 12.94 12.76 ns/op
rotating match:  14.77 14.80 14.75 14.43 14.97 ns/op
rotation:        93.48 92.95 93.33 93.35 93.81 ns/op
\`\`\`

The feature is opt-in. The ordinary \`TokenSet\` path has no new atomic load or
allocation. The live provider pays about 1.15x read CPU with no heap growth,
while rotations allocate one immutable snapshot so readers never observe
partially updated credentials.

## Verification

Tests cover overlap and expiry, invalid rotations, nil/invalid requests,
concurrent reads and rotations under the race detector, and 100 repeated
focused runs.
