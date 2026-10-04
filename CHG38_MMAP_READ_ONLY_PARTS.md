# CH-G38 Mmap-backed Read-only Parts

`hat/hatMappedPart` provides an explicit, bounded read-only view over an
immutable regular file. It is intended for callers that repeatedly inspect a
large persistent part and want to avoid copying the part into a Go heap slice
for every read.

This is an opt-in building block. It is not the default storage format, does
not replace Pebble or LevelDB, and is not automatically used by snapshot or
backup code.

## Example

```go
package main

import (
	"fmt"

	"hatrie_cache/hat/hatMappedPart"
)

func readPart(path string, expectedSHA256 string) error {
	part, err := hatMappedPart.OpenMappedReadOnlyPart(path, hatMappedPart.MappedReadOnlyPartOptions{
		MaxBytes:       64 << 20,
		ExpectedSize:   1 << 20,
		VerifySize:     true,
		ExpectedSHA256: expectedSHA256,
	})
	if err != nil {
		return err
	}
	defer part.Close()

	view := part.Bytes()
	fmt.Println(part.Size(), view[:4])
	return nil
}
```

`MaxBytes` is required and prevents an unexpectedly large file from being
mapped. `VerifySize` makes `ExpectedSize` an exact-size check. A non-empty
`ExpectedSHA256` is normalized and checked before the part is returned. An
empty file is supported without creating an mmap region.

## Safety and lifetime

- The path must name a regular, non-symlink file. The package rejects NUL
  bytes, symlinks, directories, and files outside the configured size budget.
- The file must remain unchanged for the lifetime of the returned part. A
  caller should publish immutable files atomically, then open the published
  path. Replacing the path does not change an already-open mapping, but
  truncating or modifying the underlying file is not supported.
- `Bytes` is a borrowed read-only view. Do not retain or use it after
  `Close`, and do not mutate it through unsafe code.
- `Close` is idempotent. It unmaps the view and closes the file descriptor.
- Unsupported operating systems return `ErrMappedReadOnlyPartUnsupported`.
  Callers that need a portable fallback should use the existing ordinary file
  reader or snapshot loader when this error is returned.

## Validation errors

The package returns sentinel errors that can be checked with `errors.Is`:

- `ErrMappedReadOnlyPartOptionsInvalid`
- `ErrMappedReadOnlyPartTooLarge`
- `ErrMappedReadOnlyPartSizeMismatch`
- `ErrMappedReadOnlyPartChecksumMismatch`
- `ErrMappedReadOnlyPartNotRegular`
- `ErrMappedReadOnlyPartUnsupported`

The implementation performs `Lstat` validation before opening the file and
also verifies the opened regular file's size. The publisher must still keep
the file immutable; filesystem policy and atomic publication remain the
caller's responsibility.

## Benchmark and tradeoff

The focused benchmark reads 16 deterministic offsets from a 1 MiB immutable
file. Five samples were collected with `-benchmem` on Linux/amd64. The
baseline opens and reads the file on every iteration. The reuse path opens and
validates once outside the timed loop, then reuses the mapped view. The
open-close path includes mapping and unmapping in the timed loop.

| Path | Raw ns/op samples | Median ns/op | Go heap B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Read file for every iteration | 241826, 250344, 236835, 237225, 201856 | 237225 | 1,057,193 | 5 |
| Reuse one mapped view | 483.7, 483.6, 473.5, 500.8, 496.7 | 483.7 | 0 | 0 |
| Open and close mapped view | 37802, 37892, 37014, 37942, 37636 | 37802 | 824 | 7 |

Compared with rereading the file, the reused view measured about 490.4x lower
CPU time and eliminated the measured Go heap allocation. Opening and closing
for each operation measured about 6.28x lower CPU time and 1,283x lower Go
heap bytes, but used two more allocations per operation. The mmap pages are
file-backed memory, so `B/op` is Go heap accounting and is not a complete RSS
measurement.

The main cost is correctness discipline: the file must be immutable, the
mapping consumes virtual address space, and the caller must explicitly manage
the lifetime. The default remains unchanged because these requirements are
not appropriate for arbitrary mutable cache files.

Run the focused verification with:

```text
make format-chg38-mmap-readonly
make test-chg38-mmap-readonly
make race-chg38-mmap-readonly
make vet-chg38-mmap-readonly
make benchmark-chg38-mmap-readonly
```
