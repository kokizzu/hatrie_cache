#!/usr/bin/env bash
set -euo pipefail

paths=(
	TG34_STREAMING_BINARY_RESPONSES.md
	hat/hatHttp/binary_stream.go
	hat/hatHttp/binary_stream_test.go
	hat/hatHttp/binary_stream_benchmark_test.go
	scripts/bench-tg34-binary-stream.sh
	scripts/commit-tg34-binary-stream.sh
	scripts/format-tg34-binary-stream.sh
	scripts/race-tg34-binary-stream.sh
	scripts/test-tg34-binary-stream-package.sh
	scripts/test-tg34-binary-stream.sh
	scripts/vet-tg34-binary-stream.sh
)

git add -- "${paths[@]}"
git diff --cached --check -- "${paths[@]}"
git commit --only -m "feat(http): add framed binary streaming responses" -- "${paths[@]}"
git push origin HEAD
