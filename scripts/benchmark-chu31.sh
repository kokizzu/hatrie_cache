#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench '^BenchmarkCHU31(ChecksumBaseline|MultipartChecksumBaseline|MultipartUpload|MultipartUploadFourParts)$' -benchmem -count=5
