#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatHttp -run '^TestBinaryStreamReaderNextInto' -count=1
