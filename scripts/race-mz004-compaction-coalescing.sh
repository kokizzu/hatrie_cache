#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPipeline -run '^TestMZ004(Coalescing|Priority)' -count=1
