#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatFiber -run '^(TestTU(30|31)|ExampleChannel)$' -count=1
