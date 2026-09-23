#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatHttp -run 'Test(BinaryStream|StreamBinaryHTTP)' -count=1
