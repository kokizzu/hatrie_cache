#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatHttp -run 'Test(BinaryStream|StreamBinaryHTTP)' -count=1
