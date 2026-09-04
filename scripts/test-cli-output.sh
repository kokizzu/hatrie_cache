#!/bin/sh
set -eu

go test ./cmd/hatrie-cli -run '^TestCLIOutput' -count=1
