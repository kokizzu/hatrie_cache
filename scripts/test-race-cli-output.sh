#!/bin/sh
set -eu

go test -race ./cmd/hatrie-cli -run '^TestCLIOutput' -count=1
