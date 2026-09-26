#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench 'BenchmarkTU047(HTTPStatus|DirectParticipantStatus)$' -benchmem -benchtime=200ms -count=3
