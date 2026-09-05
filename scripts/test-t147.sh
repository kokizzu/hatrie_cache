#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCommand -run '^TestCommandResponseCodeProtobufRoundTrip$' -count=1
go test ./hat/hatCache -run '^(TestCommandErrorIncludesStableCode|TestCommandErrorCodes)$' -count=1
