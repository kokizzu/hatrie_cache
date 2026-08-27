#!/bin/sh
set -eu

go test ./hat/hatCodec -run '^TestStreamCipher' -count=1
