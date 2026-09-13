#!/usr/bin/env bash
set -eu

go test ./hat/hatCache -run '^TestStorageKeyPinning' -count=1
