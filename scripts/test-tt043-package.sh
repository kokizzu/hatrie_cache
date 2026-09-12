#!/bin/sh
set -eu

go test ./hat/hatCache ./cmd/hatrie-cache -count=1
