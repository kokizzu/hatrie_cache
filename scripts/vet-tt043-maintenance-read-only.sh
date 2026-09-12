#!/bin/sh
set -eu

go vet ./hat/hatCache ./cmd/hatrie-cache
