#!/bin/sh
set -eu

gofmt -w ./cmd/hatrie-sbom/main.go ./hat/hatCache/local_verification_test.go
