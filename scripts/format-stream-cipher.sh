#!/bin/sh
set -eu

gofmt -w ./hat/hatCodec/stream_cipher.go ./hat/hatCodec/stream_cipher_test.go
