#!/bin/sh
set -eu

gofmt -w \
	hat/hatPredicate/mask.go \
	hat/hatPredicate/mask_test.go
