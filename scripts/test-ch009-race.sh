#!/usr/bin/env bash
set -eu

go test -race ./hat/hatCache -run '^TestCH009'
