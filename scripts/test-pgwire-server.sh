#!/bin/sh
set -eu

go test ./hat/hatPgWire -run '^TestServeConn'
