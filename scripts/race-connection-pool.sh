#!/bin/sh
set -eu
exec go test -race ./hat/hatReplication
