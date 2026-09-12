#!/bin/sh
set -eu
exec go test -run '^TestConnectionPool' ./hat/hatReplication
