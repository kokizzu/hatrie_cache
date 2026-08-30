#!/bin/sh
set -eu

sed -n '1080,1165p' hat/hatCache/main.go
sed -n '6660,6825p' hat/hatCache/main.go
sed -n '2410,2465p' hat/hatCache/sql_query.go
