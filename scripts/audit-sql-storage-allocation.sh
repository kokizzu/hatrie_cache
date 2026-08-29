#!/usr/bin/env sh
set -eu

grep -R -n -m 500 -E 'type (AdaptivePlanner|ColumnarBatch|SQLColumnar|sql.*Arena|sql.*Pool)|ResolveSQLColumnarSource|Decode.*JSON|json.*Decode|sync\.Pool|Workers' hat/hatCache hat/hatSql
