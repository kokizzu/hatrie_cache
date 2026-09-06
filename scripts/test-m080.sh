#!/bin/sh
set -eu

go test ./hat/hatSql -run '^Test(ExplainAnalyze(PublishesRejectedOptimizerAlternatives|MarksSelectedOptimizerAlternative)|RegularExplainOmitsOptimizerMetadata)$' -count=1
