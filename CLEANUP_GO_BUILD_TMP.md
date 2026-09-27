# Go Build Temporary Cleanup

Go can leave large top-level `/tmp/go-build*` directories after a build or
test is interrupted. The repository cleanup utility handles these separately
from Hatrie worktrees and test directories.

Preview the current selection:

```text
make cleanup-go-build-tmp-preview
```

The preview writes a plan only when a directory is old enough and no process
has its working directory or an open file below that path. The default grace
period is 300 seconds. Change it explicitly when reviewing a controlled
environment:

```text
HATRIE_GO_BUILD_MIN_AGE_SECONDS=3600 make cleanup-go-build-tmp-preview
```

After reviewing the printed candidates and the plan, apply that exact plan:

```text
make cleanup-go-build-tmp-apply
```

Apply rechecks the path scope, inode/size/mtime signature, and active process
references before removing anything. Empty plans are deleted automatically.
The default behavior never touches directories containing `.git`, active
worktrees, or recent build output.

The focused fixture test is:

```text
make test-cleanup-go-build-tmp
```
