package hatSql

import "sync"

type sqlSpillQuota struct {
	limit int64
	mu    sync.Mutex
	used  int64
	files map[string]int64
}

func newSQLSpillQuota(limit int64) *sqlSpillQuota {
	return &sqlSpillQuota{limit: limit, files: map[string]int64{}}
}

func (quota *sqlSpillQuota) reserve(path string, bytes int64) bool {
	if quota == nil || bytes <= 0 {
		return true
	}
	quota.mu.Lock()
	defer quota.mu.Unlock()
	if bytes > quota.limit-quota.used {
		return false
	}
	quota.used += bytes
	quota.files[path] += bytes
	return true
}

func (quota *sqlSpillQuota) refund(path string, bytes int64) {
	if quota == nil || bytes <= 0 {
		return
	}
	quota.mu.Lock()
	defer quota.mu.Unlock()
	tracked := quota.files[path]
	if bytes > tracked {
		bytes = tracked
	}
	if bytes <= 0 {
		return
	}
	quota.used -= bytes
	tracked -= bytes
	if tracked == 0 {
		delete(quota.files, path)
	} else {
		quota.files[path] = tracked
	}
}

func (quota *sqlSpillQuota) release(path string) {
	if quota == nil {
		return
	}
	quota.mu.Lock()
	defer quota.mu.Unlock()
	bytes := quota.files[path]
	if bytes <= 0 {
		return
	}
	quota.used -= bytes
	delete(quota.files, path)
}

func (quota *sqlSpillQuota) usedBytes() int64 {
	if quota == nil {
		return 0
	}
	quota.mu.Lock()
	defer quota.mu.Unlock()
	return quota.used
}
