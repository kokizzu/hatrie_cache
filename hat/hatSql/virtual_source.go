package hatSql

import (
	"fmt"
	"strings"
	"sync"
)

// VirtualSource is a read-only row snapshot provider. Implementations own
// file or HTTP access; the SQL engine only receives the resulting rows.
type VirtualSource interface{ Snapshot() ([]Row, error) }
type VirtualSourceFunc func() ([]Row, error)

func (source VirtualSourceFunc) Snapshot() ([]Row, error) { return source() }

type VirtualSources struct {
	mu      sync.RWMutex
	sources map[string]VirtualSource
}

func NewVirtualSources() *VirtualSources {
	return &VirtualSources{sources: make(map[string]VirtualSource)}
}
func (sources *VirtualSources) Register(name string, source VirtualSource) error {
	if sources == nil || source == nil {
		return fmt.Errorf("virtual source is required")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("virtual source name is required")
	}
	sources.mu.Lock()
	defer sources.mu.Unlock()
	if _, ok := sources.sources[name]; ok {
		return fmt.Errorf("virtual source %q already registered", name)
	}
	sources.sources[name] = source
	return nil
}
func (sources *VirtualSources) ResolveSQLVirtualSource(name string) ([]Row, error) {
	if sources == nil {
		return nil, fmt.Errorf("virtual sources are nil")
	}
	sources.mu.RLock()
	source, ok := sources.sources[strings.TrimSpace(name)]
	sources.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("virtual source %q does not exist", name)
	}
	rows, err := source.Snapshot()
	if err != nil {
		return nil, err
	}
	return CloneRows(rows), nil
}

// LogVirtualSource, MetricVirtualSource, FileVirtualSource, and
// HTTPSnapshotSource make the four standard virtual source kinds explicit.
type LogVirtualSource struct{ Read func() ([]Row, error) }

func (source LogVirtualSource) Snapshot() ([]Row, error) {
	if source.Read == nil {
		return nil, fmt.Errorf("log reader is required")
	}
	return source.Read()
}

type MetricVirtualSource struct{ Read func() ([]Row, error) }

func (source MetricVirtualSource) Snapshot() ([]Row, error) {
	if source.Read == nil {
		return nil, fmt.Errorf("metric reader is required")
	}
	return source.Read()
}

type FileVirtualSource struct{ Read func() ([]Row, error) }

func (source FileVirtualSource) Snapshot() ([]Row, error) {
	if source.Read == nil {
		return nil, fmt.Errorf("file snapshot reader is required")
	}
	return source.Read()
}

type HTTPSnapshotSource struct{ Fetch func() ([]Row, error) }

func (source HTTPSnapshotSource) Snapshot() ([]Row, error) {
	if source.Fetch == nil {
		return nil, fmt.Errorf("HTTP snapshot fetcher is required")
	}
	return source.Fetch()
}
