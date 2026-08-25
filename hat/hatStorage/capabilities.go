package hatStorage

import (
	"fmt"
	"strings"
)

// Capability describes one persistence operation supported by a store.
type Capability string

const (
	CapabilitySave        Capability = "save"
	CapabilityLoad        Capability = "load"
	CapabilityIncremental Capability = "incremental_save"
	CapabilityFlush       Capability = "flush"
	CapabilitySpill       Capability = "spill_cold"
	CapabilityCompaction  Capability = "compaction"
	CapabilityCheckpoint  Capability = "checkpoint"
)

// Properties contains portable, diagnostic-only engine properties. Values are
// intentionally strings because both LevelDB and Pebble expose engine-native
// textual diagnostics.
type Properties struct {
	Stats      string `json:"stats,omitempty"`
	SSTables   string `json:"sstables,omitempty"`
	WriteDelay string `json:"write_delay,omitempty"`
	BlockPool  string `json:"block_pool,omitempty"`
}

// Inspector is the narrow, read-only contract required to inspect a store.
// It deliberately excludes cache mutation, lifecycle, and implementation
// handles so operators can use the model independently of hatriecache.
type Inspector interface {
	Backend() Backend
	Path() string
	Format() Format
	Properties() (Properties, error)
}

// Engine is the common lifecycle and inspection surface implemented by local
// persistent engines such as LevelDB and Pebble. Cache-specific read/write
// methods intentionally remain outside this narrow package boundary.
type Engine interface {
	Inspector
	Close() error
}

// Inspection is a portable store report suitable for operator APIs.
type Inspection struct {
	Backend      Backend      `json:"backend"`
	Path         string       `json:"path"`
	Format       Format       `json:"format"`
	Capabilities []Capability `json:"capabilities"`
	Properties   Properties   `json:"properties"`
}

// Supports reports whether the inspected store supports capability.
func (inspection Inspection) Supports(capability Capability) bool {
	for _, available := range inspection.Capabilities {
		if available == capability {
			return true
		}
	}
	return false
}

// Capabilities returns a fresh list of operations supported by backend.
func Capabilities(backend Backend) []Capability {
	base := []Capability{
		CapabilitySave,
		CapabilityLoad,
		CapabilityIncremental,
		CapabilityFlush,
		CapabilitySpill,
		CapabilityCompaction,
	}
	if backend == BackendPebble {
		base = append(base, CapabilityCheckpoint)
	}
	return base
}

// Inspect builds a validated, portable store report.
func Inspect(store Inspector) (Inspection, error) {
	if store == nil {
		return Inspection{}, fmt.Errorf("storage inspector is required")
	}
	backend, err := ParseBackend(string(store.Backend()))
	if err != nil || backend == BackendAuto {
		return Inspection{}, fmt.Errorf("invalid storage backend %q", store.Backend())
	}
	format, err := ParseFormat(string(store.Format()))
	if err != nil {
		return Inspection{}, err
	}
	path := strings.TrimSpace(store.Path())
	if path == "" {
		return Inspection{}, fmt.Errorf("storage path is required")
	}
	properties, err := store.Properties()
	if err != nil {
		return Inspection{}, err
	}
	return Inspection{
		Backend:      backend,
		Path:         path,
		Format:       format,
		Capabilities: Capabilities(backend),
		Properties:   properties,
	}, nil
}
