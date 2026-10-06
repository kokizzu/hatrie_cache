package hatStorage

import "fmt"

// EngineFamily identifies the storage layout family shared by one or more
// selectable backends.
type EngineFamily string

const (
	// EngineFamilyLSM is the log-structured merge-tree family used by the
	// selectable local persistent engines.
	EngineFamilyLSM EngineFamily = "lsm"
)

// EngineProfile is the operator-facing contract for one selectable storage
// engine. Tradeoff fields are qualitative guidance, not runtime guarantees;
// workload and hardware still determine measured performance.
type EngineProfile struct {
	Backend            Backend      `json:"backend"`
	Name               string       `json:"name"`
	Family             EngineFamily `json:"family"`
	Default            bool         `json:"default"`
	Checkpoint         bool         `json:"checkpoint"`
	ReadAmplification  string       `json:"read_amplification"`
	WriteAmplification string       `json:"write_amplification"`
	Compaction         string       `json:"compaction"`
	Backup             string       `json:"backup"`
	Recovery           string       `json:"recovery"`
}

var engineProfiles = [...]EngineProfile{
	{
		Backend:            BackendPebble,
		Name:               "pebble-lsm",
		Family:             EngineFamilyLSM,
		Default:            true,
		Checkpoint:         true,
		ReadAmplification:  "low-to-moderate with block cache and filters",
		WriteAmplification: "moderate with background LSM compaction",
		Compaction:         "automatic LSM compaction plus explicit compaction",
		Backup:             "native Pebble checkpoints and generation/journal bundles",
		Recovery:           "checkpoint adoption and journal recovery are supported",
	},
	{
		Backend:            BackendLevelDB,
		Name:               "leveldb-legacy-lsm",
		Family:             EngineFamilyLSM,
		Default:            false,
		Checkpoint:         false,
		ReadAmplification:  "moderate with block cache and filters",
		WriteAmplification: "moderate-to-high while legacy levels compact",
		Compaction:         "explicit and periodic legacy LevelDB compaction",
		Backup:             "directory/generation snapshots without native Pebble checkpoints",
		Recovery:           "directory restore and journal replay",
	},
}

// EngineProfiles returns a fresh list of the selectable local engine
// profiles. The returned values can be safely modified by the caller.
func EngineProfiles() []EngineProfile {
	profiles := make([]EngineProfile, len(engineProfiles))
	copy(profiles, engineProfiles[:])
	return profiles
}

// ProfileForBackend returns the validated profile for an explicitly selected
// backend. BackendAuto is intentionally rejected because it is a resolver
// policy rather than a concrete engine.
func ProfileForBackend(backend Backend) (EngineProfile, error) {
	parsed, err := ParseBackend(string(backend))
	if err != nil {
		return EngineProfile{}, err
	}
	if parsed == BackendAuto {
		return EngineProfile{}, fmt.Errorf("hatriecache: storage engine profile requires an explicit backend")
	}
	for _, profile := range engineProfiles {
		if profile.Backend == parsed {
			return profile, nil
		}
	}
	return EngineProfile{}, fmt.Errorf("hatriecache: no storage engine profile for backend %q", parsed)
}
