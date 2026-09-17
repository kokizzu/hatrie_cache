package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

const maxCatalogSourceStatusErrorCodeBytes = 64

const (
	CatalogSourceStateUnknown    = "unknown"
	CatalogSourceStateStarting   = "starting"
	CatalogSourceStateRunning    = "running"
	CatalogSourceStateReady      = "ready"
	CatalogSourceStateCatchingUp = "catching_up"
	CatalogSourceStateStopped    = "stopped"
	CatalogSourceStateFailed     = "failed"
	CatalogSourceStateDegraded   = "degraded"
)

var ErrCatalogSourceStatusInvalid = errors.New("catalog source status is invalid")

type CatalogSourceStatus struct {
	Namespace string
	Source    string
	Kind      string
	State     string
	Available bool
	Ready     bool
	Frontier  uint64
	Observed  uint64
	Lag       uint64
	ErrorCode string
}

type CatalogSourceStatusResolver interface {
	ResolveSQLSourceStatuses() ([]CatalogSourceStatus, error)
}

type CatalogSourceStatusResolverFunc func() ([]CatalogSourceStatus, error)

func (fn CatalogSourceStatusResolverFunc) ResolveSQLSourceStatuses() ([]CatalogSourceStatus, error) {
	if fn == nil {
		return nil, nil
	}
	return fn()
}

type catalogSourceStatusKey struct {
	namespace string
	source    string
}

func catalogSourceStatusRows(resolver CatalogResolver) ([]Row, error) {
	statuses, err := resolver.catalogSourceStatuses()
	if err != nil {
		return nil, err
	}
	version := catalogVersion(resolver.Catalog)
	rows := make([]Row, len(statuses))
	for index, status := range statuses {
		rows[index] = Row{
			"catalog_version": uint64(version),
			"namespace":       status.Namespace,
			"source":          status.Source,
			"kind":            status.Kind,
			"state":           status.State,
			"available":       status.Available,
			"ready":           status.Ready,
			"frontier":        uint64(status.Frontier),
			"observed":        uint64(status.Observed),
			"lag":             uint64(status.Lag),
			"error_code":      status.ErrorCode,
		}
	}
	return rows, nil
}

func (resolver CatalogResolver) catalogSourceStatuses() ([]CatalogSourceStatus, error) {
	statuses := make(map[catalogSourceStatusKey]CatalogSourceStatus, len(resolver.Catalog.Sources)+len(resolver.Catalog.SourceStatuses))
	sourceKinds := make(map[catalogSourceStatusKey]string, len(resolver.Catalog.Sources))
	for _, source := range resolver.Catalog.Sources {
		normalizedSource := strings.TrimSpace(source.Name)
		if normalizedSource == "" {
			continue
		}
		key := catalogSourceStatusKey{namespace: strings.TrimSpace(source.Namespace), source: normalizedSource}
		sourceKinds[key] = strings.TrimSpace(source.Kind)
		statuses[key] = CatalogSourceStatus{
			Namespace: key.namespace,
			Source:    key.source,
			Kind:      sourceKinds[key],
			State:     CatalogSourceStateUnknown,
		}
	}
	explicit := make(map[catalogSourceStatusKey]struct{}, len(resolver.Catalog.SourceStatuses))
	apply := func(status CatalogSourceStatus) error {
		key := catalogSourceStatusKey{namespace: strings.TrimSpace(status.Namespace), source: strings.TrimSpace(status.Source)}
		normalized, err := normalizeCatalogSourceStatus(status, sourceKinds[key])
		if err != nil {
			return err
		}
		statuses[key] = normalized
		explicit[key] = struct{}{}
		return nil
	}
	for _, status := range resolver.Catalog.SourceStatuses {
		if err := apply(status); err != nil {
			return nil, err
		}
	}
	if resolver.SourceStatus != nil {
		snapshot, err := resolver.SourceStatus.ResolveSQLSourceStatuses()
		if err != nil {
			return nil, err
		}
		for _, status := range snapshot {
			if err := apply(status); err != nil {
				return nil, err
			}
		}
	}
	if frontierResolver, ok := resolver.Source.(SQLSourceFrontierResolver); ok {
		for _, source := range resolver.Catalog.Sources {
			key := catalogSourceStatusKey{namespace: strings.TrimSpace(source.Namespace), source: strings.TrimSpace(source.Name)}
			if key.source == "" {
				continue
			}
			if _, exists := explicit[key]; exists {
				continue
			}
			status, exists := statuses[key]
			if !exists {
				continue
			}
			frontier, ready, available, err := frontierResolver.SQLSourceFrontier(source.Kind, source.Name)
			if err != nil {
				return nil, err
			}
			if !available {
				continue
			}
			status.Frontier = frontier
			status.Available = true
			status.Ready = ready
			if ready {
				status.State = CatalogSourceStateReady
			} else {
				status.State = CatalogSourceStateCatchingUp
			}
			statuses[key] = status
		}
	}
	result := make([]CatalogSourceStatus, 0, len(statuses))
	for _, status := range statuses {
		result = append(result, status)
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Namespace != result[right].Namespace {
			return result[left].Namespace < result[right].Namespace
		}
		if result[left].Source != result[right].Source {
			return result[left].Source < result[right].Source
		}
		return result[left].Kind < result[right].Kind
	})
	return result, nil
}

func normalizeCatalogSourceStatus(status CatalogSourceStatus, sourceKind string) (CatalogSourceStatus, error) {
	status.Namespace = strings.TrimSpace(status.Namespace)
	status.Source = strings.TrimSpace(status.Source)
	status.Kind = strings.TrimSpace(status.Kind)
	status.State = strings.ToLower(strings.TrimSpace(status.State))
	status.ErrorCode = strings.ToLower(strings.TrimSpace(status.ErrorCode))
	if status.Source == "" {
		return CatalogSourceStatus{}, fmt.Errorf("%w: source is required", ErrCatalogSourceStatusInvalid)
	}
	if status.Kind == "" {
		status.Kind = sourceKind
	}
	if status.Kind == "" {
		status.Kind = "unknown"
	}
	if status.State == "" {
		status.State = CatalogSourceStateUnknown
	}
	switch status.State {
	case CatalogSourceStateUnknown, CatalogSourceStateStarting, CatalogSourceStateRunning, CatalogSourceStateReady, CatalogSourceStateCatchingUp, CatalogSourceStateStopped, CatalogSourceStateFailed, CatalogSourceStateDegraded:
	default:
		return CatalogSourceStatus{}, fmt.Errorf("%w: unsupported state", ErrCatalogSourceStatusInvalid)
	}
	if len(status.ErrorCode) > maxCatalogSourceStatusErrorCodeBytes || !catalogSourceStatusIdentifier(status.ErrorCode) {
		return CatalogSourceStatus{}, fmt.Errorf("%w: error code must be an identifier of at most %d bytes", ErrCatalogSourceStatusInvalid, maxCatalogSourceStatusErrorCodeBytes)
	}
	if status.State == CatalogSourceStateReady {
		status.Available = true
		status.Ready = true
	}
	if status.Ready {
		status.Available = true
	}
	if status.Observed > status.Frontier {
		expectedLag := status.Observed - status.Frontier
		if status.Lag == 0 {
			status.Lag = expectedLag
		} else if status.Lag != expectedLag {
			return CatalogSourceStatus{}, fmt.Errorf("%w: lag does not match observed and frontier", ErrCatalogSourceStatusInvalid)
		}
	} else if status.Lag != 0 {
		return CatalogSourceStatus{}, fmt.Errorf("%w: lag must be zero when observed is not ahead", ErrCatalogSourceStatusInvalid)
	}
	return status, nil
}

func catalogSourceStatusIdentifier(value string) bool {
	if value == "" {
		return true
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_' || character == '-' || character == '.') {
			return false
		}
	}
	return true
}

func (resolver CatalogResolver) SQLSourceFrontier(name, key string) (uint64, bool, bool, error) {
	if catalogOwnsVirtualSource(name, key) || resolver.Source == nil {
		return 0, false, false, nil
	}
	frontier, ok := resolver.Source.(SQLSourceFrontierResolver)
	if !ok {
		return 0, false, false, nil
	}
	return frontier.SQLSourceFrontier(name, key)
}
