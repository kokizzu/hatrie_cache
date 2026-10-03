package hatSchema

import (
	"fmt"
	"strings"

	"hatrie_cache/hat/hatSql"
)

// NewRTreeSpatialSourceFromSpaceDefinition builds a SQL spatial source from
// a named-space definition. The selected R-tree index must declare latitude
// then longitude columns in that order; callers still own row writes, while
// the source keeps the tree and SQL resolver in sync.
func NewRTreeSpatialSourceFromSpaceDefinition(definition SpaceDefinition, options hatSql.RTreeSpatialSourceOptions) (*hatSql.RTreeSpatialSource, error) {
	catalog, err := NewSpaceCatalog([]SpaceDefinition{definition})
	if err != nil {
		return nil, fmt.Errorf("validate spatial space definition: %w", err)
	}
	return catalog.NewRTreeSpatialSource(definition.Name, options)
}

// NewRTreeSpatialSource builds a SQL spatial source from one validated
// catalog entry without revalidating or cloning the entire catalog.
func (catalog *SpaceCatalog) NewRTreeSpatialSource(name string, options hatSql.RTreeSpatialSourceOptions) (*hatSql.RTreeSpatialSource, error) {
	if catalog == nil {
		return nil, ErrSpaceCatalogNil
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, ErrSpaceCatalogNameRequired
	}
	catalog.mu.RLock()
	normalized, ok := catalog.spaces[name]
	catalog.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("spatial space definition %q was not found", name)
	}
	indexName := strings.TrimSpace(options.RTreeIndexName)
	var selected *IndexDefinition
	for index := range normalized.Indexes {
		declared := &normalized.Indexes[index]
		if declared.Kind != IndexKindRTree {
			continue
		}
		if indexName != "" && declared.Name != indexName {
			continue
		}
		if selected != nil {
			return nil, fmt.Errorf("spatial space %q has multiple R-tree indexes; set RTreeIndexName", normalized.Name)
		}
		selected = declared
	}
	if selected == nil {
		if indexName == "" {
			return nil, fmt.Errorf("spatial space %q has no R-tree index", normalized.Name)
		}
		return nil, fmt.Errorf("spatial space %q has no R-tree index %q", normalized.Name, indexName)
	}
	if len(selected.Columns) != 2 {
		return nil, fmt.Errorf("spatial space %q R-tree index %q must declare latitude and longitude columns", normalized.Name, selected.Name)
	}
	if options.Name == "" {
		options.Name = normalized.Name
	} else if strings.TrimSpace(options.Name) != normalized.Name {
		return nil, fmt.Errorf("spatial source name %q does not match space %q", options.Name, normalized.Name)
	}
	if options.LatitudeField == "" {
		options.LatitudeField = selected.Columns[0]
	} else if strings.TrimSpace(options.LatitudeField) != selected.Columns[0] {
		return nil, fmt.Errorf("spatial latitude field %q does not match R-tree index column %q", options.LatitudeField, selected.Columns[0])
	}
	if options.LongitudeField == "" {
		options.LongitudeField = selected.Columns[1]
	} else if strings.TrimSpace(options.LongitudeField) != selected.Columns[1] {
		return nil, fmt.Errorf("spatial longitude field %q does not match R-tree index column %q", options.LongitudeField, selected.Columns[1])
	}
	return hatSql.NewRTreeSpatialSource(options)
}
