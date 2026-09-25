package hatSchema

import (
	"context"
	"errors"
	"strings"
)

var (
	// ErrTextIndexCatalogNil reports that no catalog implementation was supplied.
	ErrTextIndexCatalogNil = errors.New("hatSchema: text index catalog is nil")
	// ErrTextIndexCatalogSourceNil reports that the source receiver is nil.
	ErrTextIndexCatalogSourceNil = errors.New("hatSchema: materialized source is nil")
	// ErrTextIndexCatalogSourceKeyRequired reports that a durable source identity is missing.
	ErrTextIndexCatalogSourceKeyRequired = errors.New("hatSchema: text index catalog source key is required")
	// ErrTextIndexCatalogFieldRequired reports that the indexed field name is missing.
	ErrTextIndexCatalogFieldRequired = errors.New("hatSchema: text index catalog field is required")
)

// TextIndexCatalog stores validated HTI1 text-index frames outside a source.
//
// The catalog owns durability, retention, replication, and access control. The
// source only supplies a stable source key and field name; it performs no
// implicit I/O and does not retain the catalog implementation.
type TextIndexCatalog interface {
	PutTextIndex(ctx context.Context, sourceKey, field string, frame []byte) error
	GetTextIndex(ctx context.Context, sourceKey, field string) ([]byte, error)
}

func normalizeTextIndexCatalogContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func checkTextIndexCatalogContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func validateTextIndexCatalogRequest(catalog TextIndexCatalog, sourceKey, field string) (string, string, error) {
	if catalog == nil {
		return "", "", ErrTextIndexCatalogNil
	}
	sourceKey = strings.TrimSpace(sourceKey)
	if sourceKey == "" {
		return "", "", ErrTextIndexCatalogSourceKeyRequired
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return "", "", ErrTextIndexCatalogFieldRequired
	}
	return sourceKey, field, nil
}

// PersistTextIndex serializes field's positional text index and writes the
// resulting HTI1 frame to the caller-owned catalog.
func (source *MaterializedSource) PersistTextIndex(ctx context.Context, catalog TextIndexCatalog, sourceKey, field string) error {
	if source == nil {
		return ErrTextIndexCatalogSourceNil
	}
	sourceKey, field, err := validateTextIndexCatalogRequest(catalog, sourceKey, field)
	if err != nil {
		return err
	}
	ctx = normalizeTextIndexCatalogContext(ctx)
	if err := checkTextIndexCatalogContext(ctx); err != nil {
		return err
	}
	frame, err := source.MarshalTextIndex(field)
	if err != nil {
		return err
	}
	if err := checkTextIndexCatalogContext(ctx); err != nil {
		return err
	}
	return catalog.PutTextIndex(ctx, sourceKey, field, frame)
}

// RestoreTextIndexFromCatalog loads an HTI1 frame from the caller-owned
// catalog and restores it into the source's positional text index.
func (source *MaterializedSource) RestoreTextIndexFromCatalog(ctx context.Context, catalog TextIndexCatalog, sourceKey, field string) error {
	if source == nil {
		return ErrTextIndexCatalogSourceNil
	}
	sourceKey, field, err := validateTextIndexCatalogRequest(catalog, sourceKey, field)
	if err != nil {
		return err
	}
	ctx = normalizeTextIndexCatalogContext(ctx)
	if err := checkTextIndexCatalogContext(ctx); err != nil {
		return err
	}
	frame, err := catalog.GetTextIndex(ctx, sourceKey, field)
	if err != nil {
		return err
	}
	if err := checkTextIndexCatalogContext(ctx); err != nil {
		return err
	}
	return source.RestoreTextIndex(field, frame)
}
