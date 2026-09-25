package hatStorage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrStorageTierReaderInvalid            = errors.New("hatriecache: storage tier reader input is invalid")
	ErrStorageTierReaderContextRequired    = errors.New("hatriecache: storage tier reader context is required")
	ErrStorageTierReaderLocalRootNeeded    = errors.New("hatriecache: storage tier reader local root is required")
	ErrStorageTierReaderRemoteCacheNeeded  = errors.New("hatriecache: storage tier reader remote cache is required")
	ErrStorageTierReaderRemoteLoaderNeeded = errors.New("hatriecache: storage tier reader remote loader is required")
)

// StorageTierReadLocation identifies where a published immutable part is read.
// The zero value is the local hot tier; object storage is opt-in.
type StorageTierReadLocation uint8

const (
	StorageTierLocal StorageTierReadLocation = iota
	StorageTierObject
)

// StorageTierReadPart is the immutable metadata published for one readable
// part. LocalPath is relative to StorageTierReaderOptions.LocalRoot. Remote is
// used only for StorageTierObject and is loaded through the bounded cache.
type StorageTierReadPart struct {
	Key       string
	Tier      StorageTierReadLocation
	LocalPath string
	Remote    RemotePartReference
	Priority  int
}

// StorageTierReaderOptions configures the opt-in transparent part reader.
// LocalRoot is required only when local parts will be read. RemoteCache and
// RemoteLoader must be supplied together when object parts will be read.
type StorageTierReaderOptions struct {
	LocalRoot    string
	RemoteCache  *RemotePartCache
	RemoteLoader RemotePartCacheLoader
}

// StorageTierReader reads either a local hot part or an object-storage part
// through the existing bounded remote-part cache. Returned object-tier bytes
// are immutable cache bytes and must not be modified by the caller.
type StorageTierReader struct {
	localRoot    string
	localPrefix  string
	remoteCache  *RemotePartCache
	remoteLoader RemotePartCacheLoader
}

// NewStorageTierReader validates and copies an opt-in transparent reader
// configuration. It performs no I/O and does not enable any global behavior.
func NewStorageTierReader(options StorageTierReaderOptions) (*StorageTierReader, error) {
	localRoot := strings.TrimSpace(options.LocalRoot)
	if localRoot != "" {
		absolute, err := filepath.Abs(localRoot)
		if err != nil {
			return nil, fmt.Errorf("%w: local root: %v", ErrStorageTierReaderInvalid, err)
		}
		localRoot = filepath.Clean(absolute)
	}
	if (options.RemoteCache == nil) != (options.RemoteLoader == nil) {
		if options.RemoteCache == nil {
			return nil, ErrStorageTierReaderRemoteCacheNeeded
		}
		return nil, ErrStorageTierReaderRemoteLoaderNeeded
	}
	if localRoot == "" && options.RemoteCache == nil {
		return nil, ErrStorageTierReaderLocalRootNeeded
	}
	return &StorageTierReader{
		localRoot:    localRoot,
		localPrefix:  localRoot + string(filepath.Separator),
		remoteCache:  options.RemoteCache,
		remoteLoader: options.RemoteLoader,
	}, nil
}

// Read returns the bytes for one published part. It keeps the caller-facing
// read operation identical across local and object tiers; tier movement and
// publication remain caller-owned.
func (reader *StorageTierReader) Read(ctx context.Context, part StorageTierReadPart) ([]byte, error) {
	if reader == nil {
		return nil, ErrStorageTierReaderInvalid
	}
	if ctx == nil {
		return nil, ErrStorageTierReaderContextRequired
	}
	if part.Key == "" {
		return nil, fmt.Errorf("%w: part key is required", ErrStorageTierReaderInvalid)
	}
	switch part.Tier {
	case StorageTierLocal:
		return reader.readLocal(part)
	case StorageTierObject:
		if reader.remoteCache == nil {
			return nil, ErrStorageTierReaderRemoteCacheNeeded
		}
		if reader.remoteLoader == nil {
			return nil, ErrStorageTierReaderRemoteLoaderNeeded
		}
		metadata := part.Remote.Metadata()
		if metadata.ObjectURI == "" || metadata.LocalMetadataPath == "" {
			return nil, fmt.Errorf("%w: remote reference is required", ErrStorageTierReaderInvalid)
		}
		return reader.remoteCache.Get(ctx, part.Remote, part.Priority, reader.remoteLoader)
	default:
		return nil, fmt.Errorf("%w: unknown tier %d", ErrStorageTierReaderInvalid, part.Tier)
	}
}

func (reader *StorageTierReader) readLocal(part StorageTierReadPart) ([]byte, error) {
	path, err := reader.resolveLocalPath(part.LocalPath)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

func (reader *StorageTierReader) resolveLocalPath(relative string) (string, error) {
	if reader.localRoot == "" {
		return "", ErrStorageTierReaderLocalRootNeeded
	}
	relative = strings.TrimSpace(relative)
	if relative == "" || filepath.IsAbs(relative) || strings.IndexByte(relative, 0) >= 0 {
		return "", fmt.Errorf("%w: local path must be relative", ErrStorageTierReaderInvalid)
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: local path escapes root", ErrStorageTierReaderInvalid)
	}
	path := reader.localPrefix + clean
	if path != reader.localRoot && !strings.HasPrefix(path, reader.localPrefix) {
		return "", fmt.Errorf("%w: local path escapes root", ErrStorageTierReaderInvalid)
	}
	return path, nil
}
