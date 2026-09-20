package hatStorage

import (
	"errors"
	"net/url"
	"path/filepath"
	"strings"
)

var ErrRemotePartReferenceInvalid = errors.New("hatriecache: remote part reference is invalid")
var ErrRemotePartColumnReferenceInvalid = errors.New("hatriecache: remote part column reference is invalid")

// RemotePartMetadata is the local manifest information needed to locate and
// verify a remote immutable part.
type RemotePartMetadata struct {
	ObjectURI         string `json:"object_uri"`
	LocalMetadataPath string `json:"local_metadata_path"`
	SizeBytes         uint64 `json:"size_bytes"`
	Checksum          string `json:"checksum"`
}

// RemotePartReference is immutable after construction. It separates the
// remote object identity from a local metadata path and performs no network
// or filesystem I/O.
type RemotePartReference struct {
	metadata RemotePartMetadata
}

// NewRemotePartReference validates and copies a remote object reference. The
// URI may use S3, GCS, Azure, HTTP, or HTTPS addressing and may contain query
// parameters such as a signed-object token.
func NewRemotePartReference(objectURI, localMetadataPath, checksum string, sizeBytes uint64) (RemotePartReference, error) {
	objectURI = strings.TrimSpace(objectURI)
	localMetadataPath = strings.TrimSpace(localMetadataPath)
	checksum = strings.TrimSpace(checksum)
	parsed, err := url.Parse(objectURI)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.Path == "" || strings.Trim(parsed.Path, "/") == "" || parsed.User != nil || parsed.Fragment != "" {
		return RemotePartReference{}, ErrRemotePartReferenceInvalid
	}
	scheme := strings.ToLower(parsed.Scheme)
	switch scheme {
	case "s3", "gs", "az", "http", "https":
	default:
		return RemotePartReference{}, ErrRemotePartReferenceInvalid
	}
	if localMetadataPath == "" || filepath.IsAbs(localMetadataPath) || strings.IndexByte(localMetadataPath, 0) >= 0 || checksum == "" {
		return RemotePartReference{}, ErrRemotePartReferenceInvalid
	}
	cleanMetadataPath := filepath.Clean(localMetadataPath)
	if cleanMetadataPath == "." || cleanMetadataPath == ".." || strings.HasPrefix(cleanMetadataPath, ".."+string(filepath.Separator)) {
		return RemotePartReference{}, ErrRemotePartReferenceInvalid
	}
	parsed.Scheme = scheme
	return RemotePartReference{metadata: RemotePartMetadata{
		ObjectURI:         parsed.String(),
		LocalMetadataPath: cleanMetadataPath,
		SizeBytes:         sizeBytes,
		Checksum:          checksum,
	}}, nil
}

// ObjectURI returns the normalized remote object URI.
func (reference RemotePartReference) ObjectURI() string {
	return reference.metadata.ObjectURI
}

// LocalMetadataPath returns the normalized path relative to a local metadata
// root.
func (reference RemotePartReference) LocalMetadataPath() string {
	return reference.metadata.LocalMetadataPath
}

// SizeBytes returns the declared remote object size.
func (reference RemotePartReference) SizeBytes() uint64 {
	return reference.metadata.SizeBytes
}

// Checksum returns the caller-supplied object checksum.
func (reference RemotePartReference) Checksum() string {
	return reference.metadata.Checksum
}

// Metadata returns an independent value suitable for local manifest storage.
func (reference RemotePartReference) Metadata() RemotePartMetadata {
	return reference.metadata
}

// ResolveMetadataPath joins the relative metadata path to root and rejects
// traversal outside that root. It does not follow symlinks; callers should
// apply their filesystem trust policy before opening the result.
func (reference RemotePartReference) ResolveMetadataPath(root string) (string, error) {
	if strings.TrimSpace(root) == "" || reference.metadata.LocalMetadataPath == "" {
		return "", ErrRemotePartReferenceInvalid
	}
	rootPath, err := filepath.Abs(root)
	if err != nil {
		return "", ErrRemotePartReferenceInvalid
	}
	rootPath = filepath.Clean(rootPath)
	metadataPath, err := filepath.Abs(filepath.Join(rootPath, reference.metadata.LocalMetadataPath))
	if err != nil {
		return "", ErrRemotePartReferenceInvalid
	}
	if metadataPath != rootPath && !strings.HasPrefix(metadataPath, rootPath+string(filepath.Separator)) {
		return "", ErrRemotePartReferenceInvalid
	}
	return metadataPath, nil
}

// RemotePartColumnReference identifies one immutable byte range within a
// remote part. The caller obtains the range from trusted part metadata and
// the loader may use it for a range request instead of downloading the whole
// part.
type RemotePartColumnReference struct {
	part        RemotePartReference
	name        string
	checksum    string
	offsetBytes uint64
	sizeBytes   uint64
}

// NewRemotePartColumnReference validates one projected column/range identity.
// A zero size means that the loader size is not declared; a non-zero size is
// checked against the loader result by RemotePartCache.
func NewRemotePartColumnReference(part RemotePartReference, name, checksum string, offsetBytes, sizeBytes uint64) (RemotePartColumnReference, error) {
	if part.ObjectURI() == "" || part.LocalMetadataPath() == "" || part.Checksum() == "" {
		return RemotePartColumnReference{}, ErrRemotePartColumnReferenceInvalid
	}
	name = strings.TrimSpace(name)
	checksum = strings.TrimSpace(checksum)
	if name == "" || checksum == "" || strings.IndexByte(name, 0) >= 0 || strings.IndexByte(checksum, 0) >= 0 || sizeBytes > ^uint64(0)-offsetBytes {
		return RemotePartColumnReference{}, ErrRemotePartColumnReferenceInvalid
	}
	return RemotePartColumnReference{
		part:        part,
		name:        name,
		checksum:    checksum,
		offsetBytes: offsetBytes,
		sizeBytes:   sizeBytes,
	}, nil
}

// Part returns the immutable parent part reference.
func (reference RemotePartColumnReference) Part() RemotePartReference {
	return reference.part
}

// ColumnName returns the logical column name.
func (reference RemotePartColumnReference) ColumnName() string {
	return reference.name
}

// OffsetBytes returns the inclusive byte offset within the remote part.
func (reference RemotePartColumnReference) OffsetBytes() uint64 {
	return reference.offsetBytes
}

// SizeBytes returns the declared projected range size.
func (reference RemotePartColumnReference) SizeBytes() uint64 {
	return reference.sizeBytes
}

// Checksum returns the checksum for the projected range.
func (reference RemotePartColumnReference) Checksum() string {
	return reference.checksum
}
