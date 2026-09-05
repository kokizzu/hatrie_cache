package hatJournal

import (
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

// CompressFile writes a CRC-protected zstd frame from sourcePath to
// destinationPath. Callers can publish the destination atomically after this
// function returns successfully.
func CompressFile(sourcePath string, destinationPath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	encoder, err := zstd.NewWriter(destination, zstd.WithEncoderCRC(true))
	if err != nil {
		_ = destination.Close()
		return err
	}
	if _, err := io.Copy(encoder, source); err != nil {
		_ = encoder.Close()
		_ = destination.Close()
		return err
	}
	if err := encoder.Close(); err != nil {
		_ = destination.Close()
		return err
	}
	if err := destination.Sync(); err != nil {
		_ = destination.Close()
		return err
	}
	return destination.Close()
}
