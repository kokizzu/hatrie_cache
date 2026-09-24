package hatBackup

import (
	"context"
	"crypto/sha256"
	"io"
)

func (attachment *ReadOnlyBackupAttachment) openChunked(ctx context.Context, file BundleFile) (io.ReadCloser, error) {
	chunks := &readOnlyBackupChunkReader{
		attachment: attachment,
		ctx:        ctx,
		file:       file,
	}
	return &readOnlyBackupFileReader{
		body:           chunks,
		reader:         chunks,
		expectedSize:   file.Size,
		expectedSHA256: file.SHA256,
		digest:         sha256.New(),
	}, nil
}

type readOnlyBackupChunkReader struct {
	attachment *ReadOnlyBackupAttachment
	ctx        context.Context
	file       BundleFile
	next       int
	current    *readOnlyBackupFileReader
	closed     bool
}

func (reader *readOnlyBackupChunkReader) Read(buffer []byte) (int, error) {
	if reader.closed {
		return 0, ErrReadOnlyBackupAttachmentClosed
	}
	for {
		if reader.current == nil {
			if reader.next >= len(reader.file.Chunks) {
				return 0, io.EOF
			}
			chunk := reader.file.Chunks[reader.next]
			payload, body, err := reader.attachment.target.openChunk(reader.ctx, reader.attachment.layout, reader.file, chunk, reader.attachment.manifest)
			if err != nil {
				return 0, err
			}
			reader.current = &readOnlyBackupFileReader{
				body:           body,
				reader:         payload,
				expectedSize:   chunk.Size,
				expectedSHA256: chunk.SHA256,
				digest:         sha256.New(),
			}
			reader.next++
		}
		n, err := reader.current.Read(buffer)
		if err != io.EOF {
			return n, err
		}
		closeErr := reader.current.Close()
		reader.current = nil
		if closeErr != nil {
			return n, closeErr
		}
		if n > 0 {
			return n, nil
		}
	}
}

func (reader *readOnlyBackupChunkReader) Close() error {
	if reader.closed {
		return nil
	}
	reader.closed = true
	if reader.current == nil {
		return nil
	}
	err := reader.current.Close()
	reader.current = nil
	return err
}
