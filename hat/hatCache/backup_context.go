package hatCache

import (
	"context"
	"io"
)

func normalizeBackupContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func checkBackupContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

type backupContextReader struct {
	ctx context.Context
	io.Reader
}

func (reader backupContextReader) Read(data []byte) (int, error) {
	if err := checkBackupContext(reader.ctx); err != nil {
		return 0, err
	}
	return reader.Reader.Read(data)
}

type backupContextWriter struct {
	ctx context.Context
	io.Writer
}

func (writer backupContextWriter) Write(data []byte) (int, error) {
	if err := checkBackupContext(writer.ctx); err != nil {
		return 0, err
	}
	n, err := writer.Writer.Write(data)
	if err != nil {
		return n, err
	}
	if contextErr := checkBackupContext(writer.ctx); contextErr != nil {
		return n, contextErr
	}
	return n, nil
}
