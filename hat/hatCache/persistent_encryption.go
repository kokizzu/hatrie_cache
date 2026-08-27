package hatCache

import (
	"bytes"
	"fmt"
	"io"

	"hatrie_cache/hat/hatCodec"
)

func sealPersistentEntry(cipher *hatCodec.StreamCipher, backend StorageBackend, key string, data []byte) ([]byte, error) {
	if cipher == nil {
		return cloneBytes(data), nil
	}
	var encrypted bytes.Buffer
	writer, err := cipher.NewWriter(&encrypted, persistentEntryAssociatedData(backend, key))
	if err != nil {
		return nil, fmt.Errorf("initialize encrypted persistent entry: %w", err)
	}
	if _, err := writer.Write(data); err != nil {
		return nil, fmt.Errorf("encrypt persistent entry: %w", err)
	}
	return encrypted.Bytes(), nil
}

func openPersistentEntry(cipher *hatCodec.StreamCipher, backend StorageBackend, key string, data []byte) ([]byte, error) {
	if cipher == nil {
		return cloneBytes(data), nil
	}
	reader, err := cipher.NewReader(bytes.NewReader(data), persistentEntryAssociatedData(backend, key))
	if err != nil {
		return nil, fmt.Errorf("initialize encrypted persistent entry reader: %w", err)
	}
	plain, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("decrypt persistent entry: %w", err)
	}
	return plain, nil
}

func persistentEntryAssociatedData(backend StorageBackend, key string) []byte {
	return []byte("hatrie/persistent-entry/" + string(backend) + "/" + key)
}
