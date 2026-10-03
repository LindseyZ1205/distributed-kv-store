package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const snapshotName = "snapshot.bin"

// SaveSnapshot atomically replaces the snapshot in dir with data, the state
// machine as of log entry (index, term). The file is
//
//	| crc32c uint32 | uvarint index | uvarint term | data |
//
// written to a temporary file, fsynced, and renamed into place.
func SaveSnapshot(dir string, index, term int, data []byte) error {
	body := binary.AppendUvarint(nil, uint64(index))
	body = binary.AppendUvarint(body, uint64(term))
	body = append(body, data...)
	buf := make([]byte, 4, 4+len(body))
	binary.LittleEndian.PutUint32(buf, crc32.Checksum(body, crcTable))
	buf = append(buf, body...)

	tmp := filepath.Join(dir, snapshotName+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, snapshotName)); err != nil {
		return err
	}
	return syncDir(dir)
}

// LoadSnapshot reads the snapshot in dir. It returns index 0 and nil data
// if there is none. A snapshot that fails its checksum is an error: the
// log entries it covered are gone, so there is nothing to fall back to.
func LoadSnapshot(dir string) (index, term int, data []byte, err error) {
	buf, err := os.ReadFile(filepath.Join(dir, snapshotName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil, nil
	}
	if err != nil {
		return 0, 0, nil, err
	}
	if len(buf) < 4 {
		return 0, 0, nil, errors.New("wal: snapshot too short")
	}
	body := buf[4:]
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(buf[:4]) {
		return 0, 0, nil, errors.New("wal: snapshot checksum mismatch")
	}
	i, body, err := uvarint(body)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("wal: snapshot header: %w", err)
	}
	t, body, err := uvarint(body)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("wal: snapshot header: %w", err)
	}
	return int(i), int(t), body, nil
}
