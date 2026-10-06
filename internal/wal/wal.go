// Package wal implements the write-ahead log that makes Raft state durable.
//
// The log is one append-only file of records, each framed as
//
//	| length uint32 | crc32c uint32 | type byte | body |
//
// where length counts the type byte and the body, and the CRC covers the
// same bytes. There are three record types:
//
//	hardState  the current term and vote; the last one on disk wins
//	entry      one Raft log entry with its index; an entry whose index is
//	           already present replaces that entry and everything after it,
//	           which is how a follower overwrites a conflicting suffix
//	           without a separate truncation record
//	snapshot   says entries up to (index, term) are covered by the snapshot
//	           file; Compact writes it as the first record of a new log
//
// Writes are buffered and Sync flushes and fsyncs them, so the caller
// decides which operations must be durable before it acknowledges them.
// A crash can tear the last record; Open drops a torn or corrupt tail and
// keeps everything before it.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	logName = "wal.log"

	recHardState byte = 1
	recEntry     byte = 2
	recSnapshot  byte = 3

	headerSize = 8
	maxRecord  = 256 << 20
	bufferSize = 64 << 10
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrClosed is returned by every method after Close.
var ErrClosed = errors.New("wal: closed")

// HardState is the Raft state other than the log that must survive a
// restart.
type HardState struct {
	Term int
	Vote int
}

// Entry is one Raft log entry.
type Entry struct {
	Index int
	Term  int
	Data  []byte
}

// State is what Open recovers from disk.
type State struct {
	HardState HardState
	// HasHardState reports whether any hardState record was found.
	HasHardState bool
	// SnapIndex and SnapTerm identify the last entry covered by the most
	// recent compaction. Entries holds the entries after it, in order.
	SnapIndex int
	SnapTerm  int
	Entries   []Entry
}

// WAL is a write-ahead log. It is safe for concurrent use.
type WAL struct {
	dir string

	// syncMu serializes Sync with Compact, so an fsync never runs on a
	// file that Compact is about to replace.
	syncMu sync.Mutex

	mu     sync.Mutex // guards the fields below
	f      *os.File
	bw     *bufio.Writer
	closed bool
}

// Open opens the log in dir, creating dir and the log if needed, and
// returns the state it holds.
func Open(dir string) (*WAL, *State, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, logName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	st, good, err := replay(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	// Cut off a torn tail so that new records follow the last good one.
	if err := f.Truncate(good); err != nil {
		f.Close()
		return nil, nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, nil, err
	}
	return &WAL{dir: dir, f: f, bw: bufio.NewWriterSize(f, bufferSize)}, st, nil
}

// replay reads records until the end of the file or the first record that
// is torn or fails its checksum. It returns the recovered state and the
// offset just past the last good record.
func replay(r io.Reader) (*State, int64, error) {
	st := &State{}
	br := bufio.NewReaderSize(r, bufferSize)
	hdr := make([]byte, headerSize)
	var off int64
	for {
		if _, err := io.ReadFull(br, hdr); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return st, off, nil
			}
			return nil, 0, err
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		if n == 0 || n > maxRecord {
			return st, off, nil
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(br, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return st, off, nil
			}
			return nil, 0, err
		}
		if crc32.Checksum(payload, crcTable) != sum {
			return st, off, nil
		}
		if err := st.apply(payload); err != nil {
			return nil, 0, fmt.Errorf("wal: record at offset %d: %w", off, err)
		}
		off += headerSize + int64(n)
	}
}

func (st *State) apply(p []byte) error {
	typ, body := p[0], p[1:]
	switch typ {
	case recHardState:
		term, body, err := uvarint(body)
		if err != nil {
			return err
		}
		vote, n := binary.Varint(body)
		if n <= 0 {
			return errors.New("bad vote")
		}
		st.HardState = HardState{Term: int(term), Vote: int(vote)}
		st.HasHardState = true

	case recEntry:
		index, body, err := uvarint(body)
		if err != nil {
			return err
		}
		term, body, err := uvarint(body)
		if err != nil {
			return err
		}
		size, body, err := uvarint(body)
		if err != nil {
			return err
		}
		if uint64(len(body)) != size {
			return errors.New("bad entry length")
		}
		next := st.SnapIndex + len(st.Entries) + 1
		i := int(index)
		if i <= st.SnapIndex || i > next {
			return fmt.Errorf("entry %d does not follow %d", i, next-1)
		}
		// Overwrite from i onward, as a follower does on a conflict.
		st.Entries = append(st.Entries[:i-st.SnapIndex-1], Entry{Index: i, Term: int(term), Data: body})

	case recSnapshot:
		index, body, err := uvarint(body)
		if err != nil {
			return err
		}
		term, _, err := uvarint(body)
		if err != nil {
			return err
		}
		i := int(index)
		if covered := i - st.SnapIndex; covered >= len(st.Entries) {
			st.Entries = nil
		} else if covered > 0 {
			st.Entries = append([]Entry(nil), st.Entries[covered:]...)
		}
		st.SnapIndex, st.SnapTerm = i, int(term)

	default:
		return fmt.Errorf("unknown record type %d", typ)
	}
	return nil
}

func uvarint(b []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, nil, errors.New("bad varint")
	}
	return v, b[n:], nil
}

// SaveHardState appends a hardState record. It is not durable until Sync.
func (w *WAL) SaveHardState(hs HardState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return writeHardState(w.bw, hs)
}

// Append appends entries. An entry at an index already in the log replaces
// it and every entry after it. Entries are not durable until Sync.
func (w *WAL) Append(entries []Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return writeEntries(w.bw, entries)
}

// Sync makes every record written so far durable. Writes may continue
// while the fsync is in progress; they are covered by a later Sync.
func (w *WAL) Sync() error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	err := w.bw.Flush()
	f := w.f
	w.mu.Unlock()
	if err != nil {
		return err
	}
	return f.Sync()
}

// Compact replaces the log with one that holds only a snapshot record for
// (snapIndex, snapTerm), hs, and entries, which must start at snapIndex+1.
// The new log is written to a temporary file, fsynced and renamed into
// place, so a crash leaves either the old log or the complete new one.
func (w *WAL) Compact(snapIndex, snapTerm int, hs HardState, entries []Entry) error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}

	tmp := filepath.Join(w.dir, logName+".tmp")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, bufferSize)
	body := binary.AppendUvarint(nil, uint64(snapIndex))
	body = binary.AppendUvarint(body, uint64(snapTerm))
	err = writeRecord(bw, recSnapshot, body)
	if err == nil {
		err = writeHardState(bw, hs)
	}
	if err == nil {
		err = writeEntries(bw, entries)
	}
	if err == nil {
		err = bw.Flush()
	}
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		err = os.Rename(tmp, filepath.Join(w.dir, logName))
	}
	if err == nil {
		err = syncDir(w.dir)
	}
	if err != nil {
		f.Close()
		return err
	}
	// Records still buffered for the old file are dropped on purpose: the
	// new file already holds the state they described.
	w.f.Close()
	w.f, w.bw = f, bw
	return nil
}

// Close flushes and fsyncs the log and closes it.
func (w *WAL) Close() error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	err := w.bw.Flush()
	if serr := w.f.Sync(); err == nil {
		err = serr
	}
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	return err
}

func writeHardState(bw *bufio.Writer, hs HardState) error {
	body := binary.AppendUvarint(nil, uint64(hs.Term))
	body = binary.AppendVarint(body, int64(hs.Vote))
	return writeRecord(bw, recHardState, body)
}

func writeEntries(bw *bufio.Writer, entries []Entry) error {
	var body []byte
	for _, e := range entries {
		body = binary.AppendUvarint(body[:0], uint64(e.Index))
		body = binary.AppendUvarint(body, uint64(e.Term))
		body = binary.AppendUvarint(body, uint64(len(e.Data)))
		body = append(body, e.Data...)
		if err := writeRecord(bw, recEntry, body); err != nil {
			return err
		}
	}
	return nil
}

func writeRecord(bw *bufio.Writer, typ byte, body []byte) error {
	var hdr [headerSize]byte
	sum := crc32.Update(crc32.Update(0, crcTable, []byte{typ}), crcTable, body)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(1+len(body)))
	binary.LittleEndian.PutUint32(hdr[4:8], sum)
	if _, err := bw.Write(hdr[:]); err != nil {
		return err
	}
	if err := bw.WriteByte(typ); err != nil {
		return err
	}
	_, err := bw.Write(body)
	return err
}

// syncDir fsyncs a directory so that a rename inside it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
