package lsm

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io/fs"
	"os"
)

// wal – write-ahead log. Kiekvienas įrašas yra atominis rašymų paketas:
// [ilgis u32][crc32 u32][įrašų skaičius u32][įrašai...]
type wal struct {
	path string
	f    *os.File
	sync bool
}

func openWAL(path string, sync bool) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &wal{path: path, f: f, sync: sync}, nil
}

func (w *wal) append(batch []Entry) error {
	payload := binary.BigEndian.AppendUint32(nil, uint32(len(batch)))
	for _, e := range batch {
		payload = appendEntry(payload, e)
	}
	rec := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
	rec = binary.BigEndian.AppendUint32(rec, crc32.ChecksumIEEE(payload))
	if _, err := w.f.Write(append(rec, payload...)); err != nil {
		return err
	}
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

// replayWAL grąžina visus sveikus paketus; nebaigtas paskutinis įrašas ignoruojamas.
func replayWAL(path string) ([][]Entry, error) {
	buf, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var batches [][]Entry
	for len(buf) >= 8 {
		n := binary.BigEndian.Uint32(buf)
		crc := binary.BigEndian.Uint32(buf[4:])
		if uint64(len(buf)-8) < uint64(n) || n < 4 {
			break
		}
		payload := buf[8 : 8+n]
		if crc32.ChecksumIEEE(payload) != crc {
			break
		}
		batch, err := decodeEntries(payload[4:], int(binary.BigEndian.Uint32(payload)))
		if err != nil {
			break
		}
		batches = append(batches, batch)
		buf = buf[8+n:]
	}
	return batches, nil
}

func (w *wal) reset() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.f = f
	return nil
}

func (w *wal) close() error { return w.f.Close() }
