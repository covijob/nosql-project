package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
)

// SSTable failo formatas:
//
//	[įrašai][retas indeksas – kas indexInterval-as raktas][Bloom filtras][footer]
//
// Įrašas: rakto ilgis u32, reikšmės ilgis i32 (-1 = antkapis), raktas, reikšmė.
const (
	indexInterval = 16
	footerSize    = 8 + 4 + 8 + 4 + 4 + 8 + 8
	bloomK        = 7
)

var sstMagic = []byte("KVDBSST1")

type bloom struct {
	bits []byte
	k    uint32
}

func newBloom(n int) *bloom {
	nbits := max(64, n*10)
	return &bloom{bits: make([]byte, (nbits+7)/8), k: bloomK}
}

func (b *bloom) positions(key []byte, f func(uint64)) {
	h1, h2 := fnv.New64a(), fnv.New64()
	h1.Write(key)
	h2.Write(key)
	x1, x2 := h1.Sum64(), h2.Sum64()|1
	nbits := uint64(len(b.bits)) * 8
	for i := uint64(0); i < uint64(b.k); i++ {
		f((x1 + i*x2) % nbits)
	}
}

func (b *bloom) add(key []byte) {
	b.positions(key, func(p uint64) { b.bits[p>>3] |= 1 << (p & 7) })
}

func (b *bloom) mightContain(key []byte) bool {
	ok := true
	b.positions(key, func(p uint64) { ok = ok && b.bits[p>>3]&(1<<(p&7)) != 0 })
	return ok
}

func appendEntry(buf []byte, e Entry) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.Key)))
	if e.Deleted {
		buf = binary.BigEndian.AppendUint32(buf, ^uint32(0)) // -1
		return append(buf, e.Key...)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.Value)))
	buf = append(buf, e.Key...)
	return append(buf, e.Value...)
}

func decodeEntries(buf []byte, count int) ([]Entry, error) {
	var out []Entry
	for len(buf) > 0 && (count < 0 || len(out) < count) {
		if len(buf) < 8 {
			return nil, errors.New("nepilnas įrašas")
		}
		klen := int(binary.BigEndian.Uint32(buf))
		vlen := int32(binary.BigEndian.Uint32(buf[4:]))
		buf = buf[8:]
		if len(buf) < klen {
			return nil, errors.New("nepilnas raktas")
		}
		e := Entry{Key: buf[:klen:klen]}
		buf = buf[klen:]
		if vlen < 0 {
			e.Deleted = true
		} else {
			if len(buf) < int(vlen) {
				return nil, errors.New("nepilna reikšmė")
			}
			e.Value = buf[:vlen:vlen]
			buf = buf[vlen:]
		}
		out = append(out, e)
	}
	return out, nil
}

type sstable struct {
	path    string
	f       *os.File
	keys    [][]byte
	offsets []uint64
	dataEnd uint64
	bloom   *bloom
	count   uint64
}

func writeSSTable(path string, entries []Entry) (*sstable, error) {
	bl := newBloom(len(entries))
	var data, index []byte
	for i, e := range entries {
		if i%indexInterval == 0 {
			index = binary.BigEndian.AppendUint32(index, uint32(len(e.Key)))
			index = binary.BigEndian.AppendUint64(index, uint64(len(data)))
			index = append(index, e.Key...)
		}
		data = appendEntry(data, e)
		bl.add(e.Key)
	}
	indexCount := (len(entries) + indexInterval - 1) / indexInterval
	buf := append(data, index...)
	bloomOff := len(buf)
	buf = append(buf, bl.bits...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(data)))
	buf = binary.BigEndian.AppendUint32(buf, uint32(indexCount))
	buf = binary.BigEndian.AppendUint64(buf, uint64(bloomOff))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(bl.bits)))
	buf = binary.BigEndian.AppendUint32(buf, bl.k)
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(entries)))
	buf = append(buf, sstMagic...)
	if err := writeFileSync(path, buf); err != nil {
		return nil, err
	}
	return openSSTable(path)
}

// writeFileSync atomiškai (per laikiną failą) įrašo turinį ir jį fsync'ina.
func writeFileSync(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func openSSTable(path string) (*sstable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	t, err := loadSSTable(path, f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("SSTable %s: %w", path, err)
	}
	return t, nil
}

func loadSSTable(path string, f *os.File) (*sstable, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < footerSize {
		return nil, errors.New("per trumpas failas")
	}
	footer := make([]byte, footerSize)
	if _, err := f.ReadAt(footer, st.Size()-footerSize); err != nil {
		return nil, err
	}
	if !bytes.Equal(footer[footerSize-8:], sstMagic) {
		return nil, errors.New("neteisingas magic")
	}
	be := binary.BigEndian
	indexOff := be.Uint64(footer)
	indexCount := int(be.Uint32(footer[8:]))
	bloomOff := be.Uint64(footer[12:])
	bloomLen := be.Uint32(footer[20:])
	t := &sstable{path: path, f: f, dataEnd: indexOff, count: be.Uint64(footer[28:])}
	t.bloom = &bloom{bits: make([]byte, bloomLen), k: be.Uint32(footer[24:])}
	raw := make([]byte, bloomOff-indexOff)
	if _, err := f.ReadAt(raw, int64(indexOff)); err != nil {
		return nil, err
	}
	if _, err := f.ReadAt(t.bloom.bits, int64(bloomOff)); err != nil && err != io.EOF {
		return nil, err
	}
	for range indexCount {
		klen := be.Uint32(raw)
		t.offsets = append(t.offsets, be.Uint64(raw[4:]))
		t.keys = append(t.keys, raw[12:12+klen])
		raw = raw[12+klen:]
	}
	return t, nil
}

func (t *sstable) readBlock(i int) ([]Entry, error) {
	end := t.dataEnd
	if i+1 < len(t.offsets) {
		end = t.offsets[i+1]
	}
	buf := make([]byte, end-t.offsets[i])
	if _, err := t.f.ReadAt(buf, int64(t.offsets[i])); err != nil {
		return nil, err
	}
	return decodeEntries(buf, -1)
}

// blockFor grąžina bloko, kuriame gali būti raktas, numerį (-1, jei raktas mažesnis už visus).
func (t *sstable) blockFor(key []byte) int {
	return sort.Search(len(t.keys), func(i int) bool { return bytes.Compare(t.keys[i], key) > 0 }) - 1
}

func (t *sstable) get(key []byte) (Entry, bool, error) {
	if !t.bloom.mightContain(key) {
		return Entry{}, false, nil
	}
	i := t.blockFor(key)
	if i < 0 {
		return Entry{}, false, nil
	}
	block, err := t.readBlock(i)
	if err != nil {
		return Entry{}, false, err
	}
	for _, e := range block {
		switch bytes.Compare(e.Key, key) {
		case 0:
			return e, true, nil
		case 1:
			return Entry{}, false, nil
		}
	}
	return Entry{}, false, nil
}

func (t *sstable) iterFrom(start []byte) *sstIter {
	return &sstIter{t: t, start: start, block: max(0, t.blockFor(start))}
}

func (t *sstable) close() error { return t.f.Close() }

// sstIter skaito SSTable bloką po bloko nuo nurodyto rakto.
type sstIter struct {
	t     *sstable
	start []byte
	block int
	buf   []Entry
	err   error
}

func (it *sstIter) next() (Entry, bool) {
	for {
		for len(it.buf) > 0 {
			e := it.buf[0]
			it.buf = it.buf[1:]
			if bytes.Compare(e.Key, it.start) >= 0 {
				return e, true
			}
		}
		if it.err != nil || it.block >= len(it.t.offsets) {
			return Entry{}, false
		}
		it.buf, it.err = it.t.readBlock(it.block)
		it.block++
	}
}
