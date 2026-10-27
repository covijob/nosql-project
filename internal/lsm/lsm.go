// Package lsm – LSM medžio saugojimo variklis.
//
// Rašymas: WAL -> memtable (atmintyje, surikiuota). Memtable viršijus ribą ji
// išrašoma į nekeičiamą SSTable failą. SSTable failų skaičiui viršijus ribą visi
// sujungiami (kompaktavimas) – lieka naujausia kiekvieno rakto versija, antkapiai
// (ištrynimo žymės) išmetami.
//
// Skaitymas: memtable -> SSTable nuo naujausio iki seniausio (pirmas radinys laimi).
package lsm

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
)

// MetaPrefix – vidinių (sisteminių) raktų prefiksas; tokie raktai nematomi Scan rezultatuose.
const MetaPrefix = "\x00"

// Entry – rašymo operacija arba saugomas įrašas. Deleted = antkapis.
type Entry struct {
	Key     []byte
	Value   []byte
	Deleted bool
}

type Options struct {
	MemtableLimit int  // baitais, numatyta 1 MB
	MaxSSTables   int  // numatyta 4
	Sync          bool // fsync po kiekvieno WAL įrašo
}

type Tree struct {
	mu      sync.RWMutex
	dir     string
	opts    Options
	mem     map[string]Entry
	memKeys []string // surikiuoti memtable raktai
	memSize int
	tables  []*sstable // naujausias pirmas
	nextID  int
	wal     *wal
}

type manifest struct {
	SSTables []string `json:"sstables"`
	NextID   int      `json:"next_id"`
}

func Open(dir string, opts Options) (*Tree, error) {
	if opts.MemtableLimit <= 0 {
		opts.MemtableLimit = 1 << 20
	}
	if opts.MaxSSTables <= 0 {
		opts.MaxSSTables = 4
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	t := &Tree{dir: dir, opts: opts}
	if err := t.load(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *Tree) manifestPath() string { return filepath.Join(t.dir, "MANIFEST.json") }

func (t *Tree) load() error {
	m := manifest{NextID: 1}
	raw, err := os.ReadFile(t.manifestPath())
	if err == nil {
		if err := json.Unmarshal(raw, &m); err != nil {
			return fmt.Errorf("MANIFEST: %w", err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	t.nextID, t.tables = m.NextID, nil
	for _, name := range m.SSTables {
		st, err := openSSTable(filepath.Join(t.dir, name))
		if err != nil {
			return err
		}
		t.tables = append(t.tables, st)
	}
	// Neužbaigtų flush/kompaktavimų likučiai.
	files, err := os.ReadDir(t.dir)
	if err != nil {
		return err
	}
	for _, f := range files {
		name := f.Name()
		if strings.HasSuffix(name, ".tmp") || (strings.HasSuffix(name, ".sst") && !slices.Contains(m.SSTables, name)) {
			os.Remove(filepath.Join(t.dir, name))
		}
	}
	t.mem, t.memKeys, t.memSize = map[string]Entry{}, nil, 0
	walPath := filepath.Join(t.dir, "wal.log")
	batches, err := replayWAL(walPath)
	if err != nil {
		return err
	}
	for _, b := range batches {
		t.memApply(b)
	}
	t.wal, err = openWAL(walPath, t.opts.Sync)
	return err
}

func (t *Tree) writeManifest() error {
	m := manifest{NextID: t.nextID, SSTables: []string{}}
	for _, st := range t.tables {
		m.SSTables = append(m.SSTables, filepath.Base(st.path))
	}
	raw, _ := json.Marshal(m)
	return writeFileSync(t.manifestPath(), raw)
}

// ---------- rašymas ----------

func (t *Tree) memApply(batch []Entry) {
	for _, e := range batch {
		k := string(e.Key)
		if _, ok := t.mem[k]; !ok {
			i := sort.SearchStrings(t.memKeys, k)
			t.memKeys = slices.Insert(t.memKeys, i, k)
		}
		t.mem[k] = e
		t.memSize += len(e.Key) + len(e.Value) + 16
	}
}

// WriteBatch atomiškai įrašo paketą operacijų.
func (t *Tree) WriteBatch(batch []Entry) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.wal.append(batch); err != nil {
		return err
	}
	t.memApply(batch)
	if t.memSize >= t.opts.MemtableLimit {
		return t.flush()
	}
	return nil
}

func (t *Tree) Put(key, value []byte) error {
	return t.WriteBatch([]Entry{{Key: key, Value: value}})
}

func (t *Tree) Delete(key []byte) error {
	return t.WriteBatch([]Entry{{Key: key, Deleted: true}})
}

func (t *Tree) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.flush()
}

func (t *Tree) flush() error {
	if len(t.mem) == 0 {
		return nil
	}
	entries := make([]Entry, len(t.memKeys))
	for i, k := range t.memKeys {
		entries[i] = t.mem[k]
	}
	st, err := writeSSTable(t.newTablePath(), entries)
	if err != nil {
		return err
	}
	t.tables = slices.Insert(t.tables, 0, st)
	if err := t.writeManifest(); err != nil {
		return err
	}
	if err := t.wal.reset(); err != nil {
		return err
	}
	t.mem, t.memKeys, t.memSize = map[string]Entry{}, nil, 0
	if len(t.tables) > t.opts.MaxSSTables {
		return t.compact()
	}
	return nil
}

func (t *Tree) newTablePath() string {
	p := filepath.Join(t.dir, fmt.Sprintf("%08d.sst", t.nextID))
	t.nextID++
	return p
}

func (t *Tree) compact() error {
	old := t.tables
	iters := make([]iterator, len(old))
	for i, st := range old {
		iters[i] = st.iterFrom(nil)
	}
	// Sujungiami visi failai, todėl antkapiai nebereikalingi – jie nieko neslepia.
	var merged []Entry
	m := newMerger(iters)
	for e, ok := m.next(); ok; e, ok = m.next() {
		if !e.Deleted {
			merged = append(merged, e)
		}
	}
	if err := iterErr(iters); err != nil {
		return err
	}
	t.tables = nil
	if len(merged) > 0 {
		st, err := writeSSTable(t.newTablePath(), merged)
		if err != nil {
			t.tables = old
			return err
		}
		t.tables = []*sstable{st}
	}
	if err := t.writeManifest(); err != nil {
		return err
	}
	for _, st := range old {
		st.close()
		os.Remove(st.path)
	}
	return nil
}

// ---------- skaitymas ----------

// Get grąžina reikšmę ir ar raktas egzistuoja.
func (t *Tree) Get(key []byte) ([]byte, bool, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if e, ok := t.mem[string(key)]; ok {
		return e.Value, !e.Deleted, nil
	}
	for _, st := range t.tables {
		e, ok, err := st.get(key)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return e.Value, !e.Deleted, nil
		}
	}
	return nil, false, nil
}

// Scan grąžina iki limit gyvų įrašų, kurių raktas >= start (arba > start, jei !inclusive).
// limit <= 0 – visi.
func (t *Tree) Scan(start []byte, limit int, inclusive bool) ([]Entry, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	i := sort.SearchStrings(t.memKeys, string(start))
	iters := []iterator{&memIter{t: t, keys: t.memKeys[i:]}}
	for _, st := range t.tables {
		iters = append(iters, st.iterFrom(start))
	}
	var out []Entry
	m := newMerger(iters)
	for e, ok := m.next(); ok; e, ok = m.next() {
		if e.Deleted || bytes.HasPrefix(e.Key, []byte(MetaPrefix)) || (!inclusive && bytes.Equal(e.Key, start)) {
			continue
		}
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, iterErr(iters)
}

// Items grąžina visus gyvus (ne sisteminius) įrašus.
func (t *Tree) Items() ([]Entry, error) { return t.Scan(nil, 0, true) }

type Stats struct {
	MemtableKeys int `json:"memtable_keys"`
	SSTables     int `json:"sstables"`
}

func (t *Tree) Stats() Stats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return Stats{MemtableKeys: len(t.mem), SSTables: len(t.tables)}
}

// Clear ištrina visus duomenis (naudojama diegiant snapshot'ą iš lyderio).
func (t *Tree) Clear() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closeFiles()
	files, err := os.ReadDir(t.dir)
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := os.Remove(filepath.Join(t.dir, f.Name())); err != nil {
			return err
		}
	}
	return t.load()
}

func (t *Tree) closeFiles() {
	for _, st := range t.tables {
		st.close()
	}
	t.wal.close()
}

func (t *Tree) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closeFiles()
	return nil
}

// ---------- iteratoriai ir k-krypčių sąlaja ----------

type iterator interface {
	next() (Entry, bool)
}

func iterErr(iters []iterator) error {
	for _, it := range iters {
		if s, ok := it.(*sstIter); ok && s.err != nil {
			return s.err
		}
	}
	return nil
}

type memIter struct {
	t    *Tree
	keys []string
}

func (it *memIter) next() (Entry, bool) {
	if len(it.keys) == 0 {
		return Entry{}, false
	}
	e := it.t.mem[it.keys[0]]
	it.keys = it.keys[1:]
	return e, true
}

type heapItem struct {
	e    Entry
	prio int // mažesnis = naujesnis šaltinis
	it   iterator
}

type entryHeap []heapItem

func (h entryHeap) Len() int { return len(h) }
func (h entryHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].e.Key, h[j].e.Key); c != 0 {
		return c < 0
	}
	return h[i].prio < h[j].prio
}
func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *entryHeap) Push(x any)   { *h = append(*h, x.(heapItem)) }
func (h *entryHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// merger sulieja surikiuotus iteratorius (naujausias pirmas) ir kiekvienam raktui
// grąžina tik naujausią versiją (įskaitant antkapius).
type merger struct {
	h    entryHeap
	last []byte
	seen bool
}

func newMerger(iters []iterator) *merger {
	m := &merger{}
	for prio, it := range iters {
		if e, ok := it.next(); ok {
			m.h = append(m.h, heapItem{e, prio, it})
		}
	}
	heap.Init(&m.h)
	return m
}

func (m *merger) next() (Entry, bool) {
	for m.h.Len() > 0 {
		top := heap.Pop(&m.h).(heapItem)
		if e, ok := top.it.next(); ok {
			heap.Push(&m.h, heapItem{e, top.prio, top.it})
		}
		if m.seen && bytes.Equal(top.e.Key, m.last) {
			continue
		}
		m.last, m.seen = top.e.Key, true
		return top.e, true
	}
	return Entry{}, false
}
