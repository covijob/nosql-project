// Package oplog – replikacijos žurnalas.
//
// Kiekvienas įrašas turi lyderio priskirtą eilės numerį (Seq) ir term'ą.
// Sekėjai įrašus taiko ta pačia tvarka. Seni įrašai periodiškai nukerpami –
// per daug atsilikęs sekėjas gauna snapshot'ą.
package oplog

import (
	"bufio"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
)

// Op – viena rašymo operacija. Del = ištrynimas.
type Op struct {
	Key   string          `json:"k"`
	Value json.RawMessage `json:"v,omitempty"`
	Del   bool            `json:"d,omitempty"`
}

type Entry struct {
	Seq  uint64 `json:"seq"`
	Term uint64 `json:"term"`
	Ops  []Op   `json:"ops"`
}

type header struct {
	BaseSeq  uint64 `json:"base_seq"`
	BaseTerm uint64 `json:"base_term"`
}

type Log struct {
	path     string
	f        *os.File
	BaseSeq  uint64
	BaseTerm uint64
	Entries  []Entry
}

func Open(path string) (*Log, error) {
	l := &Log{path: path}
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return l, l.rewrite()
	}
	if err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<30)
	if sc.Scan() {
		var h header
		if json.Unmarshal(sc.Bytes(), &h) == nil {
			l.BaseSeq, l.BaseTerm = h.BaseSeq, h.BaseTerm
		}
	}
	for sc.Scan() {
		var e Entry
		if json.Unmarshal(sc.Bytes(), &e) != nil || e.Seq != l.LastSeq()+1 {
			break // nebaigta paskutinė eilutė
		}
		l.Entries = append(l.Entries, e)
	}
	f.Close()
	// Perrašom, kad būtų išmesta galima sugadinta uodega.
	return l, l.rewrite()
}

func (l *Log) LastSeq() uint64 {
	if n := len(l.Entries); n > 0 {
		return l.Entries[n-1].Seq
	}
	return l.BaseSeq
}

func (l *Log) LastTerm() uint64 {
	if n := len(l.Entries); n > 0 {
		return l.Entries[n-1].Term
	}
	return l.BaseTerm
}

// TermAt grąžina įrašo seq term'ą; false – įrašo nėra arba jis nukirptas.
func (l *Log) TermAt(seq uint64) (uint64, bool) {
	switch {
	case seq == l.BaseSeq:
		return l.BaseTerm, true
	case seq < l.BaseSeq || seq > l.LastSeq():
		return 0, false
	}
	return l.Entries[seq-l.BaseSeq-1].Term, true
}

// From grąžina įrašus nuo seq (iki limit vnt.; limit <= 0 – visus).
func (l *Log) From(seq uint64, limit int) []Entry {
	i := 0
	if seq > l.BaseSeq {
		i = int(seq - l.BaseSeq - 1)
	}
	if i > len(l.Entries) {
		return nil
	}
	out := l.Entries[i:]
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func (l *Log) Append(e Entry) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := l.f.Write(append(raw, '\n')); err != nil {
		return err
	}
	l.Entries = append(l.Entries, e)
	return nil
}

func (l *Log) Reset(seq, term uint64) error {
	l.BaseSeq, l.BaseTerm, l.Entries = seq, term, nil
	return l.rewrite()
}

// Trim palieka tik paskutinius keep įrašų.
func (l *Log) Trim(keep int) error {
	if len(l.Entries) <= keep {
		return nil
	}
	base := l.Entries[len(l.Entries)-keep-1]
	l.BaseSeq, l.BaseTerm = base.Seq, base.Term
	l.Entries = append([]Entry(nil), l.Entries[len(l.Entries)-keep:]...)
	return l.rewrite()
}

func (l *Log) rewrite() error {
	if l.f != nil {
		l.f.Close()
		l.f = nil
	}
	tmp := l.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	enc.Encode(header{l.BaseSeq, l.BaseTerm})
	for _, e := range l.Entries {
		enc.Encode(e)
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, l.path); err != nil {
		return err
	}
	l.f, err = os.OpenFile(l.path, os.O_WRONLY|os.O_APPEND, 0o644)
	return err
}

func (l *Log) Close() error { return l.f.Close() }
