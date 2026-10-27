package lsm

import (
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"
)

func openTree(t *testing.T, dir string) *Tree {
	t.Helper()
	tree, err := Open(dir, Options{MemtableLimit: 2048, MaxSSTables: 3})
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

func mustGet(t *testing.T, tree *Tree, key string) (string, bool) {
	t.Helper()
	v, ok, err := tree.Get([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	return string(v), ok
}

func keys(entries []Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = string(e.Key)
	}
	return out
}

func TestPutGetDelete(t *testing.T) {
	tree := openTree(t, t.TempDir())
	defer tree.Close()
	tree.Put([]byte("a"), []byte("1"))
	if v, ok := mustGet(t, tree, "a"); !ok || v != "1" {
		t.Fatalf("get a = %q %v", v, ok)
	}
	tree.Put([]byte("a"), []byte("2"))
	if v, _ := mustGet(t, tree, "a"); v != "2" {
		t.Fatalf("get a = %q", v)
	}
	tree.Delete([]byte("a"))
	if _, ok := mustGet(t, tree, "a"); ok {
		t.Fatal("ištrintas raktas vis dar randamas")
	}
	if _, ok := mustGet(t, tree, "missing"); ok {
		t.Fatal("rastas neegzistuojantis raktas")
	}
}

func TestFlushCompactionAndReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "lsm")
	tree := openTree(t, dir)
	expected := map[string]string{}
	rnd := rand.New(rand.NewPCG(1, 2))
	for i := range 3000 {
		key := fmt.Sprintf("k%04d", rnd.IntN(500))
		if rnd.Float64() < 0.2 {
			tree.Delete([]byte(key))
			delete(expected, key)
		} else {
			value := fmt.Sprintf("v%d", i)
			tree.Put([]byte(key), []byte(value))
			expected[key] = value
		}
	}
	if s := tree.Stats(); s.SSTables > 3 || s.SSTables == 0 {
		t.Fatalf("netikėtas SSTable skaičius: %d", s.SSTables)
	}
	check := func(state string) {
		for i := range 500 {
			key := fmt.Sprintf("k%04d", i)
			v, ok := mustGet(t, tree, key)
			want, wantOK := expected[key]
			if v != want || ok != wantOK {
				t.Fatalf("%s: %s = %q %v, laukta %q %v", state, key, v, ok, want, wantOK)
			}
		}
		items, err := tree.Items()
		if err != nil {
			t.Fatal(err)
		}
		var want []string
		for k := range expected {
			want = append(want, k)
		}
		slices.Sort(want)
		if !slices.Equal(keys(items), want) {
			t.Fatalf("%s: Items() raktai nesutampa", state)
		}
	}
	check("gyvas")
	tree.Close()
	tree = openTree(t, dir)
	defer tree.Close()
	check("atidarytas iš naujo")
}

func TestWALRecovery(t *testing.T) {
	dir := t.TempDir()
	tree := openTree(t, dir)
	tree.Put([]byte("x"), []byte("1"))
	tree.WriteBatch([]Entry{{Key: []byte("y"), Value: []byte("2")}, {Key: []byte("x"), Deleted: true}})
	tree.Close()
	tree = openTree(t, dir)
	defer tree.Close()
	if _, ok := mustGet(t, tree, "x"); ok {
		t.Fatal("x turėjo būti ištrintas")
	}
	if v, _ := mustGet(t, tree, "y"); v != "2" {
		t.Fatalf("y = %q", v)
	}
}

func TestScanPages(t *testing.T) {
	tree := openTree(t, t.TempDir())
	defer tree.Close()
	for i := range 100 {
		tree.Put(fmt.Appendf(nil, "k%03d", i), fmt.Append(nil, i))
	}
	tree.Delete([]byte("k050"))
	page, _ := tree.Scan([]byte("k048"), 3, true)
	if got := keys(page); !slices.Equal(got, []string{"k048", "k049", "k051"}) {
		t.Fatalf("puslapis: %v", got)
	}
	page, _ = tree.Scan([]byte("k049"), 2, false)
	if got := keys(page); !slices.Equal(got, []string{"k051", "k052"}) {
		t.Fatalf("puslapis be pradžios rakto: %v", got)
	}
}

func TestMetaKeysHidden(t *testing.T) {
	tree := openTree(t, t.TempDir())
	defer tree.Close()
	tree.Put([]byte(MetaPrefix+"meta"), []byte("x"))
	tree.Put([]byte("a"), []byte("1"))
	items, _ := tree.Items()
	if got := keys(items); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("Items() = %v", got)
	}
}

func TestClear(t *testing.T) {
	dir := t.TempDir()
	tree := openTree(t, dir)
	for i := range 200 {
		tree.Put(fmt.Appendf(nil, "k%d", i), make([]byte, 50))
	}
	if err := tree.Clear(); err != nil {
		t.Fatal(err)
	}
	tree.Put([]byte("a"), []byte("1"))
	tree.Close()
	tree = openTree(t, dir)
	defer tree.Close()
	items, _ := tree.Items()
	if got := keys(items); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("Items() po Clear = %v", got)
	}
}
