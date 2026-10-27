package node_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/covijob/nosql-project/client"
	"github.com/covijob/nosql-project/internal/node"
)

func TestMain(m *testing.M) {
	if os.Getenv("KVDB_TEST_LOG") == "" {
		log.SetOutput(io.Discard)
	}
	os.Exit(m.Run())
}

type cluster struct {
	t     *testing.T
	dir   string
	addrs map[string]string
	nodes map[string]*node.Node
	db    *client.Client
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func newCluster(t *testing.T) *cluster {
	c := &cluster{t: t, dir: t.TempDir(), addrs: map[string]string{}, nodes: map[string]*node.Node{}}
	for _, id := range []string{"n1", "n2", "n3"} {
		c.addrs[id] = freeAddr(t)
	}
	var list []string
	for id, addr := range c.addrs {
		c.start(id)
		list = append(list, addr)
	}
	c.db = client.New(list)
	c.db.Timeout = 5 * time.Second
	t.Cleanup(func() {
		for id := range c.nodes {
			c.stop(id)
		}
	})
	return c
}

func (c *cluster) start(id string) *node.Node {
	peers := map[string]string{}
	for p, a := range c.addrs {
		if p != id {
			peers[p] = a
		}
	}
	n, err := node.New(node.Config{ID: id, Addr: c.addrs[id], Peers: peers,
		DataDir: filepath.Join(c.dir, id), Heartbeat: 100 * time.Millisecond,
		ElectionMin: 400 * time.Millisecond, ElectionMax: 800 * time.Millisecond,
		WriteTimeout: 1500 * time.Millisecond, MemtableLimit: 4096})
	if err != nil {
		c.t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = n
	return n
}

func (c *cluster) stop(id string) {
	if n, ok := c.nodes[id]; ok {
		n.Stop()
		delete(c.nodes, id)
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("per skirtą laiką neįvyko: %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// leader laukia, kol tarp veikiančių mazgų bus lygiai vienas lyderis.
func (c *cluster) leader() *node.Node {
	var leader *node.Node
	waitUntil(c.t, "išrinktas lyderis", func() bool {
		var leaders []*node.Node
		for _, n := range c.nodes {
			if n.Status().Role == node.Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			leader = leaders[0]
		}
		return len(leaders) == 1
	})
	return leader
}

func (c *cluster) get(key, addr string) string {
	c.t.Helper()
	v, _, err := c.db.Get(key, addr)
	if err != nil {
		c.t.Fatal(err)
	}
	return string(v)
}

func items(t *testing.T, n *node.Node) []string {
	t.Helper()
	db := client.New([]string{n.Addr()})
	cur, err := db.Traverse("", 1000, "")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for page, err := db.Continue(cur); len(page) > 0; page, err = db.Continue(cur) {
		if err != nil {
			t.Fatal(err)
		}
		for _, kv := range page {
			out = append(out, kv.Key+"="+string(kv.Value))
		}
	}
	return out
}

func mustOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestReplicationFailoverAndCatchUp(t *testing.T) {
	c := newCluster(t)
	oldLeader := c.leader()

	// w=all: po atsakymo duomenys yra visose replikose.
	for i := range 60 {
		mustOK(t, c.db.Put(fmt.Sprintf("key%03d", i), map[string]int{"i": i}, client.All))
	}
	for _, addr := range c.addrs {
		if v := c.get("key042", addr); v != `{"i":42}` {
			t.Fatalf("%s: key042 = %s", addr, v)
		}
	}

	mustOK(t, c.db.PutAll([]client.KV{{Key: "a", Value: json.RawMessage(`1`)},
		{Key: "b", Value: json.RawMessage(`[1,2]`)}, {Key: "c", Value: json.RawMessage(`"tekstas"`)}}, client.W2))
	old, found, err := c.db.Delete("b", client.Majority)
	mustOK(t, err)
	if !found || string(old) != "[1,2]" {
		t.Fatalf("delete grąžino %s %v", old, found)
	}
	if _, found, _ := c.db.Delete("b", client.W1); found {
		t.Fatal("antras delete rado raktą")
	}
	waitUntil(t, "b ištrintas visose replikose", func() bool {
		for _, addr := range c.addrs {
			if _, found, _ := c.db.Get("b", addr); found {
				return false
			}
		}
		return true
	})

	// traverse/continue puslapiais (kiekvienas puslapis gali būti skaitomas iš kitos replikos).
	cur, err := c.db.Traverse("key050", 4, "")
	mustOK(t, err)
	var got []string
	for page, err := c.db.Continue(cur); len(page) > 0; page, err = c.db.Continue(cur) {
		mustOK(t, err)
		if len(page) > 4 {
			t.Fatalf("puslapis per didelis: %d", len(page))
		}
		for _, kv := range page {
			got = append(got, kv.Key)
		}
	}
	var want []string
	for i := 50; i < 60; i++ {
		want = append(want, fmt.Sprintf("key%03d", i))
	}
	if !slices.Equal(got, want) {
		t.Fatalf("traverse = %v", got)
	}

	// Praradus lyderį – išrenkamas naujas, rašymas tęsiasi.
	oldID := oldLeader.ID()
	c.stop(oldID)
	newLeader := c.leader()
	if newLeader.ID() == oldID {
		t.Fatal("lyderis nepasikeitė")
	}
	if v := c.get("key042", newLeader.Addr()); v != `{"i":42}` {
		t.Fatalf("naujas lyderis neturi key042: %s", v)
	}
	for i := 60; i < 120; i++ {
		mustOK(t, c.db.Put(fmt.Sprintf("key%03d", i), i, client.Majority))
	}
	err = c.db.Put("x", 1, client.All) // trečioji replika neveikia
	var se *client.Error
	if !errors.As(err, &se) || se.Status != 504 {
		t.Fatalf("w=all be vienos replikos: laukta 504, gauta %v", err)
	}

	// Grįžęs mazgas pasiveja (eventual consistency).
	restarted := c.start(oldID)
	waitUntil(t, "grįžęs mazgas pasiveja", func() bool {
		v, _, _ := c.db.Get("key119", restarted.Addr())
		return string(v) == "119" && restarted.Status().LastSeq == newLeader.Status().LastSeq
	})
	if !slices.Equal(items(t, restarted), items(t, newLeader)) {
		t.Fatal("grįžusio mazgo duomenys nesutampa su lyderio")
	}
	if restarted.Status().Role != node.Follower {
		t.Fatal("grįžęs mazgas turėjo likti sekėju")
	}
}

func TestDivergentFollowerGetsSnapshot(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	mustOK(t, c.db.Put("base", 0, client.All))
	var followers []string
	for id := range c.nodes {
		if id != leader.ID() {
			followers = append(followers, id)
		}
	}
	for _, id := range followers {
		c.stop(id)
	}
	// w=1: lyderis priima rašymą, kurio niekas kitas negavo.
	mustOK(t, c.db.Put("lost", "only-on-old-leader", client.W1))
	c.stop(leader.ID())
	for _, id := range followers {
		c.start(id)
	}
	newLeader := c.leader()
	mustOK(t, c.db.Put("after", 1, client.Majority))
	old := c.start(leader.ID())
	// Senasis lyderis atsisako nepatvirtinto įrašo ir sutampa su nauju lyderiu.
	waitUntil(t, "senasis lyderis gauna naujus duomenis", func() bool {
		v, _, _ := c.db.Get("after", old.Addr())
		return string(v) == "1"
	})
	if _, found, _ := c.db.Get("lost", old.Addr()); found {
		t.Fatal("nepatvirtintas įrašas turėjo dingti")
	}
	if !slices.Equal(items(t, old), items(t, newLeader)) {
		t.Fatal("duomenys nesutampa su lyderio")
	}
}

func TestInvalidRequests(t *testing.T) {
	c := newCluster(t)
	c.leader()
	var se *client.Error
	if err := c.db.Put("k", 1, "5"); !errors.As(err, &se) || se.Status != 400 {
		t.Fatalf("w=5: laukta 400, gauta %v", err)
	}
	if err := c.db.Put("", 1, client.W1); err == nil {
		t.Fatal("tuščias raktas turėjo būti atmestas")
	}
	if _, err := c.db.Traverse("a", 0, ""); err == nil {
		t.Fatal("pageSize=0 turėjo būti atmestas")
	}
}
