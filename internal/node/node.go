// Package node – replikuotos rakto-reikšmės duomenų bazės mazgas.
//
// Replikacija – vienas pirminis mazgas (lyderis) ir sekėjai (Raft principu):
//   - visi rašymai vykdomi lyderyje, jis priskiria jiems eilės numerį (seq) ir
//     asinchroniškai siunčia sekėjams (/raft/append, kartu tai ir heartbeat);
//   - klientas renkasi rašymo patikimumą w = 1 | 2 | majority | all – lyderis
//     atsako tik tada, kai įrašą patvirtino tiek replikų;
//   - skaityti galima iš bet kurio mazgo (eventual consistency);
//   - negavęs heartbeat'o sekėjas pradeda rinkimus; balsas atiduodamas tik
//     kandidatui, kurio žurnalas ne senesnis – laimi gavęs daugumą balsų;
//   - išsiskyręs (divergavęs) ar per daug atsilikęs sekėjas gauna pilną snapshot'ą.
package node

import (
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/covijob/nosql-project/internal/lsm"
	"github.com/covijob/nosql-project/internal/oplog"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

const (
	metaApplied     = lsm.MetaPrefix + "applied"
	rpcTimeout      = 500 * time.Millisecond
	snapshotTimeout = 30 * time.Second
	appendBatch     = 500
	oplogMax        = 50_000
	oplogKeep       = 10_000
)

type Config struct {
	ID            string
	Addr          string            // host:port
	Peers         map[string]string // kiti mazgai: id -> host:port
	DataDir       string
	Heartbeat     time.Duration
	ElectionMin   time.Duration
	ElectionMax   time.Duration
	WriteTimeout  time.Duration
	MemtableLimit int
}

func (c *Config) setDefaults() {
	if c.Heartbeat == 0 {
		c.Heartbeat = 300 * time.Millisecond
	}
	if c.ElectionMin == 0 {
		c.ElectionMin = time.Second
	}
	if c.ElectionMax <= c.ElectionMin {
		c.ElectionMax = 2 * c.ElectionMin
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 5 * time.Second
	}
}

type Node struct {
	cfg  Config
	lsm  *lsm.Tree
	log  *oplog.Log
	http *http.Client

	mu              sync.Mutex
	changed         chan struct{} // uždaromas ir pakeičiamas kiekvieno būsenos pokyčio metu
	role            Role
	term            uint64
	votedFor        string
	leaderID        string
	nextSeq         map[string]uint64
	matchSeq        map[string]uint64
	needSnapshot    map[string]bool
	lastContact     time.Time
	electionTimeout time.Duration

	server  *http.Server
	stopped chan struct{}
	wg      sync.WaitGroup
}

type persistentState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

func New(cfg Config) (*Node, error) {
	cfg.setDefaults()
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	tree, err := lsm.Open(filepath.Join(cfg.DataDir, "lsm"), lsm.Options{MemtableLimit: cfg.MemtableLimit})
	if err != nil {
		return nil, err
	}
	ol, err := oplog.Open(filepath.Join(cfg.DataDir, "oplog.jsonl"))
	if err != nil {
		tree.Close()
		return nil, err
	}
	n := &Node{
		cfg: cfg, lsm: tree, log: ol,
		http:         &http.Client{Transport: &http.Transport{Proxy: nil, MaxIdleConnsPerHost: 16}},
		changed:      make(chan struct{}),
		role:         Follower,
		nextSeq:      map[string]uint64{},
		matchSeq:     map[string]uint64{},
		needSnapshot: map[string]bool{},
		lastContact:  time.Now(),
		stopped:      make(chan struct{}),
	}
	n.resetElectionTimer()
	raw, err := os.ReadFile(n.statePath())
	if err == nil {
		var st persistentState
		if err := json.Unmarshal(raw, &st); err != nil {
			return nil, err
		}
		n.term, n.votedFor = st.Term, st.VotedFor
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if err := n.recover(); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Node) ID() string   { return n.cfg.ID }
func (n *Node) Addr() string { return n.cfg.Addr }

func (n *Node) clusterSize() int { return len(n.cfg.Peers) + 1 }
func (n *Node) majority() int    { return n.clusterSize()/2 + 1 }

// ---------- būsena ir duomenų taikymas ----------

func (n *Node) statePath() string { return filepath.Join(n.cfg.DataDir, "state.json") }

func (n *Node) recover() error {
	var applied uint64
	if raw, ok, err := n.lsm.Get([]byte(metaApplied)); err != nil {
		return err
	} else if ok {
		var e oplog.Entry
		json.Unmarshal(raw, &e)
		applied = e.Seq
	}
	if applied < n.log.BaseSeq {
		// Žurnalas nukirptas toliau nei pritaikyta – būseną gausime iš lyderio.
		if err := n.lsm.Clear(); err != nil {
			return err
		}
		return n.log.Reset(0, 0)
	}
	for _, e := range n.log.From(applied+1, 0) {
		if err := n.applyToLSM(e); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) saveState() {
	raw, _ := json.Marshal(persistentState{n.term, n.votedFor})
	tmp := n.statePath() + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err == nil {
		err = os.Rename(tmp, n.statePath())
	}
}

// apply įrašo įrašą į žurnalą ir LSM. Kviečiama laikant n.mu.
func (n *Node) apply(e oplog.Entry) error {
	if err := n.log.Append(e); err != nil {
		return err
	}
	if err := n.applyToLSM(e); err != nil {
		return err
	}
	if len(n.log.Entries) > oplogMax {
		return n.log.Trim(oplogKeep)
	}
	return nil
}

func (n *Node) applyToLSM(e oplog.Entry) error {
	batch := make([]lsm.Entry, 0, len(e.Ops)+1)
	for _, op := range e.Ops {
		batch = append(batch, lsm.Entry{Key: []byte(op.Key), Value: op.Value, Deleted: op.Del})
	}
	meta, _ := json.Marshal(oplog.Entry{Seq: e.Seq, Term: e.Term})
	batch = append(batch, lsm.Entry{Key: []byte(metaApplied), Value: meta})
	return n.lsm.WriteBatch(batch)
}

func (n *Node) resetElectionTimer() {
	span := n.cfg.ElectionMax - n.cfg.ElectionMin
	n.electionTimeout = n.cfg.ElectionMin + rand.N(span)
}

// notify pažadina visus laukiančius būsenos pokyčio. Kviečiama laikant n.mu.
func (n *Node) notify() {
	close(n.changed)
	n.changed = make(chan struct{})
}

// waitFor laukia, kol cond() taps true arba baigsis laikas. Kviečiama laikant n.mu.
func (n *Node) waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for !cond() {
		left := time.Until(deadline)
		if left <= 0 {
			return false
		}
		ch := n.changed
		n.mu.Unlock()
		t := time.NewTimer(left)
		select {
		case <-ch:
		case <-t.C:
		case <-n.stopped:
		}
		t.Stop()
		n.mu.Lock()
		if n.isStopped() {
			return cond()
		}
	}
	return true
}

func (n *Node) isStopped() bool {
	select {
	case <-n.stopped:
		return true
	default:
		return false
	}
}

func (n *Node) becomeFollower(term uint64, leader string) {
	if term > n.term {
		n.term, n.votedFor = term, ""
		n.saveState()
	}
	if n.role != Follower {
		log.Printf("[%s] tampa sekėju (term %d)", n.cfg.ID, n.term)
	}
	n.role = Follower
	if leader != "" {
		n.leaderID = leader
	}
	n.notify()
}

func (n *Node) becomeLeader() {
	log.Printf("[%s] išrinktas lyderiu (term %d, seq %d)", n.cfg.ID, n.term, n.log.LastSeq())
	n.role, n.leaderID = Leader, n.cfg.ID
	for p := range n.cfg.Peers {
		n.nextSeq[p] = n.log.LastSeq() + 1
		n.matchSeq[p] = 0
		n.needSnapshot[p] = false
	}
	n.notify()
}

// ---------- gyvavimo ciklas ----------

func (n *Node) Start() error {
	ln, err := net.Listen("tcp", n.cfg.Addr)
	if err != nil {
		return err
	}
	n.server = &http.Server{Handler: n.routes()}
	n.goRun(func() { n.server.Serve(ln) })
	n.goRun(n.electionLoop)
	for p := range n.cfg.Peers {
		n.goRun(func() { n.replicateLoop(p) })
	}
	log.Printf("[%s] klausosi %s, kaimynai: %v", n.cfg.ID, n.cfg.Addr, n.cfg.Peers)
	return nil
}

func (n *Node) goRun(f func()) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		f()
	}()
}

func (n *Node) Stop() {
	close(n.stopped)
	n.server.Close()
	n.wg.Wait()
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lsm.Close()
	n.log.Close()
}

// ---------- lyderio rinkimai ----------

type voteRequest struct {
	Term      uint64 `json:"term"`
	Candidate string `json:"candidate"`
	LastSeq   uint64 `json:"last_seq"`
	LastTerm  uint64 `json:"last_term"`
}

type voteResponse struct {
	Term    uint64 `json:"term"`
	Granted bool   `json:"granted"`
}

func (n *Node) electionLoop() {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-n.stopped:
			return
		case <-tick.C:
		}
		n.mu.Lock()
		if n.role == Leader || time.Since(n.lastContact) < n.electionTimeout {
			n.mu.Unlock()
			continue
		}
		n.term++
		n.role, n.votedFor, n.leaderID = Candidate, n.cfg.ID, ""
		n.saveState()
		n.lastContact = time.Now()
		n.resetElectionTimer()
		req := voteRequest{n.term, n.cfg.ID, n.log.LastSeq(), n.log.LastTerm()}
		log.Printf("[%s] pradeda rinkimus (term %d)", n.cfg.ID, req.Term)
		n.mu.Unlock()

		votes := 1 + n.collectVotes(req)

		n.mu.Lock()
		if n.role == Candidate && n.term == req.Term && votes >= n.majority() {
			n.becomeLeader()
		}
		n.mu.Unlock()
	}
}

func (n *Node) collectVotes(req voteRequest) int {
	results := make(chan *voteResponse, len(n.cfg.Peers))
	for p := range n.cfg.Peers {
		go func() {
			var resp voteResponse
			if n.callPeer(p, "/raft/vote", req, &resp, rpcTimeout) != nil {
				results <- nil
				return
			}
			results <- &resp
		}()
	}
	granted := 0
	for range n.cfg.Peers {
		resp := <-results
		if resp == nil {
			continue
		}
		n.mu.Lock()
		if resp.Term > n.term {
			n.becomeFollower(resp.Term, "")
		}
		n.mu.Unlock()
		if resp.Granted {
			granted++
		}
	}
	return granted
}

func (n *Node) onVote(req voteRequest) voteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term > n.term {
		n.becomeFollower(req.Term, "")
	}
	upToDate := req.LastTerm > n.log.LastTerm() ||
		(req.LastTerm == n.log.LastTerm() && req.LastSeq >= n.log.LastSeq())
	granted := req.Term == n.term && upToDate && (n.votedFor == "" || n.votedFor == req.Candidate)
	if granted {
		n.votedFor = req.Candidate
		n.saveState()
		n.lastContact = time.Now()
	}
	return voteResponse{n.term, granted}
}

// ---------- replikacija (lyderio pusė) ----------

type appendRequest struct {
	Term          uint64        `json:"term"`
	Leader        string        `json:"leader"`
	PrevSeq       uint64        `json:"prev_seq"`
	PrevTerm      uint64        `json:"prev_term"`
	Entries       []oplog.Entry `json:"entries"`
	LeaderLastSeq uint64        `json:"leader_last_seq"`
}

type snapshotRequest struct {
	Term     uint64 `json:"term"`
	Leader   string `json:"leader"`
	Seq      uint64 `json:"seq"`
	LastTerm uint64 `json:"last_term"`
	Items    []KV   `json:"items"`
}

type appendResponse struct {
	Term         uint64 `json:"term"`
	Success      bool   `json:"success"`
	MatchSeq     uint64 `json:"match_seq,omitempty"`
	LastSeq      uint64 `json:"last_seq,omitempty"`
	NeedSnapshot bool   `json:"need_snapshot,omitempty"`
}

// acks – kiek replikų (įskaitant lyderį) turi įrašą seq. Kviečiama laikant n.mu.
func (n *Node) acks(seq uint64) int {
	count := 1
	for _, m := range n.matchSeq {
		if m >= seq {
			count++
		}
	}
	return count
}

func (n *Node) replicateLoop(peer string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for !n.isStopped() {
		if n.role != Leader {
			n.waitFor(func() bool { return n.role == Leader || n.isStopped() }, n.cfg.Heartbeat)
			continue
		}
		term, next := n.term, n.nextSeq[peer]
		var (
			path    string
			payload any
			timeout = rpcTimeout
		)
		if n.needSnapshot[peer] || next <= n.log.BaseSeq {
			items, err := n.lsm.Items()
			if err != nil {
				log.Printf("[%s] snapshot klaida: %v", n.cfg.ID, err)
				n.waitFor(func() bool { return false }, n.cfg.Heartbeat)
				continue
			}
			req := snapshotRequest{term, n.cfg.ID, n.log.LastSeq(), n.log.LastTerm(), make([]KV, len(items))}
			for i, e := range items {
				req.Items[i] = KV{string(e.Key), e.Value}
			}
			path, payload, timeout = "/raft/snapshot", req, snapshotTimeout
		} else {
			prevTerm, _ := n.log.TermAt(next - 1)
			path, payload = "/raft/append", appendRequest{term, n.cfg.ID, next - 1, prevTerm,
				n.log.From(next, appendBatch), n.log.LastSeq()}
		}
		n.mu.Unlock()
		var resp appendResponse
		err := n.callPeer(peer, path, payload, &resp, timeout)
		n.mu.Lock()

		switch {
		case err != nil: // mazgas nepasiekiamas – bandysime po heartbeat intervalo
			n.waitFor(func() bool { return n.isStopped() }, n.cfg.Heartbeat)
		case resp.Term > n.term:
			n.becomeFollower(resp.Term, "")
		case n.role != Leader || n.term != term:
		case resp.Success:
			n.needSnapshot[peer] = false
			n.matchSeq[peer] = max(n.matchSeq[peer], resp.MatchSeq)
			n.nextSeq[peer] = resp.MatchSeq + 1
			n.notify()
			// Laukiam naujų įrašų arba heartbeat laiko.
			n.waitFor(func() bool {
				return n.isStopped() || n.role != Leader || n.log.LastSeq() >= n.nextSeq[peer]
			}, n.cfg.Heartbeat)
		case resp.NeedSnapshot:
			n.needSnapshot[peer] = true
		default:
			n.nextSeq[peer] = min(next-1, resp.LastSeq+1)
		}
	}
}

// ---------- replikacija (sekėjo pusė) ----------

// acceptLeader – bendra append/snapshot pradžia. Grąžina false, jei užklausa iš pasenusio lyderio.
func (n *Node) acceptLeader(term uint64, leader string) bool {
	if term < n.term {
		return false
	}
	n.becomeFollower(term, leader)
	n.lastContact = time.Now()
	return true
}

func (n *Node) onAppend(req appendRequest) (appendResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.acceptLeader(req.Term, req.Leader) {
		return appendResponse{Term: n.term}, nil
	}
	resp := appendResponse{Term: n.term}
	if req.PrevSeq > n.log.LastSeq() {
		resp.LastSeq = n.log.LastSeq()
		return resp, nil
	}
	if t, ok := n.log.TermAt(req.PrevSeq); !ok || t != req.PrevTerm {
		resp.NeedSnapshot = true
		return resp, nil
	}
	for _, e := range req.Entries {
		if e.Seq <= n.log.LastSeq() {
			if t, _ := n.log.TermAt(e.Seq); t == e.Term {
				continue
			}
			resp.NeedSnapshot = true
			return resp, nil
		}
		if err := n.apply(e); err != nil {
			return resp, err
		}
	}
	if n.log.LastSeq() > req.LeaderLastSeq {
		// Turime įrašų, kurių lyderis neturi (likę iš senesnio term'o).
		resp.NeedSnapshot = true
		return resp, nil
	}
	resp.Success, resp.MatchSeq = true, req.PrevSeq+uint64(len(req.Entries))
	return resp, nil
}

func (n *Node) onSnapshot(req snapshotRequest) (appendResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.acceptLeader(req.Term, req.Leader) {
		return appendResponse{Term: n.term}, nil
	}
	log.Printf("[%s] diegia snapshot'ą iš %s (seq %d, %d raktų)", n.cfg.ID, req.Leader, req.Seq, len(req.Items))
	if err := n.log.Reset(0, 0); err != nil {
		return appendResponse{}, err
	}
	if err := n.lsm.Clear(); err != nil {
		return appendResponse{}, err
	}
	batch := make([]lsm.Entry, 0, len(req.Items)+1)
	for _, kv := range req.Items {
		batch = append(batch, lsm.Entry{Key: []byte(kv.Key), Value: kv.Value})
	}
	meta, _ := json.Marshal(oplog.Entry{Seq: req.Seq, Term: req.LastTerm})
	batch = append(batch, lsm.Entry{Key: []byte(metaApplied), Value: meta})
	if err := n.lsm.WriteBatch(batch); err != nil {
		return appendResponse{}, err
	}
	if err := n.log.Reset(req.Seq, req.LastTerm); err != nil {
		return appendResponse{}, err
	}
	n.lastContact = time.Now()
	return appendResponse{Term: n.term, Success: true, MatchSeq: req.Seq}, nil
}
