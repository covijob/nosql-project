package node

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/covijob/nosql-project/internal/oplog"
)

const maxPageSize = 10_000

type KV struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type apiError struct {
	status int
	msg    string
	retry  bool
	extra  map[string]any
}

func (e *apiError) Error() string { return e.msg }

func errf(status int, format string, args ...any) *apiError {
	return &apiError{status: status, msg: fmt.Sprintf(format, args...)}
}

func retryable(msg string) *apiError {
	return &apiError{status: http.StatusServiceUnavailable, msg: msg, retry: true}
}

func validKey(key string, allowEmpty bool) error {
	if key == "" && !allowEmpty {
		return errf(400, "'key' turi būti netuščia eilutė")
	}
	if strings.HasPrefix(key, "\x00") {
		return errf(400, "raktai, prasidedantys \\x00, rezervuoti sistemai")
	}
	return nil
}

// ---------- vidinis RPC ----------

func (n *Node) callPeer(peer, path string, req, resp any, timeout time.Duration) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	status, raw, err := n.post(n.cfg.Peers[peer], path, body, nil, timeout)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("%s%s: HTTP %d", peer, path, status)
	}
	return json.Unmarshal(raw, resp)
}

func (n *Node) post(addr, path string, body []byte, header http.Header, timeout time.Duration) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	return resp.StatusCode, raw, err
}

// ---------- kliento operacijos ----------

// parseW paverčia w (1, 2, "majority", "all") reikalaujamu patvirtinimų skaičiumi.
func (n *Node) parseW(raw json.RawMessage) (int, error) {
	if len(raw) == 0 {
		return 1, nil
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		s = string(raw)
	}
	switch s {
	case "all":
		return n.clusterSize(), nil
	case "majority":
		return n.majority(), nil
	}
	w, err := strconv.Atoi(s)
	if err != nil || w < 1 || w > n.clusterSize() {
		return 0, errf(400, "w turi būti 1..%d, 'majority' arba 'all'", n.clusterSize())
	}
	return w, nil
}

type writeResult struct {
	Seq   uint64          `json:"seq"`
	Acks  int             `json:"acks"`
	Found bool            `json:"found,omitempty"`
	Old   json.RawMessage `json:"-"`
}

// write įvykdo rašymą lyderyje ir laukia `needed` patvirtinimų.
func (n *Node) write(ops []oplog.Op, needed int, oldKey string) (writeResult, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return writeResult{}, retryable("mazgas nebėra lyderis, kartokite")
	}
	var res writeResult
	if oldKey != "" {
		old, ok, err := n.lsm.Get([]byte(oldKey))
		if err != nil {
			return res, err
		}
		res.Old, res.Found = old, ok
	}
	term, seq := n.term, n.log.LastSeq()+1
	if err := n.apply(oplog.Entry{Seq: seq, Term: term, Ops: ops}); err != nil {
		return res, err
	}
	n.notify()
	n.waitFor(func() bool {
		return n.role != Leader || n.term != term || n.acks(seq) >= needed
	}, n.cfg.WriteTimeout)
	res.Seq, res.Acks = seq, 1
	if n.role == Leader {
		res.Acks = n.acks(seq)
	}
	if res.Acks < needed {
		return res, &apiError{status: http.StatusGatewayTimeout,
			msg: fmt.Sprintf("įrašą patvirtino %d iš reikalaujamų %d replikų; jis gali būti pritaikytas vėliau",
				res.Acks, needed),
			extra: map[string]any{"seq": seq, "acks": res.Acks}}
	}
	return res, nil
}

type cursor struct {
	Key       string `json:"k"`
	Inclusive bool   `json:"inc"`
	PageSize  int    `json:"n"`
}

func encodeCursor(c cursor) string {
	raw, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeCursor(token string) (cursor, error) {
	var c cursor
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || json.Unmarshal(raw, &c) != nil || c.PageSize < 1 || c.PageSize > maxPageSize {
		return c, errf(400, "neteisingas kursorius")
	}
	return c, nil
}

type Status struct {
	ID         string                  `json:"id"`
	Addr       string                  `json:"addr"`
	Role       Role                    `json:"role"`
	Term       uint64                  `json:"term"`
	Leader     string                  `json:"leader"`
	LeaderAddr string                  `json:"leader_addr"`
	LastSeq    uint64                  `json:"last_seq"`
	Replicas   map[string]ReplicaState `json:"replicas,omitempty"`
	LSM        any                     `json:"lsm"`
}

type ReplicaState struct {
	Addr     string `json:"addr"`
	MatchSeq uint64 `json:"match_seq"`
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := Status{ID: n.cfg.ID, Addr: n.cfg.Addr, Role: n.role, Term: n.term, Leader: n.leaderID,
		LeaderAddr: n.cfg.Peers[n.leaderID], LastSeq: n.log.LastSeq(), LSM: n.lsm.Stats()}
	if n.leaderID == n.cfg.ID {
		s.LeaderAddr = n.cfg.Addr
	}
	if n.role == Leader {
		s.Replicas = map[string]ReplicaState{}
		for p, a := range n.cfg.Peers {
			s.Replicas[p] = ReplicaState{a, n.matchSeq[p]}
		}
	}
	return s
}

// ---------- HTTP maršrutai ----------

func (n *Node) routes() http.Handler {
	mux := http.NewServeMux()
	handle := func(pattern string, h func(r *http.Request) (any, error)) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			resp, err := h(r)
			if err != nil {
				writeError(w, n.cfg.ID, err)
				return
			}
			if raw, ok := resp.(rawResponse); ok { // persiųsto rašymo atsakymas
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(raw.status)
				w.Write(raw.body)
				return
			}
			writeJSON(w, http.StatusOK, resp)
		})
	}

	handle("POST /raft/vote", func(r *http.Request) (any, error) {
		var req voteRequest
		if err := decodeBody(r, &req); err != nil {
			return nil, err
		}
		return n.onVote(req), nil
	})
	handle("POST /raft/append", func(r *http.Request) (any, error) {
		var req appendRequest
		if err := decodeBody(r, &req); err != nil {
			return nil, err
		}
		return n.onAppend(req)
	})
	handle("POST /raft/snapshot", func(r *http.Request) (any, error) {
		var req snapshotRequest
		if err := decodeBody(r, &req); err != nil {
			return nil, err
		}
		return n.onSnapshot(req)
	})
	handle("GET /status", func(r *http.Request) (any, error) { return n.Status(), nil })

	handle("GET /kv/get", func(r *http.Request) (any, error) {
		key := r.URL.Query().Get("key")
		if err := validKey(key, false); err != nil {
			return nil, err
		}
		value, found, err := n.lsm.Get([]byte(key))
		if err != nil {
			return nil, err
		}
		if !found {
			value = json.RawMessage("null")
		}
		return map[string]any{"key": key, "found": found, "value": json.RawMessage(value)}, nil
	})
	handle("GET /kv/traverse", func(r *http.Request) (any, error) {
		q := r.URL.Query()
		key := q.Get("key")
		if err := validKey(key, true); err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(q.Get("pageSize"))
		if err != nil || size < 1 || size > maxPageSize {
			return nil, errf(400, "pageSize turi būti 1..%d", maxPageSize)
		}
		return map[string]string{"cursor": encodeCursor(cursor{key, true, size})}, nil
	})
	handle("GET /kv/continue", func(r *http.Request) (any, error) {
		c, err := decodeCursor(r.URL.Query().Get("cursor"))
		if err != nil {
			return nil, err
		}
		rows, err := n.lsm.Scan([]byte(c.Key), c.PageSize, c.Inclusive)
		if err != nil {
			return nil, err
		}
		values := make([]KV, len(rows))
		for i, e := range rows {
			values[i] = KV{string(e.Key), e.Value}
		}
		var next *string
		if len(rows) == c.PageSize {
			s := encodeCursor(cursor{values[len(values)-1].Key, false, c.PageSize})
			next = &s
		}
		return map[string]any{"values": values, "cursor": next}, nil
	})

	handle("POST /kv/put", n.writeHandler(func(body writeBody) ([]oplog.Op, string, error) {
		if err := validKey(body.Key, false); err != nil {
			return nil, "", err
		}
		if len(body.Value) == 0 {
			return nil, "", errf(400, "trūksta 'value'")
		}
		return []oplog.Op{{Key: body.Key, Value: body.Value}}, "", nil
	}))
	handle("POST /kv/putAll", n.writeHandler(func(body writeBody) ([]oplog.Op, string, error) {
		if len(body.Items) == 0 {
			return nil, "", errf(400, "'items' turi būti netuščias [[raktas, reikšmė], ...] sąrašas")
		}
		ops := make([]oplog.Op, len(body.Items))
		for i, item := range body.Items {
			var key string
			if len(item) != 2 || json.Unmarshal(item[0], &key) != nil {
				return nil, "", errf(400, "kiekvienas 'items' elementas turi būti [raktas, reikšmė]")
			}
			if err := validKey(key, false); err != nil {
				return nil, "", err
			}
			ops[i] = oplog.Op{Key: key, Value: item[1]}
		}
		return ops, "", nil
	}))
	handle("POST /kv/delete", n.writeHandler(func(body writeBody) ([]oplog.Op, string, error) {
		if err := validKey(body.Key, false); err != nil {
			return nil, "", err
		}
		return []oplog.Op{{Key: body.Key, Del: true}}, body.Key, nil
	}))
	return mux
}

type writeBody struct {
	Key   string              `json:"key"`
	Value json.RawMessage     `json:"value"`
	Items [][]json.RawMessage `json:"items"`
	W     json.RawMessage     `json:"w"`
}

type rawResponse struct {
	status int
	body   []byte
}

// writeHandler: lyderis vykdo rašymą, sekėjas jį persiunčia lyderiui –
// todėl klientas gali rašyti į bet kurį mazgą.
func (n *Node) writeHandler(build func(writeBody) ([]oplog.Op, string, error)) func(*http.Request) (any, error) {
	return func(r *http.Request) (any, error) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		n.mu.Lock()
		role, leader := n.role, n.leaderID
		n.mu.Unlock()
		if role != Leader {
			addr, ok := n.cfg.Peers[leader]
			if r.Header.Get("X-KVDB-Forwarded") == "1" || !ok {
				return nil, retryable("lyderis nežinomas (vyksta rinkimai?), kartokite")
			}
			h := http.Header{"X-Kvdb-Forwarded": {"1"}}
			status, body, err := n.post(addr, r.URL.Path, raw, h, n.cfg.WriteTimeout+2*time.Second)
			if err != nil {
				return nil, retryable("lyderis nepasiekiamas, kartokite")
			}
			return rawResponse{status, body}, nil
		}
		var body writeBody
		if err := json.Unmarshal(raw, &body); err != nil {
			return nil, errf(400, "neteisingas JSON: %v", err)
		}
		needed, err := n.parseW(body.W)
		if err != nil {
			return nil, err
		}
		ops, oldKey, err := build(body)
		if err != nil {
			return nil, err
		}
		res, err := n.write(ops, needed, oldKey)
		if err != nil {
			return nil, err
		}
		out := map[string]any{"ok": true, "seq": res.Seq, "acks": res.Acks}
		if oldKey != "" {
			out["key"], out["found"], out["value"] = oldKey, res.Found, json.RawMessage("null")
			if res.Found {
				out["value"] = res.Old
			}
		}
		if len(body.Items) > 0 {
			out["count"] = len(ops)
		}
		return out, nil
	}
}

func decodeBody(r *http.Request, v any) error {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		return errf(400, "neteisingas JSON: %v", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, nodeID string, err error) {
	var ae *apiError
	if !errors.As(err, &ae) {
		log.Printf("[%s] vidinė klaida: %v", nodeID, err)
		ae = &apiError{status: http.StatusInternalServerError, msg: err.Error()}
	}
	body := map[string]any{"error": ae.msg}
	if ae.retry {
		body["retry"] = true
	}
	for k, v := range ae.extra {
		body[k] = v
	}
	writeJSON(w, ae.status, body)
}
