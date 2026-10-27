// Package client – KVDB kliento biblioteka.
//
//	db := client.New([]string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"})
//	db.Put("a", map[string]int{"x": 1}, client.Majority)
//	v, found, _ := db.Get("a", "")
//	cur, _ := db.Traverse("a", 10, "")
//	for page, _ := db.Continue(cur); len(page) > 0; page, _ = db.Continue(cur) { ... }
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"
)

// W – rašymo patikimumas: kiek replikų turi patvirtinti įrašą.
type W string

const (
	W1       W = "1"
	W2       W = "2"
	Majority W = "majority"
	All      W = "all"
)

type KV struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

// Error – serverio grąžinta klaida.
type Error struct {
	Status  int
	Message string
	Retry   bool
}

func (e *Error) Error() string { return fmt.Sprintf("%d: %s", e.Status, e.Message) }

// Cursor – kliento pusės kursorius. Serveris bebūsenis, todėl tęsti galima bet kurioje replikoje.
type Cursor struct {
	Token string
	Node  string
	Done  bool
}

type Client struct {
	mu       sync.Mutex
	nodes    []string
	http     *http.Client
	Timeout  time.Duration
	RetryFor time.Duration // kiek laiko kartoti rašymą (pvz. vykstant lyderio rinkimams)
}

func New(nodes []string) *Client {
	return &Client{
		nodes:    slices.Clone(nodes),
		http:     &http.Client{Transport: &http.Transport{Proxy: nil}},
		Timeout:  10 * time.Second,
		RetryFor: 10 * time.Second,
	}
}

func (c *Client) Nodes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.nodes)
}

func (c *Client) do(method, addr, path string, payload any, out any) error {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, "http://"+addr+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
			Retry bool   `json:"retry"`
		}
		json.Unmarshal(raw, &e)
		return &Error{resp.StatusCode, e.Error, e.Retry}
	}
	return json.Unmarshal(raw, out)
}

// call bando mazgus iš eilės; ryšio klaidos ir „kartokite“ atsakymai pereina prie kito mazgo.
func (c *Client) call(method, path string, payload, out any, nodes []string, retry bool) (string, error) {
	deadline := time.Now()
	if retry {
		deadline = deadline.Add(c.RetryFor)
	}
	var lastErr error
	for {
		for _, addr := range nodes {
			err := c.do(method, addr, path, payload, out)
			var se *Error
			if err == nil {
				return addr, nil
			} else if errors.As(err, &se) && !se.Retry {
				return addr, err
			}
			lastErr = err
		}
		if time.Now().After(deadline) {
			var se *Error
			if errors.As(lastErr, &se) {
				return "", lastErr
			}
			return "", fmt.Errorf("nepavyko pasiekti nė vieno mazgo: %w", lastErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (c *Client) read(path string, params url.Values, out any, node string) (string, error) {
	nodes := c.Nodes()
	rand.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
	if node != "" { // pageidaujamas mazgas pirmas, kiti – atsarginiai
		nodes = slices.DeleteFunc(nodes, func(n string) bool { return n == node })
		nodes = append([]string{node}, nodes...)
	}
	return c.call(http.MethodGet, path+"?"+params.Encode(), nil, out, nodes, false)
}

func (c *Client) write(path string, payload, out any) error {
	addr, err := c.call(http.MethodPost, path, payload, out, c.Nodes(), true)
	if addr != "" {
		// Paskutinis atsakęs mazgas bandomas pirmas – nelaukiame neveikiančių.
		c.mu.Lock()
		c.nodes = slices.DeleteFunc(c.nodes, func(n string) bool { return n == addr })
		c.nodes = append([]string{addr}, c.nodes...)
		c.mu.Unlock()
	}
	return err
}

// ---------- API ----------

// Get grąžina reikšmę (JSON) ir ar raktas rastas. node – iš kurios replikos skaityti ("" – atsitiktinė).
func (c *Client) Get(key, node string) (json.RawMessage, bool, error) {
	var out struct {
		Found bool            `json:"found"`
		Value json.RawMessage `json:"value"`
	}
	_, err := c.read("/kv/get", url.Values{"key": {key}}, &out, node)
	return out.Value, out.Found, err
}

// Put įrašo reikšmę (bet koks JSON serializuojamas Go objektas arba json.RawMessage).
func (c *Client) Put(key string, value any, w W) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.write("/kv/put", map[string]any{"key": key, "value": json.RawMessage(raw), "w": w}, &struct{}{})
}

func (c *Client) PutAll(items []KV, w W) error {
	pairs := make([][2]any, len(items))
	for i, kv := range items {
		pairs[i] = [2]any{kv.Key, kv.Value}
	}
	return c.write("/kv/putAll", map[string]any{"items": pairs, "w": w}, &struct{}{})
}

// Delete ištrina raktą ir grąžina buvusią reikšmę bei ar raktas egzistavo.
func (c *Client) Delete(key string, w W) (json.RawMessage, bool, error) {
	var out struct {
		Found bool            `json:"found"`
		Value json.RawMessage `json:"value"`
	}
	err := c.write("/kv/delete", map[string]any{"key": key, "w": w}, &out)
	return out.Value, out.Found, err
}

// Traverse sukuria kursorių, kuris eina per raktus nuo key (imtinai) pageSize dydžio puslapiais.
func (c *Client) Traverse(key string, pageSize int, node string) (*Cursor, error) {
	var out struct {
		Cursor string `json:"cursor"`
	}
	addr, err := c.read("/kv/traverse", url.Values{"key": {key}, "pageSize": {strconv.Itoa(pageSize)}}, &out, node)
	if err != nil {
		return nil, err
	}
	return &Cursor{Token: out.Cursor, Node: addr}, nil
}

// Continue grąžina kitą puslapį ir pastumia kursorių. Tuščias puslapis – duomenys baigėsi.
func (c *Client) Continue(cur *Cursor) ([]KV, error) {
	if cur.Done {
		return nil, nil
	}
	var out struct {
		Values []KV    `json:"values"`
		Cursor *string `json:"cursor"`
	}
	addr, err := c.read("/kv/continue", url.Values{"cursor": {cur.Token}}, &out, cur.Node)
	if err != nil {
		return nil, err
	}
	cur.Node = addr
	if out.Cursor == nil {
		cur.Done = true
	} else {
		cur.Token = *out.Cursor
	}
	return out.Values, nil
}

type Status struct {
	ID      string `json:"id"`
	Role    string `json:"role"`
	Term    uint64 `json:"term"`
	Leader  string `json:"leader"`
	LastSeq uint64 `json:"last_seq"`
	LSM     struct {
		MemtableKeys int `json:"memtable_keys"`
		SSTables     int `json:"sstables"`
	} `json:"lsm"`
}

func (c *Client) Status(node string) (Status, error) {
	var s Status
	err := c.do(http.MethodGet, node, "/status", nil, &s)
	return s, err
}
