// kvdb – vienas įrankis mazgui paleisti, lokaliam klasteriui paleisti ir kliento komandoms.
//
//	kvdb node -id n1 -addr 127.0.0.1:7001 -peers n2=127.0.0.1:7002,n3=127.0.0.1:7003 -data data/n1
//	kvdb cluster -nodes 3 -base-port 7001 -data data
//	kvdb put vardas '"Jonas"' -w majority
//	kvdb get vardas -node 127.0.0.1:7003
//	kvdb putall a=1 b=2 -w all
//	kvdb delete a
//	kvdb traverse a -page-size 10
//	kvdb status
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"time"

	"github.com/covijob/nosql-project/client"
	"github.com/covijob/nosql-project/internal/node"
)

const defaultNodes = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "node":
		err = runNode(args)
	case "cluster":
		err = runCluster(args)
	case "get", "put", "putall", "delete", "traverse", "status":
		err = runClient(cmd, args)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "Klaida:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `Naudojimas:
  kvdb node -id ID -addr HOST:PORT -peers ID=HOST:PORT,... -data DIR
  kvdb cluster [-nodes 3] [-base-port 7001] [-data data]
  kvdb get KEY [-node ADDR]
  kvdb put KEY VALUE [-w 1|2|majority|all]
  kvdb putall KEY=VALUE... [-w ...]
  kvdb delete KEY [-w ...]
  kvdb traverse [KEY] [-page-size 10] [-pages 0]
  kvdb status
Kliento komandos priima -nodes ADDR,ADDR,... (numatyta `+defaultNodes+`)`)
	os.Exit(2)
}

// parseArgs leidžia vėliavėles rašyti ir po pozicinių argumentų.
func parseArgs(fs *flag.FlagSet, args []string) []string {
	var positional []string
	for {
		fs.Parse(args)
		args = fs.Args()
		if len(args) == 0 {
			return positional
		}
		positional = append(positional, args[0])
		args = args[1:]
	}
}

func runNode(args []string) error {
	fs := flag.NewFlagSet("node", flag.ExitOnError)
	id := fs.String("id", "", "mazgo identifikatorius, pvz. n1")
	addr := fs.String("addr", "", "host:port, pvz. 127.0.0.1:7001")
	peers := fs.String("peers", "", "kiti mazgai: n2=127.0.0.1:7002,n3=127.0.0.1:7003")
	data := fs.String("data", "", "duomenų katalogas")
	heartbeat := fs.Duration("heartbeat", 300*time.Millisecond, "heartbeat intervalas")
	electionMin := fs.Duration("election-min", time.Second, "min. rinkimų laukimo laikas")
	electionMax := fs.Duration("election-max", 2*time.Second, "maks. rinkimų laukimo laikas")
	writeTimeout := fs.Duration("write-timeout", 5*time.Second, "kiek laukti w patvirtinimų")
	memtableKB := fs.Int("memtable-kb", 1024, "memtable dydis (KB) iki išrašymo į SSTable")
	parseArgs(fs, args)
	if *id == "" || *addr == "" || *data == "" {
		return fmt.Errorf("būtini -id, -addr ir -data")
	}
	peerMap := map[string]string{}
	for _, part := range strings.Split(*peers, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		pid, paddr, ok := strings.Cut(part, "=")
		if !ok {
			return fmt.Errorf("neteisingas -peers elementas %q", part)
		}
		peerMap[pid] = paddr
	}
	n, err := node.New(node.Config{ID: *id, Addr: *addr, Peers: peerMap, DataDir: *data,
		Heartbeat: *heartbeat, ElectionMin: *electionMin, ElectionMax: *electionMax,
		WriteTimeout: *writeTimeout, MemtableLimit: *memtableKB * 1024})
	if err != nil {
		return err
	}
	if err := n.Start(); err != nil {
		return err
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	n.Stop()
	return nil
}

func runCluster(args []string) error {
	fs := flag.NewFlagSet("cluster", flag.ExitOnError)
	count := fs.Int("nodes", 3, "mazgų skaičius")
	basePort := fs.Int("base-port", 7001, "pirmo mazgo prievadas")
	data := fs.String("data", "data", "duomenų katalogas")
	parseArgs(fs, args)

	self, err := os.Executable()
	if err != nil {
		return err
	}
	addrs := map[string]string{}
	var ids []string
	for i := range *count {
		id := fmt.Sprintf("n%d", i+1)
		ids = append(ids, id)
		addrs[id] = fmt.Sprintf("127.0.0.1:%d", *basePort+i)
	}
	var procs []*exec.Cmd
	for _, id := range ids {
		var peers []string
		for _, p := range ids {
			if p != id {
				peers = append(peers, p+"="+addrs[p])
			}
		}
		cmd := exec.Command(self, "node", "-id", id, "-addr", addrs[id],
			"-peers", strings.Join(peers, ","), "-data", *data+"/"+id)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Start(); err != nil {
			return err
		}
		procs = append(procs, cmd)
		log.Printf("paleistas %s (%s), PID %d", id, addrs[id], cmd.Process.Pid)
	}
	log.Print("Klasteris veikia. Atskiro mazgo nutraukimas (failover demonstracijai) kitų nestabdo. Ctrl+C – sustabdyti.")
	done := make(chan struct{})
	go func() {
		for _, p := range procs {
			p.Wait()
		}
		close(done)
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	select {
	case <-sig:
	case <-done:
	}
	for _, p := range procs {
		p.Process.Kill()
	}
	<-done
	return nil
}

// parseValue: galiojantis JSON paliekamas, kitaip reikšmė laikoma eilute.
func parseValue(s string) json.RawMessage {
	if json.Valid([]byte(s)) {
		return json.RawMessage(s)
	}
	raw, _ := json.Marshal(s)
	return raw
}

func runClient(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	nodes := fs.String("nodes", defaultNodes, "klasterio mazgai")
	readNode := fs.String("node", "", "skaityti iš konkretaus mazgo")
	w := fs.String("w", "1", "rašymo patikimumas: 1, 2, majority, all")
	pageSize := fs.Int("page-size", 10, "traverse puslapio dydis")
	pages := fs.Int("pages", 0, "kiek puslapių rodyti (0 = visus)")
	pos := parseArgs(fs, args)
	db := client.New(strings.Split(*nodes, ","))
	need := func(n int) error {
		if len(pos) < n {
			return fmt.Errorf("%s: trūksta argumentų", cmd)
		}
		return nil
	}

	switch cmd {
	case "get":
		if err := need(1); err != nil {
			return err
		}
		v, _, err := db.Get(pos[0], *readNode)
		if err != nil {
			return err
		}
		fmt.Println(string(v))
	case "put":
		if err := need(2); err != nil {
			return err
		}
		if err := db.Put(pos[0], parseValue(pos[1]), client.W(*w)); err != nil {
			return err
		}
		fmt.Println("OK")
	case "putall":
		if err := need(1); err != nil {
			return err
		}
		var items []client.KV
		for _, p := range pos {
			k, v, ok := strings.Cut(p, "=")
			if !ok {
				return fmt.Errorf("putall: laukta raktas=reikšmė, gauta %q", p)
			}
			items = append(items, client.KV{Key: k, Value: parseValue(v)})
		}
		if err := db.PutAll(items, client.W(*w)); err != nil {
			return err
		}
		fmt.Printf("OK (%d)\n", len(items))
	case "delete":
		if err := need(1); err != nil {
			return err
		}
		v, _, err := db.Delete(pos[0], client.W(*w))
		if err != nil {
			return err
		}
		fmt.Println(string(v))
	case "traverse":
		start := ""
		if len(pos) > 0 {
			start = pos[0]
		}
		cur, err := db.Traverse(start, *pageSize, *readNode)
		if err != nil {
			return err
		}
		for i := 1; *pages == 0 || i <= *pages; i++ {
			page, err := db.Continue(cur)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				break
			}
			fmt.Printf("--- puslapis %d (mazgas %s) ---\n", i, cur.Node)
			for _, kv := range page {
				fmt.Printf("%s = %s\n", kv.Key, kv.Value)
			}
		}
	case "status":
		for _, addr := range db.Nodes() {
			s, err := db.Status(addr)
			if err != nil {
				fmt.Printf("%s: NEPASIEKIAMAS\n", addr)
				continue
			}
			fmt.Printf("%s: %s %-9s term=%d leader=%s seq=%d sstables=%d\n",
				addr, s.ID, s.Role, s.Term, s.Leader, s.LastSeq, s.LSM.SSTables)
		}
	}
	return nil
}
