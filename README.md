# nosql-project
Duomenų bazės projektas Nereliacinių duomenų bazių dalykui.

KVDB – replikuota rakto-reikšmės duomenų bazė, parašyta Go (tik standartinė biblioteka, be išorinių priklausomybių).

| Reikalavimas | Kaip įgyvendinta |
|---|---|
| Saugojimas – LSM medis | [internal/lsm](internal/lsm): WAL → memtable → SSTable failai (retas indeksas + Bloom filtras) → kompaktavimas |
| Išskirstymas – replikacija | [internal/node](internal/node): vienas pirminis mazgas (lyderis) + sekėjai; lyderis siunčia operacijų žurnalą ([internal/oplog](internal/oplog)) |
| Skaitymas iš bet kurios replikos | `get`, `traverse`, `continue` aptarnaujami lokaliai bet kuriame mazge |
| Praradus pirminį – išrenkamas naujas | Raft tipo rinkimai: term'ai, heartbeat'ai, atsitiktinis rinkimų laikas, balsavimas daugumos principu |
| Eventual consistency | sekėjai taiko žurnalą asinchroniškai; atsilikę pasiveja, išsiskyrę gauna snapshot'ą |
| Rašymo patikimumas `w` | `1`, `2`, `majority`, `all` – lyderis atsako tik gavęs tiek patvirtinimų |
| Operacijos | `get`, `delete`, `put`, `putAll`, `traverse`, `continue` |

## Kompiliavimas ir paleidimas

Reikia Go 1.22+.

```bash
go build -o kvdb.exe ./cmd/kvdb
./kvdb.exe cluster -nodes 3 -base-port 7001 -data data
```

Arba kiekvieną mazgą atskirame terminale (patogu demonstruojant lyderio praradimą – tiesiog Ctrl+C lyderio lange):

```bash
./kvdb.exe node -id n1 -addr 127.0.0.1:7001 -peers n2=127.0.0.1:7002,n3=127.0.0.1:7003 -data data/n1
./kvdb.exe node -id n2 -addr 127.0.0.1:7002 -peers n1=127.0.0.1:7001,n3=127.0.0.1:7003 -data data/n2
./kvdb.exe node -id n3 -addr 127.0.0.1:7003 -peers n1=127.0.0.1:7001,n2=127.0.0.1:7002 -data data/n3
```

## Klientas

Komandinė eilutė (numatytai jungiasi prie 7001–7003, keičiama `-nodes`):

```bash
./kvdb.exe status
./kvdb.exe put vardas '"Jonas"' -w majority
./kvdb.exe putall a=1 b=2 'c={"x":3}' -w all
./kvdb.exe get vardas -node 127.0.0.1:7003     # skaityti iš konkrečios replikos
./kvdb.exe delete a -w 2                       # išveda ištrintą reikšmę
./kvdb.exe traverse a -page-size 2
```

Go biblioteka ([client](client)):

```go
db := client.New([]string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"})
db.Put("a", map[string]int{"x": 1}, client.Majority)
db.PutAll([]client.KV{{Key: "b", Value: json.RawMessage(`2`)}}, client.All)
value, found, err := db.Get("a", "")        // "" – atsitiktinė replika
old, found, err := db.Delete("b", client.W1)
cur, err := db.Traverse("a", 10, "")
for page, _ := db.Continue(cur); len(page) > 0; page, _ = db.Continue(cur) {
    // page: []client.KV{Key, Value}
}
```

Raktai – eilutės, reikšmės – bet kokia JSON reikšmė.

HTTP API (JSON): `GET /kv/get?key=`, `POST /kv/put {key, value, w}`, `POST /kv/putAll {items: [[k, v], ...], w}`,
`POST /kv/delete {key, w}`, `GET /kv/traverse?key=&pageSize=`, `GET /kv/continue?cursor=`, `GET /status`.

## Architektūra

### LSM medis ([internal/lsm](internal/lsm))
1. **WAL** – kiekvienas rašymų paketas įrašomas į `wal.log` su CRC32; po gedimo iš jo atkuriamas memtable.
2. **Memtable** – žodynas + surikiuotas raktų sąrašas. Ištrynimas – „antkapis“.
3. **Flush** – memtable viršijus ribą (numatyta 1 MB), išrašomas į nekeičiamą `.sst` failą:
   `[įrašai][retas indeksas – kas 16 raktas][Bloom filtras][footer]`. `MANIFEST.json` atnaujinamas atomiškai.
4. **Skaitymas** – memtable, tada SSTable nuo naujausio; Bloom filtras leidžia praleisti failus be rakto.
5. **Kompaktavimas** – kai SSTable failų daugiau nei 4, visi sujungiami į vieną (k-krypčių sąlaja per heap),
   paliekama naujausia kiekvieno rakto versija, antkapiai išmetami.
6. **Diapazono skaitymas** (`Scan`) – memtable ir visų SSTable sąlaja nuo nurodyto rakto.

### Replikacija ir rinkimai ([internal/node](internal/node))
- Rašymą gali gauti bet kuris mazgas – sekėjas jį persiunčia lyderiui.
- Lyderis priskiria operacijai `seq` numerį, įrašo į savo `oplog` ir LSM, o atskiros gorutinos kiekvienam
  sekėjui siunčia `/raft/append` (tuščias append = heartbeat).
- `w` nusako, kiek replikų (įskaitant lyderį) turi patvirtinti įrašą. Jei per `-write-timeout`
  nesurenkama, klientas gauna 504 – įrašas lieka lyderyje ir bus replikuotas vėliau.
- Sekėjas, negavęs heartbeat'o per atsitiktinį 1–2 s laiką, padidina `term` ir prašo balsų.
  Balsas atiduodamas tik kandidatui, kurio žurnalas `(last_term, last_seq)` ne senesnis – todėl
  naujasis lyderis turi visus daugumos patvirtintus įrašus. Lyderiu tampa gavęs daugumą balsų.
- Atsilikęs sekėjas gauna trūkstamus įrašus; jei jie jau nukirpti iš žurnalo arba sekėjo žurnalas
  išsiskyrė (pvz. buvęs lyderis su `w=1` įrašu, kurio niekas negavo), lyderis siunčia pilną snapshot'ą.
- `traverse` grąžina bebūsenį kursorių (paskutinis raktas + puslapio dydis), todėl `continue` gali būti
  vykdomas bet kurioje replikoje.

### Garantijos ir ribojimai
- `w=majority`/`all` patvirtinti įrašai išlieka pasikeitus lyderiui. `w=1` (ir `w=2`, kai mazgų > 3)
  įrašai gali būti prarasti, jei lyderis žūsta prieš juos išsiuntęs – tai eventual consistency kaina.
- Skaitymas iš sekėjo gali grąžinti pasenusią reikšmę, bet visos replikos ilgainiui sutampa.
- Klasterio sudėtis statinė (nurodoma paleidžiant).

## Testai

```bash
go test ./...
```

`internal/lsm` – LSM variklis (flush, kompaktavimas, WAL atkūrimas, scan, atidarymas iš naujo).
`internal/node` – 3 mazgų klasteris: replikacija su `w=all`, `putAll`/`delete`, puslapiavimas,
lyderio praradimas ir naujo išrinkimas, `w=all` klaida trūkstant replikos, grįžusio mazgo pasivijimas,
išsiskyrusio mazgo sutapatinimas per snapshot'ą.
