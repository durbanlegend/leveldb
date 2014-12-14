package main

import (
	"fmt"

	"code.google.com/p/leveldb-go/leveldb/db"
	"code.google.com/p/leveldb-go/leveldb/table"
)

type kv struct {
	K []byte
	V []byte
}

type kvs struct {
	items map[int]kv
}

func (p *kv) PutKV(k []byte, v []byte) {
	p.K = k
	p.V = v
}

func (items *kvs) PutKVs() {
	fmt.Println(items)
}

func (p *kv) GetKV() (key []byte, value []byte) {
	key = p.K
	value = p.V
	return
}

func Check(e error) {
	if e != nil {
		panic(e)
	}
}

func p(r []byte, e error) error {
	if e != nil {
		return e
	}
	println(string(r))
	return nil
}

const (
	DBFILE = "/tmp/leveldb2.db"
)

var DBFS = db.DefaultFileSystem

func ex_main() {
	Connection, e := DBFS.Create(DBFILE)
	//fmt.Println(runtime.Caller(1))
	Check(e)
	w := table.NewWriter(Connection, nil)
	defer w.Close()

	e = w.Set([]byte("1"), []byte("red"), nil)
	Check(e)
	e = w.Set([]byte("2"), []byte("yellow"), nil)
	Check(e)
	e = w.Set([]byte("3"), []byte("blue"), nil)
	Check(e)
	e = w.Close()
	Check(e)
	w = nil

	Connection, e = DBFS.Open(DBFILE)
	Check(e)
	count(Connection)

	fmt.Println("Printing # KV")
	Connection, e = DBFS.Open(DBFILE)
	Check(e)
	itemsKV := readByte(Connection)
	for k, v := range itemsKV {
		fmt.Printf("k: %v v: %v\n", k, v)
	}
	fmt.Println("Done Printing # KV")

	Connection, e = DBFS.Open(DBFILE)
	Check(e)
	stringsKV := read(Connection)
	for k, v := range stringsKV {
		fmt.Printf("k: %v v: %v\n", k, v)
	}
	fmt.Println("Done Printing # KV as map of strings")

	Connection, e = DBFS.Create(DBFILE)
	Check(e)
	w = table.NewWriter(Connection, nil)
	defer w.Close()
	e = w.Set([]byte("4"), []byte("green"), nil)
	Check(e)
	e = w.Set([]byte("5"), []byte("white"), nil)
	Check(e)
	e = w.Set([]byte("6"), []byte("black"), nil)
	Check(e)
	e = w.Close()
	Check(e)

	Connection, e = DBFS.Open(DBFILE)
	Check(e)
	count(Connection)

	fmt.Println("Printing # KV (2)")
	Connection, e = DBFS.Open(DBFILE)
	Check(e)
	itemsKV = readByte(Connection)
	for k, v := range itemsKV {
		fmt.Printf("k: %v v: %v\n", k, v)
	}
	fmt.Println("Done Printing # KV (2)")

	Connection, e = DBFS.Open(DBFILE)
	Check(e)
	stringsKV = read(Connection)
	for k, v := range stringsKV {
		fmt.Printf("k: %v v: %v\n", k, v)
	}
	fmt.Println("Done Printing # KV (2) as map of strings")

	v := findOne([]byte("5"))
	fmt.Printf("findOne v: %s\n", v)

}

func count(Connection db.File) {
	b := []byte("0")
	r := table.NewReader(Connection, nil)

	println("\n\n###### Counting ###### ")

	iter, n := r.Find(b, nil), 0
	for iter.Next() {
		n++
		println("Count # ", n)
	}

	e := r.Close()
	Check(e)
	println("#####Total: ", n)
}

// Donf: this returns by value
func read(Connection db.File) map[int64]string {
	//Connection, e := DBFS.Open(DBFILE)
	//Check(e)
	b := []byte("0")
	r := table.NewReader(Connection, nil)

	items := map[int64]string{}
	iter, _ := r.Find(b, nil), 0
	for iter.Next() {
		k := iter.Key()
		fmt.Printf("k: %v\n", k)
		v := iter.Value()
		items[int64(k[0])] = string(v)
	}

	e := r.Close()
	Check(e)
	return items
}

// Donf: this returns a map of pointers to the underlying byte arrays, so should
// be much more efficient than read(), but more vulnerable to race conditions.
func readByte(Connection db.File) map[int]kv {
	c := 0
	b := []byte("0")
	r := table.NewReader(Connection, nil)

	//items := map[int64]kv{}
	item := new(kv)
	items := map[int]kv{}
	iter, _ := r.Find(b, nil), 0
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		item.PutKV(k, v)
		items[c] = *item
		c++
	}

	e := r.Close()
	Check(e)
	return items
}

func findOne(k []byte) []byte {
	Connection, e := DBFS.Open(DBFILE)
	Check(e)
	//b := []byte("0")
	r := table.NewReader(Connection, nil)

	v, e := r.Get(k, nil)
	Check(e)

	e = r.Close()
	Check(e)
	return v
}
