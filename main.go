// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"

	"code.google.com/p/leveldb-go/leveldb/db"
	"code.google.com/p/leveldb-go/leveldb/memfs"
	//"code.google.com/p/leveldb-go/leveldb/memfs"
	"code.google.com/p/leveldb-go/leveldb/table"
)

var wordCount = map[string]string{}

const (
	DBFILE1 = "/tmp/leveldb3.db"
)

var DBFS1 = db.DefaultFileSystem

func init() {
	f, err := os.Open("./h.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		s, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		k := strings.TrimSpace(string(s[8:]))
		v := strings.TrimSpace(string(s[:8]))
		wordCount[k] = v
	}
	if len(wordCount) != 1710 {
		panic(fmt.Sprintf("h.txt entry count: got %d, want %d", len(wordCount), 1710))
	}
}

func check(f db.File) error {
	r := table.NewReader(f, &db.Options{
		VerifyChecksums: true,
	})
	// Check that each key/value pair in wordCount is also in the table.
	for k, v := range wordCount {
		// Check using Get.
		if v1, err := r.Get([]byte(k), nil); string(v1) != string(v) || err != nil {
			return fmt.Errorf("Get %q: got (%q, %v), want (%q, %v)", k, v1, err, v, error(nil))
		} else if len(v1) != cap(v1) {
			return fmt.Errorf("Get %q: len(v1)=%d, cap(v1)=%d", k, len(v1), cap(v1))
		} else {
			fmt.Printf("Get %q: successfully got (%q)\n", k, v1)
		}

		// Check using Find.
		i := r.Find([]byte(k), nil)
		if !i.Next() || string(i.Key()) != k {
			return fmt.Errorf("Find %q: key was not in the table", k)
		}
		if k1 := i.Key(); len(k1) != cap(k1) {
			return fmt.Errorf("Find %q: len(k1)=%d, cap(k1)=%d", k, len(k1), cap(k1))
		}
		if string(i.Value()) != v {
			return fmt.Errorf("Find %q: got value %q, want %q", k, i.Value(), v)
		}
		if v1 := i.Value(); len(v1) != cap(v1) {
			return fmt.Errorf("Find %q: len(v1)=%d, cap(v1)=%d", k, len(v1), cap(v1))
		}
		if err := i.Close(); err != nil {
			return err
		}
	}

	// Check that nonsense words are not in the table.
	var nonsenseWords = []string{
		"",
		"\x00",
		"kwyjibo",
		"\xff",
	}
	for _, s := range nonsenseWords {
		// Check using Get.
		if _, err := r.Get([]byte(s), nil); err != db.ErrNotFound {
			return fmt.Errorf("Get %q: got %v, want ErrNotFound", s, err)
		}

		// Check using Find.
		i := r.Find([]byte(s), nil)
		if i.Next() && s == string(i.Key()) {
			return fmt.Errorf("Find %q: unexpectedly found key in the table", s)
		}
		if err := i.Close(); err != nil {
			return err
		}
	}

	// Check that the number of keys >= a given start key matches the expected number.
	var countTests = []struct {
		count int
		start string
	}{
		// cat h.txt | cut -c 9- | wc -l gives 1710.
		{1710, ""},
		// cat h.txt | cut -c 9- | grep -v "^[a-b]" | wc -l gives 1522.
		{1522, "c"},
		// cat h.txt | cut -c 9- | grep -v "^[a-j]" | wc -l gives 940.
		{940, "k"},
		// cat h.txt | cut -c 9- | grep -v "^[a-x]" | wc -l gives 12.
		{12, "y"},
		// cat h.txt | cut -c 9- | grep -v "^[a-z]" | wc -l gives 0.
		{0, "~"},
	}
	for _, ct := range countTests {
		n, i := 0, r.Find([]byte(ct.start), nil)
		for i.Next() {
			n++
		}
		if err := i.Close(); err != nil {
			return err
		}
		if n != ct.count {
			return fmt.Errorf("count %q: got %d, want %d", ct.start, n, ct.count)
		}
	}

	return r.Close()
}

var (
	//memFileSystem = memfs.New()
	tmpFileCount int
)

func build(compression db.Compression) (db.File, error) {
	// Create a sorted list of wordCount's keys.
	keys := make([]string, len(wordCount))
	i := 0
	for k := range wordCount {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	// Write the key/value pairs to a new table, in increasing key order.
	//filename := fmt.Sprintf("/tmp%d", tmpFileCount)
	f0, err := DBFS1.Create(DBFILE1)
	//f0, err := memFileSystem.Create(filename)
	if err != nil {
		return nil, err
	}
	defer f0.Close()
	tmpFileCount++
	w := table.NewWriter(f0, &db.Options{
		Compression: compression,
	})
	for _, k := range keys {
		v := wordCount[k]
		if err := w.Set([]byte(k), []byte(v), nil); err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	// Re-open that filename for reading.
	//f1, err := memFileSystem.Open(filename)
	f1, err := DBFS1.Open(DBFILE1)
	if err != nil {
		return nil, err
	}
	return f1, nil
}

func main() {
	// Check that we can read a pre-made table.
	ff, err := os.Open("./h.sst")
	if err != nil {
		panic(err)
	}
	err = check(ff)
	if err != nil {
		panic(err)
	}

	// Check that we can read a freshly made table.
	f, err := build(db.DefaultCompression)
	if err != nil {
		panic(err)
	}
	err = check(f)
	if err != nil {
		panic(err)
	}

	// Check that a freshly made NoCompression table is byte-for-byte equal
	// to a pre-made table.
	a, err := ioutil.ReadFile("./h.no-compression.sst")
	if err != nil {
		panic(err)
	}
	f, err = build(db.NoCompression)
	if err != nil {
		panic(err)
	}
	stat, err := f.Stat()
	if err != nil {
		panic(err)
	}
	b := make([]byte, stat.Size())
	_, err = f.ReadAt(b, 0)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(a, b) {
		panic("built table does not match pre-made table")
	}

	const blockSize = 100
	keys := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	valueLengths := []int{0, 1, 22, 28, 33, 40, 50, 61, 87, 100, 143, 200}
	xxx := bytes.Repeat([]byte("x"), valueLengths[len(valueLengths)-1])

	for nk := 0; nk <= len(keys); nk++ {
	loop:
		for _, vLen := range valueLengths {
			got, memFS := 0, memfs.New()

			wf, err := memFS.Create("foo")
			if err != nil {
				fmt.Printf("nk=%d, vLen=%d: memFS create: %v\n", nk, vLen, err)
				continue
			}
			w := table.NewWriter(wf, &db.Options{
				BlockSize: blockSize,
			})
			for _, k := range keys[:nk] {
				if err := w.Set([]byte(k), xxx[:vLen], nil); err != nil {
					fmt.Printf("nk=%d, vLen=%d: set: %v\n", nk, vLen, err)
					continue loop
				}
			}
			if err := w.Close(); err != nil {
				fmt.Printf("nk=%d, vLen=%d: writer close: %v\n", nk, vLen, err)
				continue
			}

			rf, err := memFS.Open("foo")
			if err != nil {
				fmt.Printf("nk=%d, vLen=%d: memFS open: %v\n", nk, vLen, err)
				continue
			}
			r := table.NewReader(rf, nil)
			i := r.Find(nil, nil)
			for i.Next() {
				got++
			}
			if err := i.Close(); err != nil {
				fmt.Printf("nk=%d, vLen=%d: Iterator close: %v\n", nk, vLen, err)
				continue
			}
			if err := r.Close(); err != nil {
				fmt.Printf("nk=%d, vLen=%d: reader close: %v\n", nk, vLen, err)
				continue
			}

			if got != nk {
				fmt.Printf("nk=%2d, vLen=%3d: got %2d keys, want %2d", nk, vLen, got, nk)
				continue
			}
		}
	}
}
