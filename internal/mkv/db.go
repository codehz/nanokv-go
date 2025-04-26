package mkv

import (
	"bytes"
	"errors"
	"os"
	"sync"

	"modernc.org/lldb"
)

type DB struct {
	*KV
	wal    *os.File
	filer  lldb.Filer
	store  *lldb.Allocator
	mtx    *sync.RWMutex
	kvs    map[string]*KV
	update bool
}

type NoSyncFiler struct {
	lldb.SimpleFileFiler
}

func (f *NoSyncFiler) Sync() error { return nil }

func OpenDB(path string) (*DB, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	wal, err := os.OpenFile(path+".wal", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		file.Close()
		return nil, err
	}
	filer := &NoSyncFiler{SimpleFileFiler: *lldb.NewSimpleFileFiler(file)}
	acid, err := lldb.NewACIDFiler(filer, wal)
	if err != nil {
		file.Close()
		wal.Close()
		return nil, err
	}
	err = acid.BeginUpdate()
	if err != nil {
		file.Close()
		wal.Close()
		return nil, err
	}
	store, err := lldb.NewAllocator(acid, &lldb.Options{})
	if err != nil {
		file.Close()
		wal.Close()
		return nil, err
	}
	root, err := openOrCreateRootBTree(store)
	if err != nil {
		file.Close()
		wal.Close()
		return nil, err
	}
	err = acid.EndUpdate()
	if err != nil {
		file.Close()
		wal.Close()
		return nil, err
	}
	result := &DB{wal: wal, filer: acid, store: store, mtx: &sync.RWMutex{}, kvs: map[string]*KV{}, update: false}
	result.KV = &KV{tree: root, db: result}
	return result, nil
}

func (db *DB) Close() {
	if err := db.filer.Sync(); err != nil {
		panic(err)
	}
	if err := db.filer.Close(); err != nil {
		panic(err)
	}
	if err := db.wal.Close(); err != nil {
		panic(err)
	}
}

func (db *DB) begin() {
	if db.update {
		panic(errors.New("invalid update"))
	}
	if err := db.filer.BeginUpdate(); err != nil {
		panic(err)
	}
	db.update = true
}

func (db *DB) end() {
	if !db.update {
		return
	}
	if err := db.filer.EndUpdate(); err != nil {
		panic(err)
	}
	db.update = false
}

func (db *DB) rollback() {
	if !db.update {
		return
	}
	if err := db.filer.Rollback(); err != nil {
		panic(err)
	}
	db.update = false
}

func openOrCreateRootBTree(store *lldb.Allocator) (tree *lldb.BTree, err error) {
	tree, err = lldb.OpenBTree(store, bytes.Compare, 1)
	if err != nil {
		tree, _, err = lldb.CreateBTree(store, bytes.Compare)
	}
	return
}
