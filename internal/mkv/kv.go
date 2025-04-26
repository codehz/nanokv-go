package mkv

import (
	"bytes"
	"encoding/binary"
	"io"
	"iter"

	"modernc.org/lldb"
)

type KV struct {
	tree *lldb.BTree
	db   *DB
}

func (db *DB) NewKV() (*KV, int64) {
	tree, handle, err := lldb.CreateBTree(db.store, bytes.Compare)
	if err != nil {
		panic(err)
	}
	return &KV{tree: tree, db: db}, handle
}

func (db *DB) GetKV(handle int64) (result *KV) {
	tree, err := lldb.OpenBTree(db.store, bytes.Compare, handle)
	if err != nil {
		panic(err)
	}
	return &KV{tree: tree, db: db}
}

func (db *DB) EnsureKV(name string) (result *KV) {
	if cached, ok := db.kvs[name]; ok {
		return cached
	}
	var buffer []byte = make([]byte, 8)
	key, err := db.tree.Get(buffer, []byte(name))
	if err != nil {
		panic(err)
	}
	if key != nil {
		tree, err := lldb.OpenBTree(db.store, bytes.Compare, int64(binary.LittleEndian.Uint64(key)))
		if err != nil {
			panic(err)
		}
		result = &KV{tree: tree, db: db}
	} else {
		tree, handle, err := lldb.CreateBTree(db.store, bytes.Compare)
		if err != nil {
			panic(err)
		}
		if err = db.tree.Set([]byte(name), binary.LittleEndian.AppendUint64(nil, uint64(handle))); err != nil {
			panic(err)
		}
		result = &KV{tree: tree, db: db}
	}
	db.kvs[name] = result
	return
}

func (db *DB) RemoveKV(name string) {
	delete(db.kvs, name)
	var buffer []byte = make([]byte, 8)
	key, err := db.tree.Extract(buffer, []byte(name))
	if err != nil {
		panic(err)
	}
	if key == nil {
		return
	}
	db.RemoveKVByHandle(int64(binary.LittleEndian.Uint64(key)))
}

func (db *DB) RemoveKVByHandle(handle int64) {
	if err := lldb.RemoveBTree(db.store, handle); err != nil {
		panic(err)
	}
}

func (kv *KV) Get(buf []byte, key []byte) []byte {
	value, err := kv.tree.Get(buf, key)
	if err != nil {
		panic(err)
	}
	return value
}

func (kv *KV) Exists(key []byte) bool {
	_, hit, err := kv.tree.Seek(key)
	if err != nil {
		panic(err)
	}
	return hit
}

func (kv *KV) All(yield func([]byte, []byte) bool) {
	enum, err := kv.tree.SeekFirst()
	if err != nil {
		if err == io.EOF {
			return
		}
		panic(err)
	}
	for {
		key, value, err := enum.Next()
		if err != nil {
			if err == io.EOF {
				return
			}
			panic(err)
		}
		if !yield(key, value) {
			return
		}
	}
}

func (kv *KV) IterFrom(start []byte, reverse bool) iter.Seq2[[]byte, []byte] {
	if reverse {
		return func(yield func([]byte, []byte) bool) {
			enum, _, err := kv.tree.Seek(start)
			if err != nil {
				panic(err)
			}
			for {
				key, value, err := enum.Prev()
				if err != nil {
					if err == io.EOF {
						return
					}
					panic(err)
				}
				if !yield(key, value) {
					return
				}
			}
		}
	} else {
		return func(yield func([]byte, []byte) bool) {
			enum, _, err := kv.tree.Seek(start)
			if err != nil {
				panic(err)
			}
			for {
				key, value, err := enum.Next()
				if err != nil {
					if err == io.EOF {
						return
					}
					panic(err)
				}
				if !yield(key, value) {
					return
				}
			}
		}
	}
}

func (kv *KV) Iter(start []byte, end []byte, reverse bool) iter.Seq2[[]byte, []byte] {
	if reverse {
		return func(yield func([]byte, []byte) bool) {
			enum, skiphead, err := kv.tree.Seek(end)
			if err != nil {
				panic(err)
			}
			for {
				key, value, err := enum.Prev()
				if err != nil {
					if err == io.EOF {
						return
					}
					panic(err)
				}
				if skiphead {
					skiphead = false
					continue
				}
				if bytes.Compare(start, key) < 0 {
					return
				}
				if !yield(key, value) {
					return
				}
			}
		}
	} else {
		return func(yield func([]byte, []byte) bool) {
			enum, _, err := kv.tree.Seek(start)
			if err != nil {
				panic(err)
			}
			for {
				key, value, err := enum.Next()
				if err != nil {
					if err == io.EOF {
						return
					}
					panic(err)
				}
				if bytes.Compare(end, key) >= 0 {
					return
				}
				if !yield(key, value) {
					return
				}
			}
		}
	}
}

func (kv *KV) Put(buf []byte, key []byte, upd func(key, old []byte) (new []byte, write bool, err error)) (old []byte, written bool, err error) {
	return kv.tree.Put(buf, key, upd)
}

func (kv *KV) Set(key []byte, value []byte) {
	if err := kv.tree.Set(key, value); err != nil {
		panic(err)
	}
}

func (kv *KV) Clear() {
	if err := kv.tree.Clear(); err != nil {
		panic(err)
	}
}

func (kv *KV) Delete(key []byte) {
	if err := kv.tree.Delete(key); err != nil {
		panic(err)
	}
}

func (kv *KV) Extract(buf []byte, key []byte) []byte {
	value, err := kv.tree.Extract(buf, key)
	if err != nil {
		panic(err)
	}
	return value
}
