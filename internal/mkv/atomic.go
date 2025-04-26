package mkv

import "time"

type Atomic struct {
	db       *DB
	finished bool
}

func (db *DB) Atomic() *Atomic {
	db.mtx.Lock()
	defer func() {
		if err := recover(); err != nil {
			db.mtx.Unlock()
			panic(err)
		}
	}()
	if db.synctimer == nil {
		if err := db.filer.BeginUpdate(); err != nil {
			panic(err)
		}
		db.synctimer = time.AfterFunc(10*time.Second, func() {
			db.mtx.Lock()
			defer db.mtx.Unlock()
			db.synctimer = nil
			if err := db.filer.EndUpdate(); err != nil {
				panic(err)
			}
		})
	}
	db.begin()
	return &Atomic{db: db, finished: false}
}

func (atomic *Atomic) Cancel() {
	if atomic.finished {
		return
	}
	atomic.finished = true
	defer atomic.db.mtx.Unlock()
	atomic.db.rollback()
}

func (atomic *Atomic) Submit() {
	if atomic.finished {
		return
	}
	atomic.finished = true
	defer atomic.db.mtx.Unlock()
	atomic.db.end()
}
