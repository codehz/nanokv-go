package mkv

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
