package mkv

type View struct {
	db *DB
}

func (db *DB) View() View {
	db.mtx.RLock()
	return View{db: db}
}

func (view View) Close() {
	view.db.mtx.RUnlock()
}
