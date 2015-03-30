package gocassa

type mapT struct {
	*t
	idField string
}

func (m *mapT) Update(id interface{}, ma map[string]interface{}) Op {
	return m.Where(Eq(m.idField, id)).Update(ma)
}

func (m *mapT) UpdateWithOptions(id interface{}, ma map[string]interface{}, opts Options) Op {
	return m.Where(Eq(m.idField, id)).UpdateWithOptions(ma, opts)
}

func (m *mapT) Delete(id interface{}) Op {
	return m.Where(Eq(m.idField, id)).Delete()
}

func (m *mapT) Read(id, pointer interface{}) Op {
	return m.Where(Eq(m.idField, id)).Query().ReadOne(pointer)
}

func (m *mapT) MultiRead(ids []interface{}, pointerToASlice interface{}) Op {
	return m.Where(In(m.idField, ids...)).Query().Read(pointerToASlice)
}

func (m *mapT) WithOptions(o Options) MapTable {
	return &mapT{
		t:       m.t.WithOptions(o).(*t),
		idField: m.idField,
	}
}
