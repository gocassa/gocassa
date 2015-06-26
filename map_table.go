package gocassa

type mapT struct {
	Table
	idField string
}

func (m *mapT) Update(id interface{}, ma map[string]interface{}) Op {
	return m.Where(Eq(m.idField, id)).Update(ma)
}

func (m *mapT) Delete(id interface{}) Op {
	return m.Where(Eq(m.idField, id)).Delete()
}

func (m *mapT) Read(id, pointer interface{}) Op {
	return m.Where(Eq(m.idField, id)).ReadOne(pointer)
}

func (m *mapT) MultiRead(ids []interface{}, pointerToASlice interface{}) Op {
	return m.Where(In(m.idField, ids...)).Read(pointerToASlice)
}

func (m *mapT) WithOptions(o Options) MapTable {
	return &mapT{
		Table:   m.Table.WithOptions(o),
		idField: m.idField,
	}
}
