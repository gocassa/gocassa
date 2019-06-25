package gocassa

type mapT struct {
	t       Table
	idField string
}

func (m *mapT) Table() Table                        { return m.t }
func (m *mapT) Create() error                       { return m.Table().Create() }
func (m *mapT) CreateIfNotExist() error             { return m.Table().CreateIfNotExist() }
func (m *mapT) Name() string                        { return m.Table().Name() }
func (m *mapT) Recreate() error                     { return m.Table().Recreate() }
func (m *mapT) CreateStatement() (Statement, error) { return m.Table().CreateStatement() }
func (m *mapT) CreateIfNotExistStatement() (Statement, error) {
	return m.Table().CreateIfNotExistStatement()
}

func (m *mapT) Update(id interface{}, ma map[string]interface{}) Op {
	return m.Table().
		Where(Eq(m.idField, id)).
		Update(ma)
}

func (m *mapT) Set(v interface{}) Op {
	return m.Table().
		Set(v)
}

func (m *mapT) Delete(id interface{}) Op {
	return m.Table().
		Where(Eq(m.idField, id)).
		Delete()
}

func (m *mapT) Read(id, pointer interface{}) Op {
	return m.Table().
		Where(Eq(m.idField, id)).
		ReadOne(pointer)
}

func (m *mapT) MultiRead(ids []interface{}, pointerToASlice interface{}) Op {
	return m.Table().
		Where(In(m.idField, ids...)).
		Read(pointerToASlice)
}

func (m *mapT) WithOptions(o Options) MapTable {
	return &mapT{
		t:       m.Table().WithOptions(o),
		idField: m.idField,
	}
}
