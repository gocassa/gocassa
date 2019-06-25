package gocassa

type multimapT struct {
	t              Table
	fieldToIndexBy string
	idField        string
}

func (mm *multimapT) Table() Table                        { return mm.t }
func (mm *multimapT) Create() error                       { return mm.Table().Create() }
func (mm *multimapT) CreateIfNotExist() error             { return mm.Table().CreateIfNotExist() }
func (mm *multimapT) Name() string                        { return mm.Table().Name() }
func (mm *multimapT) Recreate() error                     { return mm.Table().Recreate() }
func (mm *multimapT) CreateStatement() (Statement, error) { return mm.Table().CreateStatement() }
func (mm *multimapT) CreateIfNotExistStatement() (Statement, error) {
	return mm.Table().CreateIfNotExistStatement()
}

func (mm *multimapT) Update(field, id interface{}, m map[string]interface{}) Op {
	return mm.Table().
		Where(Eq(mm.fieldToIndexBy, field),
			Eq(mm.idField, id)).
		Update(m)
}

func (mm *multimapT) Set(v interface{}) Op {
	return mm.Table().
		Set(v)
}

func (mm *multimapT) Delete(field, id interface{}) Op {
	return mm.Table().
		Where(Eq(mm.fieldToIndexBy, field), Eq(mm.idField, id)).
		Delete()
}

func (mm *multimapT) DeleteAll(field interface{}) Op {
	return mm.Table().
		Where(Eq(mm.fieldToIndexBy, field)).
		Delete()
}

func (mm *multimapT) Read(field, id, pointer interface{}) Op {
	return mm.Table().
		Where(Eq(mm.fieldToIndexBy, field),
			Eq(mm.idField, id)).
		ReadOne(pointer)
}

func (mm *multimapT) MultiRead(field interface{}, ids []interface{}, pointerToASlice interface{}) Op {
	return mm.Table().
		Where(Eq(mm.fieldToIndexBy, field),
			In(mm.idField, ids...)).
		Read(pointerToASlice)
}

func (mm *multimapT) List(field, startId interface{}, limit int, pointerToASlice interface{}) Op {
	rels := []Relation{Eq(mm.fieldToIndexBy, field)}
	if startId != nil {
		rels = append(rels, GTE(mm.idField, startId))
	}
	return mm.Table().
		WithOptions(Options{
			Limit: limit,
		}).
		Where(rels...).
		Read(pointerToASlice)
}

func (mm *multimapT) WithOptions(o Options) MultimapTable {
	return &multimapT{
		t:              mm.Table().WithOptions(o),
		fieldToIndexBy: mm.fieldToIndexBy,
		idField:        mm.idField,
	}
}
