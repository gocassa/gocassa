package gocassa

type multimapT struct {
	*t
	fieldToIndexBy string
	idField        string
}

func (mm *multimapT) Update(field, id interface{}, m map[string]interface{}) Op {
	return mm.Where(Eq(mm.fieldToIndexBy, field), Eq(mm.idField, id)).Update(m)
}

func (mm *multimapT) UpdateWithOptions(field, id interface{}, m map[string]interface{}, opts Options) Op {
	return mm.Where(Eq(mm.fieldToIndexBy, field), Eq(mm.idField, id)).UpdateWithOptions(m, opts)
}

func (mm *multimapT) Delete(field, id interface{}) Op {
	return mm.Where(Eq(mm.fieldToIndexBy, field), Eq(mm.idField, id)).Delete()
}

func (mm *multimapT) DeleteAll(field interface{}) Op {
	return mm.Where(Eq(mm.fieldToIndexBy, field)).Delete()
}

func (mm *multimapT) Read(field, id, pointer interface{}) Op {
	return mm.Where(Eq(mm.fieldToIndexBy, field), Eq(mm.idField, id)).Query().ReadOne(pointer)
}

func (mm *multimapT) MultiRead(field interface{}, ids []interface{}, pointerToASlice interface{}) Op {
	return mm.Where(Eq(mm.fieldToIndexBy, field), In(mm.idField, ids...)).Query().Read(pointerToASlice)
}

func (mm *multimapT) List(field, startId interface{}, limit int, pointerToASlice interface{}) Op {
	rels := []Relation{Eq(mm.fieldToIndexBy, field)}
	if startId != nil {
		rels = append(rels, GTE(mm.idField, startId))
	}
	return mm.Where(rels...).Query().Limit(limit).Read(pointerToASlice)
}

func (mm *multimapT) WithOptions(o Options) MultimapTable {
	return &multimapT{
		t:              mm.t.WithOptions(o).(*t),
		fieldToIndexBy: mm.fieldToIndexBy,
		idField:        mm.idField,
	}
}
