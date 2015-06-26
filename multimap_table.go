package gocassa

type multimapT struct {
	Table
	fieldToIndexBy string
	idField        string
}

func (mm *multimapT) Update(field, id interface{}, m map[string]interface{}) Op {
	return mm.Where(Eq(mm.fieldToIndexBy, field), Eq(mm.idField, id)).Update(m)
}

func (mm *multimapT) Delete(field, id interface{}) Op {
	return mm.Where(Eq(mm.fieldToIndexBy, field), Eq(mm.idField, id)).Delete()
}

func (mm *multimapT) DeleteAll(field interface{}) Op {
	return mm.Where(Eq(mm.fieldToIndexBy, field)).Delete()
}

func (mm *multimapT) Read(field, id, pointer interface{}) Op {
	return mm.Where(Eq(mm.fieldToIndexBy, field), Eq(mm.idField, id)).ReadOne(pointer)
}

func (mm *multimapT) MultiRead(field interface{}, ids []interface{}, pointerToASlice interface{}) Op {
	return mm.Where(Eq(mm.fieldToIndexBy, field), In(mm.idField, ids...)).Read(pointerToASlice)
}

func (mm *multimapT) List(field, startId interface{}, limit int, pointerToASlice interface{}) Op {
	rels := []Relation{Eq(mm.fieldToIndexBy, field)}
	if startId != nil {
		rels = append(rels, GTE(mm.idField, startId))
	}
	return mm.WithOptions(Options{Limit: limit}).(*multimapT).Where(rels...).Read(pointerToASlice)
}

func (mm *multimapT) WithOptions(o Options) MultimapTable {
	return &multimapT{
		Table:          mm.Table.WithOptions(o),
		fieldToIndexBy: mm.fieldToIndexBy,
		idField:        mm.idField,
	}
}
