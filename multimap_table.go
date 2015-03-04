package gocassa

type multimapT struct {
	Table
	fieldToIndexBy string
	idField        string
}

func (o *multimapT) Update(field, id interface{}, m map[string]interface{}) Op {
	return o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Update(m)
}

func (o *multimapT) UpdateWithOptions(field, id interface{}, m map[string]interface{}, opts Options) Op {
	return o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).UpdateWithOptions(m, opts)
}

func (o *multimapT) Delete(field, id interface{}) Op {
	return o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Delete()
}

func (o *multimapT) DeleteAll(field interface{}) Op {
	return o.Where(Eq(o.fieldToIndexBy, field)).Delete()
}

func (o *multimapT) Read(field, id, pointer interface{}) Op {
	return o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Query().ReadOne(pointer)
}

func (o *multimapT) MultiRead(field interface{}, ids []interface{}, pointerToASlice interface{}) Op {
	return o.Where(Eq(o.fieldToIndexBy, field), In(o.idField, ids...)).Query().Read(pointerToASlice)
}

func (o *multimapT) List(field, startId interface{}, limit int, pointerToASlice interface{}) Op {
	rels := []Relation{Eq(o.fieldToIndexBy, field)}
	if startId != nil {
		rels = append(rels, GTE(o.idField, startId))
	}
	return o.Where(rels...).Query().Limit(limit).Read(pointerToASlice)
}
