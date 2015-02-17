package gocassa

type mapT struct {
	*t
	idField string
}

func (o *mapT) Update(id interface{}, m map[string]interface{}) Op {
	return o.Where(Eq(o.idField, id)).Update(m)
}

func (o *mapT) UpdateWithOptions(id interface{}, m map[string]interface{}, opts Options) Op {
	return o.Where(Eq(o.idField, id)).UpdateWithOptions(m, opts)
}

func (o *mapT) Delete(id interface{}) Op {
	return o.Where(Eq(o.idField, id)).Delete()
}

func (o *mapT) Read(id, pointer interface{}) Op {
	return o.Where(Eq(o.idField, id)).Query().ReadOne(pointer)
}

func (o *mapT) MultiRead(ids []interface{}, pointerToASlice interface{}) Op {
	return o.Where(In(o.idField, ids...)).Query().Read(pointerToASlice)
}
