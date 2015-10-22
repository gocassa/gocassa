package gocassa

type multimapMkT struct {
	Table
	fieldsToIndexBy []string
	idField         []string
}

func (mm *multimapMkT) Update(field, id map[string]interface{}, m map[string]interface{}) Op {
	return mm.Where(mm.ListOfEqualRelations(field, id)...).Update(m)
}

func (mm *multimapMkT) Delete(field, id map[string]interface{}) Op {
	return mm.Where(mm.ListOfEqualRelations(field, id)...).Delete()
}

func (mm *multimapMkT) DeleteAll(field map[string]interface{}) Op {
	return mm.Where(mm.ListOfEqualRelations(field, nil)...).Delete()
}

func (mm *multimapMkT) Read(field, id map[string]interface{}, pointer interface{}) Op {
	return mm.Where(mm.ListOfEqualRelations(field, id)...).ReadOne(pointer)
}

func (mm *multimapMkT) MultiRead(field, id map[string]interface{}, pointerToASlice interface{}) Op {
	return mm.Where(mm.ListOfEqualRelations(field, id)...).Read(pointerToASlice)
}

func (mm *multimapMkT) List(field, startId map[string]interface{}, limit int, pointerToASlice interface{}) Op {
	rels := mm.ListOfEqualRelations(field, nil)
	if startId != nil {
		for _, field := range mm.idField {
			if value := startId[field]; value != "" {
				rels = append(rels, GTE(field, value))
			}
		}
	}
	return mm.WithOptions(Options{Limit: limit}).(*multimapMkT).Where(rels...).Read(pointerToASlice)
}

func (mm *multimapMkT) WithOptions(o Options) MultimapMkTable {
	return &multimapMkT{
		Table:           mm.Table.WithOptions(o),
		fieldsToIndexBy: mm.fieldsToIndexBy,
		idField:         mm.idField,
	}
}

func (mm *multimapMkT) ListOfEqualRelations(fieldsToIndex, ids map[string]interface{}) []Relation {

	relations := make([]Relation, 0)

	for _, field := range mm.fieldsToIndexBy {
		if value := fieldsToIndex[field]; value != nil && value != "" {
			relation := Eq(field, value)
			relations = append(relations, relation)
		}
	}

	for _, field := range mm.idField {
		if value := ids[field]; value != nil && value != "" {
			relation := Eq(field, value)
			relations = append(relations, relation)
		}
	}

	return relations
}

func (mm *multimapMkT) ListOfInRelations(fieldsToIndex, ids map[string][]interface{}) []Relation {

	relations := make([]Relation, 0)

	for _, field := range mm.fieldsToIndexBy {
		if value := fieldsToIndex[field]; value != nil && len(value) > 0 {
			relation := In(field, value)
			relations = append(relations, relation)
		}
	}

	for _, field := range mm.idField {
		if value := ids[field]; value != nil && len(value) > 0 {
			relation := In(field, value)
			relations = append(relations, relation)
		}
	}

	return relations
}
