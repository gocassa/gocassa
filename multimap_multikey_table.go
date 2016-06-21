package gocassa

import (
	rrr "github.com/hailocab/gocassa/reflect"
	
//	log "github.com/cihub/seelog"
)

type multimapMkT struct {
	Table
	fieldsToIndexBy []string
	idField         []string
}

func (mm *multimapMkT) Update(field, id map[string]interface{}, m map[string]interface{}) Op {
	return mm.Where(mm.ListOfRelations(field, id)...).Update(m)
}

func (mm *multimapMkT) Delete(field, id map[string]interface{}) Op {
	return mm.Where(mm.ListOfRelations(field, id)...).Delete()
}

func (mm *multimapMkT) DeleteAll(field map[string]interface{}) Op {
	return mm.Where(mm.ListOfRelations(field, nil)...).Delete()
}

func (mm *multimapMkT) Read(field, id map[string]interface{}, pointer interface{}) Op {
	return mm.Where(mm.ListOfRelations(field, id)...).ReadOne(pointer)
}

func (mm *multimapMkT) MultiRead(field, id map[string]interface{}, pointerToASlice interface{}) Op {
	return mm.Where(mm.ListOfRelations(field, id)...).Read(pointerToASlice)
}

func (mm *multimapMkT) List(field, startId map[string]interface{}, limit int, pointerToASlice interface{}) Op {
	rels := mm.ListOfRelations(field, nil)
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

func (mm *multimapMkT) ListOfRelations(fieldsToIndex, ids map[string]interface{}) []Relation {

	relations := make([]Relation, 0)

	for _, field := range mm.fieldsToIndexBy {
		if value := fieldsToIndex[field]; value != nil && value != "" {
			relations = addRelation(relations, field, value)
		}
	}

	for _, field := range mm.idField {
		if value := ids[field]; value != nil && value != "" {
			relations = addRelation(relations, field, value)
		}
	}

	return relations
}

func addRelation(relations []Relation, field string, value interface{}) []Relation {
	if vi := rrr.AsInterfaceArray(value); len(vi) > 0 {
		relations = append(relations, In(field, vi...))
	} else {
		relations = append(relations, Eq(field, value))
	}
	return relations
}
