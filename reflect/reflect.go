// This package provides some punk-rock reflection which is not in the stdlib.
package reflect

import (
	// "fmt"
	r "reflect"
	//"strings"
)

// Converts a struct to a map.
func StructToMap(val interface{}) (map[string]interface{}, bool) {
	// indirect so function works with both structs and pointers to them
	structVal := r.Indirect(r.ValueOf(val))
	kind := structVal.Kind()
	if kind != r.Struct {
		return nil, false
	}
	typ := structVal.Type()
	mapVal := make(map[string]interface{})
	for i := 0; i < typ.NumField(); i++ {
		field := structVal.Field(i)
		mapVal[typ.Field(i).Name] = field.Interface()
	}
	return mapVal, true
}

// v should be a pointer to a struct.
func MapToStruct(m map[string]interface{}, struc interface{}) error {
	val := r.Indirect(r.ValueOf(struc))
	for k, v := range m {
		structField := val.FieldByName(k)
		if structField.IsValid() && structField.Type().Name() == r.TypeOf(v).Name() {
			structField.Set(r.ValueOf(v))
		}
	}
	return nil
}

// Nice copypaste bro _,^
func FieldsAndValues(val interface{}) ([]string, []interface{}, bool) {
	// indirect so function works with both structs and pointers to them
	structVal := r.Indirect(r.ValueOf(val))
	kind := structVal.Kind()
	if kind != r.Struct {
		return nil, nil, false
	}
	typ := structVal.Type()
	fields := []string{}
	values := []interface{}{}
	for i := 0; i < typ.NumField(); i++ {
		field := structVal.Field(i)
		fields = append(fields, typ.Field(i).Name)
		values = append(values, field.Interface())
	}
	return fields, values, true
}
