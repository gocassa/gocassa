// Copyright (c) 2014 Dataence, LLC. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Taken from github.com/surgebase/compare after package disappeared.

package gocassa

import (
	"fmt"
	"reflect"
)

type comparator func(k1, k2 interface{}) (bool, error)

func builtinLessThan(k1, k2 interface{}) (bool, error) {
	if reflect.TypeOf(k1) != reflect.TypeOf(k2) {
		return false, fmt.Errorf("skiplist/BuiltinLessThan: k1.(%s) and k2.(%s) have different types",
			reflect.TypeOf(k1).Name(), reflect.TypeOf(k2).Name())
	}

	switch k1 := k1.(type) {
	case string:
		return k1 < k2.(string), nil

	case int64:
		return k1 < k2.(int64), nil

	case int32:
		return k1 < k2.(int32), nil

	case int16:
		return k1 < k2.(int16), nil

	case int8:
		return k1 < k2.(int8), nil

	case int:
		return k1 < k2.(int), nil

	case float32:
		return k1 < k2.(float32), nil

	case float64:
		return k1 < k2.(float64), nil

	case uint:
		return k1 < k2.(uint), nil

	case uint8:
		return k1 < k2.(uint8), nil

	case uint16:
		return k1 < k2.(uint16), nil

	case uint32:
		return k1 < k2.(uint32), nil

	case uint64:
		return k1 < k2.(uint64), nil

	case uintptr:
		return k1 < k2.(uintptr), nil
	}

	return false, fmt.Errorf("skiplist/BuiltinLessThan: unsupported types for k1.(%s) and k2.(%s)",
		reflect.TypeOf(k1).Name(), reflect.TypeOf(k2).Name())
}

func builtinGreaterThan(k1, k2 interface{}) (bool, error) {
	if reflect.TypeOf(k1) != reflect.TypeOf(k2) {
		return false, fmt.Errorf("skiplist/BuiltinGreaterThan: k1.(%s) and k2.(%s) have different types",
			reflect.TypeOf(k1).Name(), reflect.TypeOf(k2).Name())
	}

	switch k1 := k1.(type) {
	case string:
		return k1 > k2.(string), nil

	case int64:
		return k1 > k2.(int64), nil

	case int32:
		return k1 > k2.(int32), nil

	case int16:
		return k1 > k2.(int16), nil

	case int8:
		return k1 > k2.(int8), nil

	case int:
		return k1 > k2.(int), nil

	case float32:
		return k1 > k2.(float32), nil

	case float64:
		return k1 > k2.(float64), nil

	case uint:
		return k1 > k2.(uint), nil

	case uint8:
		return k1 > k2.(uint8), nil

	case uint16:
		return k1 > k2.(uint16), nil

	case uint32:
		return k1 > k2.(uint32), nil

	case uint64:
		return k1 > k2.(uint64), nil

	case uintptr:
		return k1 > k2.(uintptr), nil
	}

	return false, fmt.Errorf("skiplist/BuiltinGreaterThan: unsupported types for k1.(%s) and k2.(%s)",
		reflect.TypeOf(k1).Name(), reflect.TypeOf(k2).Name())
}

func builtinEqual(k1, k2 interface{}) (bool, error) {
	if reflect.TypeOf(k1) != reflect.TypeOf(k2) {
		return false, fmt.Errorf("skiplist/BuiltinEqual: k1.(%s) and k2.(%s) have different types",
			reflect.TypeOf(k1).Name(), reflect.TypeOf(k2).Name())
	}

	switch k1 := k1.(type) {
	case string:
		return k1 == k2.(string), nil

	case int64:
		return k1 == k2.(int64), nil

	case int32:
		return k1 == k2.(int32), nil

	case int16:
		return k1 == k2.(int16), nil

	case int8:
		return k1 == k2.(int8), nil

	case int:
		return k1 == k2.(int), nil

	case float32:
		return k1 == k2.(float32), nil

	case float64:
		return k1 == k2.(float64), nil

	case uint:
		return k1 == k2.(uint), nil

	case uint8:
		return k1 == k2.(uint8), nil

	case uint16:
		return k1 == k2.(uint16), nil

	case uint32:
		return k1 == k2.(uint32), nil

	case uint64:
		return k1 == k2.(uint64), nil

	case uintptr:
		return k1 == k2.(uintptr), nil
	}

	return false, fmt.Errorf("skiplist/BuiltinLessThan: unsupported types for k1.(%s) and k2.(%s)",
		reflect.TypeOf(k1).Name(), reflect.TypeOf(k2).Name())
}
