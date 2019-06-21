package gocassa

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type account struct {
	ID   string
	Name string
}

func TestScanIterSlice(t *testing.T) {
	results := []map[string]interface{}{
		{"id": "acc_abcd1", "name": "John", "created": "2018-05-01 19:00:00+0000"},
		{"id": "acc_abcd2", "name": "Jane", "created": "2018-05-02 20:00:00+0000"},
	}

	stmt := newSelectStatement("", []interface{}{}, []string{"id", "name", "created"})
	iter := newMockIterator(results, stmt.FieldNames())

	expected := []account{
		{ID: "acc_abcd1", Name: "John"},
		{ID: "acc_abcd2", Name: "Jane"},
	}

	// Test with decoding into a slice of structs
	a1 := []account{}
	rowsRead, err := newScanner(stmt, &a1).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 2, rowsRead)
	assert.Equal(t, expected, a1)
	iter.Reset()

	// Test with decoding into a pointer of slice of structs
	b1 := &[]account{}
	rowsRead, err = newScanner(stmt, &b1).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 2, rowsRead)
	assert.Equal(t, expected, *b1)
	iter.Reset()

	// Test with decoding into a pre-populated struct. It should
	// remove existing elements
	c1 := &[]account{{ID: "acc_abcd3", Name: "Joe"}}
	rowsRead, err = newScanner(stmt, &c1).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 2, rowsRead)
	assert.Equal(t, expected, *c1)
	iter.Reset()

	// Test decoding into a nil slice
	var d1 []account
	assert.Nil(t, d1)
	rowsRead, err = newScanner(stmt, &d1).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 2, rowsRead)
	assert.Equal(t, expected, d1)
	iter.Reset()

	// Test decoding into a pointer of pointer of nil-ness
	var e1 **[]account
	assert.Nil(t, e1)
	rowsRead, err = newScanner(stmt, &e1).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 2, rowsRead)
	assert.Equal(t, expected, **e1)
	iter.Reset()

	// Test decoding into a slice of pointers
	var f1 []*account
	assert.Nil(t, f1)
	rowsRead, err = newScanner(stmt, &f1).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 2, rowsRead)
	assert.Equal(t, expected[0], *f1[0])
	assert.Equal(t, expected[1], *f1[1])
	iter.Reset()

	// Test decoding into a completely tangent struct
	type fakeStruct struct {
		Foo string
		Bar string
	}
	var g1 []fakeStruct
	assert.Nil(t, g1)
	rowsRead, err = newScanner(stmt, &g1).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 2, rowsRead)
	assert.Equal(t, fakeStruct{}, g1[0])
	assert.Equal(t, fakeStruct{}, g1[1])
	iter.Reset()

	// Test decoding into a struct with no fields
	type emptyStruct struct{}
	var h1 []emptyStruct
	assert.Nil(t, h1)
	rowsRead, err = newScanner(stmt, &h1).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 2, rowsRead)
	assert.Equal(t, emptyStruct{}, h1[0])
	assert.Equal(t, emptyStruct{}, h1[1])
	iter.Reset()

	// Test decoding into a struct with invalid types panics
	type badStruct struct {
		ID   int64
		Name int32
	}
	var i1 []badStruct
	assert.Nil(t, i1)
	assert.Panics(t, func() { newScanner(stmt, &i1).ScanIter(iter) })
	iter.Reset()
}

func TestScanIterStruct(t *testing.T) {
	results := []map[string]interface{}{
		{"id": "acc_abcd1", "name": "John", "created": "2018-05-01 19:00:00+0000"},
		{"id": "acc_abcd2", "name": "Jane", "created": "2018-05-02 20:00:00+0000"},
	}

	stmt := newSelectStatement("", []interface{}{}, []string{"id", "name", "created"})
	iter := newMockIterator(results, stmt.FieldNames())

	expected := []account{
		{ID: "acc_abcd1", Name: "John"},
		{ID: "acc_abcd2", Name: "Jane"},
	}

	// Test with decoding into a struct
	a1 := account{}
	rowsRead, err := newScanner(stmt, &a1).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 1, rowsRead)
	assert.Equal(t, expected[0], a1)
	iter.Reset()

	// Test decoding into a pointer of pointer to struct
	b1 := &account{}
	rowsRead, err = newScanner(stmt, &b1).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 1, rowsRead)
	assert.Equal(t, expected[0], *b1)
	iter.Reset()

	// Test decoding into a nil struct
	var c1 *account
	assert.Nil(t, c1)
	rowsRead, err = newScanner(stmt, &c1).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 1, rowsRead)
	assert.Equal(t, expected[0], *c1)
	iter.Reset()

	// Test decoding into a pointer of pointer of pointer to struct
	var d1 **account
	assert.Nil(t, d1)
	rowsRead, err = newScanner(stmt, &d1).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 1, rowsRead)
	assert.Equal(t, expected[0], **d1)
	iter.Reset()

	// Test with multiple scans into different structs
	var e1 *account
	var e2 ****account
	rowsRead, err = newScanner(stmt, &e1).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 1, rowsRead)
	rowsRead, err = newScanner(stmt, &e2).ScanIter(iter)
	assert.NoError(t, err)
	assert.Equal(t, 1, rowsRead)
	assert.Equal(t, expected[0], *e1)
	assert.Equal(t, expected[1], ****e2)
	iter.Reset()

	// Test for row not found
	var f1 *account
	noResultsIter := newMockIterator([]map[string]interface{}{}, stmt.FieldNames())
	rowsRead, err = newScanner(stmt, &f1).ScanIter(noResultsIter)
	assert.EqualError(t, err, ":0: No rows returned")
}

func TestAllocateNilReference(t *testing.T) {
	// Test non pointer, should do nothing
	var a string
	assert.Equal(t, "", a)
	assert.False(t, allocateNilReference(a))
	assert.Equal(t, "", a)

	// Test pointer which hasn't been passed in by reference, should panic
	var b *string
	assert.Nil(t, b)
	assert.Panics(t, func() { allocateNilReference(b) })

	// Test pointer which is passed in by ref
	assert.Nil(t, b)
	assert.True(t, allocateNilReference(&b))
	assert.Equal(t, "", *b)

	// Test with a struct
	type test struct{}
	var c *test
	assert.Nil(t, c)
	assert.True(t, allocateNilReference(&c))
	assert.Equal(t, test{}, *c)

	// Test with a slice
	var d *[]test
	assert.Nil(t, d)
	assert.True(t, allocateNilReference(&d))
	assert.Equal(t, []test{}, *d)

	// Test with a slice of pointers
	var e *[]*test
	assert.Nil(t, e)
	assert.True(t, allocateNilReference(&e))
	assert.Equal(t, []*test{}, *e)

	// Test with a map
	var f map[string]test
	assert.Nil(t, f)
	assert.True(t, allocateNilReference(&f))
	assert.Equal(t, map[string]test{}, f)

	// Test with an allocated struct, it should just return
	g := []*test{}
	ref := &g
	assert.False(t, allocateNilReference(&g))
	assert.Equal(t, ref, &g)
}

func TestGetNonPtrType(t *testing.T) {
	var a int
	assert.Equal(t, reflect.TypeOf(int(0)), getNonPtrType(reflect.TypeOf(a)))
	assert.Equal(t, reflect.TypeOf(int(0)), getNonPtrType(reflect.TypeOf(&a)))

	var b *int
	assert.Equal(t, reflect.TypeOf(int(0)), getNonPtrType(reflect.TypeOf(&b)))

	var c []*int
	assert.Equal(t, reflect.TypeOf([]*int{}), getNonPtrType(reflect.TypeOf(c)))
	assert.Equal(t, reflect.TypeOf([]*int{}), getNonPtrType(reflect.TypeOf(&c)))
}

func TestWrapPtrValue(t *testing.T) {
	// Test with no pointers, should do nothing
	a := reflect.ValueOf("")
	assert.Equal(t, string(""), wrapPtrValue(a, reflect.TypeOf("")).String())

	// Go ham with a double pointer
	var s **string
	targetType := reflect.TypeOf(s)
	assert.Equal(t, string(""), wrapPtrValue(a, targetType).Elem().Elem().String())
}
