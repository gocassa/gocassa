package gocassa

import (
	"io/ioutil"
	"testing"
	"time"
)

type blogPost struct {
	PostID      string
	AuthorName  string
	Title       string
	Tags        []string
	Body        []byte
	CreatedAt   time.Time
	PublishedAt time.Time
	IsPublished bool
}

func BenchmarkDecodeBlogSliceNoBody(b *testing.B) {
	createdAt, _ := time.Parse("2006-01-02 15:04:05-0700", "2018-11-13 14:05:36+0000")
	publishedAt, _ := time.Parse("2006-01-02 15:04:05-0700", "2018-11-13 14:05:36+0000")

	m := map[string]interface{}{
		"postid":      "post_000000001234",
		"authorname":  "Jane Doe",
		"title":       "gocassa",
		"tags":        []string{"cassandra", "golang"},
		"createdat":   createdAt,
		"publishedAt": publishedAt,
		"ispublished": true,
	}

	fieldNames := sortedKeys(m)
	results := make([]map[string]interface{}, 20)
	for i := 0; i < 20; i++ {
		results[i] = m
	}
	stmt := newSelectStatement("", []interface{}{}, fieldNames)
	iter := newMockIterator(results, stmt.FieldNames())

	for i := 0; i < b.N; i++ {
		res := []blogPost{}
		iter.Reset()
		_, err := newScanner(stmt, &res).ScanIter(iter)

		if err != nil {
			b.Fatalf("err: %+v", err)
		}

		if len(res) != 20 {
			b.Fatalf("expected 20 blog posts, got %d", len(res))
		}

		if res[0].Title != "gocassa" {
			b.Fatalf("did not code result correctly, got %+v", res)
		}
	}
}

func BenchmarkDecodeBlogStruct(b *testing.B) {
	postData, _ := ioutil.ReadFile("README.md") // this is a riveting read!
	benchmarkBlogPostSingle(b, postData)
}

func BenchmarkDecodeBlogStructEmptyBody(b *testing.B) {
	benchmarkBlogPostSingle(b, []byte{})
}

func benchmarkBlogPostSingle(b *testing.B, postData []byte) {
	createdAt, _ := time.Parse("2006-01-02 15:04:05-0700", "2018-11-13 14:05:36+0000")
	publishedAt, _ := time.Parse("2006-01-02 15:04:05-0700", "2018-11-13 14:05:36+0000")

	m := map[string]interface{}{
		"postid":      "post_000000001234",
		"authorname":  "Jane Doe",
		"title":       "gocassa",
		"tags":        []string{"cassandra", "golang"},
		"body":        postData,
		"createdat":   createdAt,
		"publishedAt": publishedAt,
		"ispublished": true,
	}

	fieldNames := sortedKeys(m)
	results := []map[string]interface{}{m}
	stmt := newSelectStatement("", []interface{}{}, fieldNames)
	iter := newMockIterator(results, stmt.FieldNames())

	for i := 0; i < b.N; i++ {
		res := blogPost{}
		iter.Reset()
		_, err := newScanner(stmt, &res).ScanIter(iter)

		if err != nil {
			b.Fatalf("err: %+v", err)
		}

		if res.Title != "gocassa" {
			b.Fatalf("did not code result correctly, got %+v", res)
		}

		if len(res.Body) != len(postData) {
			b.Fatalf("did not code result correctly, got %+v", res)
		}
	}
}

type alphaStruct struct {
	A, B, C, D, E, F, G string
	H, I, J, K, L, M, N int
	O, P, Q, R, S, T, U float32
	V, W, X, Y, Z       float64
}

func BenchmarkDecodeAlphaSlice(b *testing.B) {
	m := map[string]interface{}{
		"a": "65", "b": "66", "c": "67", "d": "68", "e": "69", "f": "70", "g": "71",
		"h": 72, "i": 73, "j": 74, "k": 75, "l": 76, "m": 77, "n": 78,
		"o": 79.0, "p": 80.0, "q": 81.0, "r": 82.0, "s": 83.0, "t": 84.0, "u": 85.0,
		"v": 86.0, "w": 87.0, "x": 88.0, "y": 89.0, "z": 90.0,
	}

	fieldNames := sortedKeys(m)
	results := make([]map[string]interface{}, 20)
	for i := 0; i < 20; i++ {
		results[i] = m
	}
	stmt := newSelectStatement("", []interface{}{}, fieldNames)
	iter := newMockIterator(results, stmt.FieldNames())

	for i := 0; i < b.N; i++ {
		res := []alphaStruct{}
		iter.Reset()
		_, err := newScanner(stmt, &res).ScanIter(iter)

		if err != nil {
			b.Fatalf("err: %+v", err)
		}

		if len(res) != 20 {
			b.Fatalf("expected 20 alpha results, got %d", len(res))
		}

		if res[0].A != "65" || res[0].H != 72 || res[0].O != float32(79) || res[0].V != float64(86) {
			b.Fatalf("did not code result correctly, got %+v", res)
		}
	}
}

func BenchmarkDecodeAlphaStruct(b *testing.B) {
	m := map[string]interface{}{
		"a": "65", "b": "66", "c": "67", "d": "68", "e": "69", "f": "70", "g": "71",
		"h": 72, "i": 73, "j": 74, "k": 75, "l": 76, "m": 77, "n": 78,
		"o": 79.0, "p": 80.0, "q": 81.0, "r": 82.0, "s": 83.0, "t": 84.0, "u": 85.0,
		"v": 86.0, "w": 87.0, "x": 88.0, "y": 89.0, "z": 90.0,
	}

	fieldNames := sortedKeys(m)
	results := []map[string]interface{}{m}
	stmt := newSelectStatement("", []interface{}{}, fieldNames)
	iter := newMockIterator(results, stmt.FieldNames())

	for i := 0; i < b.N; i++ {
		res := alphaStruct{}
		iter.Reset()
		_, err := newScanner(stmt, &res).ScanIter(iter)

		if err != nil {
			b.Fatalf("err: %+v", err)
		}

		if res.A != "65" || res.H != 72 || res.O != float32(79) || res.V != float64(86) {
			b.Fatalf("did not code result correctly, got %+v", res)
		}
	}
}
