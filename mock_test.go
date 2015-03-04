package gocassa

import (
	"testing"

	//"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type user struct {
	Pk1  int
	Pk2  int
	Ck1  int
	Ck2  int
	Name string
}

func TestRunMockSuite(t *testing.T) {
	suite.Run(t, new(MockSuite))
}

type MockSuite struct {
	suite.Suite
	//*require.Assertions
	tbl Table
}

func (s *MockSuite) SetupTest() {
	ks := NewMockKeySpace()
	//s.Assertions = require.New(s.T())
	s.tbl = ks.Table("users", user{}, Keys{
		PartitionKeys:     []string{"Pk1", "Pk2"},
		ClusteringColumns: []string{"Ck1", "Ck2"},
	})
}

func (s *MockSuite) TestEmpty() {
	var result []user
	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 1)).Query().Read(&result).Run())
	s.Equal(0, len(result))
}

func (s *MockSuite) TestRead() {
	u1, u2, u3, u4 := s.insertUsers()

	var users []user
	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1)).Query().Read(&users).Run())
	s.Equal([]user{u1, u3, u4}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 2)).Query().Read(&users).Run())
	s.Equal([]user{u2}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), In("Pk2", 1, 2)).Query().Read(&users).Run())
	s.Equal([]user{u1, u3, u4, u2}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1)).Query().Read(&users).Run())
	s.Equal([]user{u1, u4}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 1)).Query().Read(&users).Run())
	s.Equal([]user{u1}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), GT("Ck1", 1)).Query().Read(&users).Run())
	s.Equal([]user{u3}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), LT("Ck2", 2)).Query().Read(&users).Run())
	s.Equal([]user{u1}, users)

	var u user
	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 1)).Query().ReadOne(&u).Run())
	s.Equal(u1, u)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 2)).Query().ReadOne(&u).Run())
	s.Equal(u4, u)
}

func (s *MockSuite) TestUpdate() {
	s.insertUsers()

	relations := []Relation{Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 2)}

	s.NoError(s.tbl.Where(relations...).Update(map[string]interface{}{
		"Name": "x",
	}).Run())

	var u user
	s.NoError(s.tbl.Where(relations...).Query().ReadOne(&u).Run())
	s.Equal("x", u.Name)

	relations = []Relation{Eq("Pk1", 1), In("Pk2", 1, 2), Eq("Ck1", 1), Eq("Ck2", 1)}

	s.NoError(s.tbl.Where(relations...).Update(map[string]interface{}{
		"Name": "y",
	}).Run())

	var users []user
	s.NoError(s.tbl.Where(relations...).Query().Read(&users).Run())
	for _, u := range users {
		s.Equal("y", u.Name)
	}
}

func (s *MockSuite) TestDeleteOne() {
	s.insertUsers()

	relations := []Relation{Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 2)}
	s.NoError(s.tbl.Where(relations...).Delete().Run())

	var users []user
	s.NoError(s.tbl.Where(relations...).Query().Read(&users).Run())
	s.Empty(users)
}

func (s *MockSuite) TestDeleteWithIn() {
	s.insertUsers()

	relations := []Relation{Eq("Pk1", 1), In("Pk2", 1, 2), Eq("Ck1", 1), Eq("Ck2", 1)}
	s.NoError(s.tbl.Where(relations...).Delete().Run())

	var users []user
	s.NoError(s.tbl.Where(relations...).Query().Read(&users).Run())
	s.Empty(users)
}

func (s *MockSuite) insertUsers() (user, user, user, user) {
	u1 := user{
		Pk1:  1,
		Pk2:  1,
		Ck1:  1,
		Ck2:  1,
		Name: "John",
	}
	u2 := user{
		Pk1:  1,
		Pk2:  2,
		Ck1:  1,
		Ck2:  1,
		Name: "Joe",
	}
	u3 := user{
		Pk1:  1,
		Pk2:  1,
		Ck1:  2,
		Ck2:  1,
		Name: "Josh",
	}
	u4 := user{
		Pk1:  1,
		Pk2:  1,
		Ck1:  1,
		Ck2:  2,
		Name: "Jane",
	}

	for _, u := range []user{u1, u2, u3, u4} {
		s.NoError(s.tbl.Set(u).Run())
	}

	return u1, u2, u3, u4
}
