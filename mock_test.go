package gocassa

import (
	"testing"

	"github.com/stretchr/testify/require"
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
	*require.Assertions
	tbl     Table
	ks      KeySpace
	mapTbl  MapTable
	mmapTbl MultimapTable
}

func (s *MockSuite) SetupTest() {
	s.ks = NewMockKeySpace()
	s.Assertions = require.New(s.T())
	s.tbl = s.ks.Table("users", user{}, Keys{
		PartitionKeys:     []string{"Pk1", "Pk2"},
		ClusteringColumns: []string{"Ck1", "Ck2"},
	})

	s.mapTbl = s.ks.MapTable("users", "Pk1", user{})
	s.mmapTbl = s.ks.MultimapTable("users", "Pk1", "Pk2", user{})
}

// Table tests
func (s *MockSuite) TestTableEmpty() {
	var result []user
	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 1)).Query().Read(&result).Run())
	s.Equal(0, len(result))
}

func (s *MockSuite) TestTableRead() {
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

func (s *MockSuite) TestTableUpdate() {
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

func (s *MockSuite) TestTableDeleteOne() {
	s.insertUsers()

	relations := []Relation{Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 2)}
	s.NoError(s.tbl.Where(relations...).Delete().Run())

	var users []user
	s.NoError(s.tbl.Where(relations...).Query().Read(&users).Run())
	s.Empty(users)
}

func (s *MockSuite) TestTableDeleteWithIn() {
	s.insertUsers()

	relations := []Relation{Eq("Pk1", 1), In("Pk2", 1, 2), Eq("Ck1", 1), Eq("Ck2", 1)}
	s.NoError(s.tbl.Where(relations...).Delete().Run())

	var users []user
	s.NoError(s.tbl.Where(relations...).Query().Read(&users).Run())
	s.Empty(users)
}

// MapTable tests
func (s *MockSuite) TestMapTableRead() {
	s.insertUsers()
	var u user
	s.NoError(s.mapTbl.Read(1, &u).Run())
	s.Equal("Jane", u.Name)
	s.Error(s.mapTbl.Read(42, &u).Run())
}

func (s *MockSuite) TestMapTableMultiRead() {
	s.insertUsers()
	var users []user
	s.NoError(s.mapTbl.MultiRead([]interface{}{1, 2}, &users).Run())
	s.Len(users, 2)
	s.Equal("Jane", users[0].Name)
	s.Equal("Jill", users[1].Name)
}

func (s *MockSuite) TestMapTableUpdate() {
	s.insertUsers()
	s.NoError(s.mapTbl.Update(1, map[string]interface{}{
		"Name": "foo",
	}).Run())
	var u user
	s.NoError(s.mapTbl.Read(1, &u).Run())
	s.Equal("foo", u.Name)
}

func (s *MockSuite) TestMapTableDelete() {
	s.insertUsers()
	s.NoError(s.mapTbl.Delete(1).Run())
	var user user
	s.Equal(RowNotFoundError{}, s.mapTbl.Read(1, &user).Run())
}

// MultiMapTable tests
func (s *MockSuite) TestMultiMapTableRead() {
	s.insertUsers()

	var u user
	s.NoError(s.mmapTbl.Read(1, 1, &u).Run())
	s.Equal("Jane", u.Name)
	s.NoError(s.mmapTbl.Read(1, 2, &u).Run())
	s.Equal("Joe", u.Name)
}

func (s *MockSuite) TestMultiMapTableMultiRead() {
	s.insertUsers()
	var users []user
	s.NoError(s.mmapTbl.MultiRead(1, []interface{}{1, 2}, &users).Run())
	s.Len(users, 2)
	s.Equal("Jane", users[0].Name)
	s.Equal("Joe", users[1].Name)
}

func (s *MockSuite) TestMultiMapTableList() {
	s.insertUsers()
	var users []user
	s.NoError(s.mmapTbl.List(1, 0, 10, &users).Run())
	s.Len(users, 2)
	s.Equal("Jane", users[0].Name)
	s.Equal("Joe", users[1].Name)
}

func (s *MockSuite) TestMultiMapTableUpdate() {
	s.insertUsers()

	s.NoError(s.mmapTbl.Update(1, 2, map[string]interface{}{
		"Name": "foo",
	}).Run())
	var u user
	s.NoError(s.mmapTbl.Read(1, 2, &u).Run())
	s.Equal("foo", u.Name)
}

func (s *MockSuite) TestMultiMapTableDelete() {
	s.insertUsers()
	s.NoError(s.mmapTbl.Delete(1, 2).Run())
	var u user
	s.Equal(RowNotFoundError{}, s.mmapTbl.Read(1, 2, &u).Run())
}

func (s *MockSuite) TestMultiMapTableDeleteAll() {
	s.insertUsers()
	s.NoError(s.mmapTbl.DeleteAll(1).Run())
	var users []user
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
	u5 := user{
		Pk1:  2,
		Pk2:  1,
		Ck1:  1,
		Ck2:  1,
		Name: "Jill",
	}

	for _, u := range []user{u1, u2, u3, u4, u5} {
		s.NoError(s.tbl.Set(u).Run())
		s.NoError(s.mapTbl.Set(u).Run())
		s.NoError(s.mmapTbl.Set(u).Run())
	}

	return u1, u2, u3, u4
}
