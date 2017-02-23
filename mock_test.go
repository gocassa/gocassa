package gocassa

import (
	"reflect"
	"testing"
	"time"

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

type UserWithMap struct {
	Id  string
	Map map[string]interface{}
}

type point struct {
	Time time.Time
	Id   int
	User string
	X    float64
	Y    float64
}

type PostalCode string

type address struct {
	Time            time.Time
	Id              string
	LocationPrice   map[string]int       // json compatible map
	LocationHistory map[time.Time]string // not json compatible map
	PostCode        PostalCode           // embedded type
}

func TestRunMockSuite(t *testing.T) {
	suite.Run(t, new(MockSuite))
}

type MockSuite struct {
	suite.Suite
	*require.Assertions
	tbl       Table
	ks        KeySpace
	mapTbl    MapTable
	mmapTbl   MultimapTable
	tsTbl     TimeSeriesTable
	mtsTbl    MultiTimeSeriesTable
	embMapTbl MapTable
	embTsTbl  TimeSeriesTable
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
	s.tsTbl = s.ks.TimeSeriesTable("points", "Time", "Id", 1*time.Minute, point{})
	s.mtsTbl = s.ks.MultiTimeSeriesTable("points", "User", "Time", "Id", 1*time.Minute, point{})

	s.embMapTbl = s.ks.MapTable("addresses", "Id", address{})
	s.embTsTbl = s.ks.TimeSeriesTable("addresses", "Time", "Id", 1*time.Minute, address{})
}

// Table tests
func (s *MockSuite) TestTableEmpty() {
	var result []user
	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 1)).Read(&result).Run())
	s.Equal(0, len(result))
}

func (s *MockSuite) TestTableRead() {
	u1, u2, u3, u4 := s.insertUsers()

	var users []user
	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1)).Read(&users).Run())
	s.Equal([]user{u1, u4, u3}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 2)).Read(&users).Run())
	s.Equal([]user{u2}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), In("Pk2", 1, 2)).Read(&users).Run())
	s.Equal([]user{u1, u4, u3, u2}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1)).Read(&users).Run())
	s.Equal([]user{u1, u4}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 1)).Read(&users).Run())
	s.Equal([]user{u1}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), GT("Ck1", 1)).Read(&users).Run())
	s.Equal([]user{u3}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), LT("Ck2", 2)).Read(&users).Run())
	s.Equal([]user{u1}, users)

	var u user
	op1 := s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 1)).ReadOne(&u)
	s.NoError(op1.Run())
	s.Equal(u1, u)

	op2 := s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 2)).ReadOne(&u)
	s.NoError(op2.Run())
	s.Equal(u4, u)

	s.NoError(op1.Add(op2).Run())
	s.NoError(op1.Add(op2).RunAtomically())
}

func (s *MockSuite) TestTableUpdate() {
	s.insertUsers()

	relations := []Relation{Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 2)}

	s.NoError(s.tbl.Where(relations...).Update(map[string]interface{}{
		"Name": "x",
	}).Run())

	var u user
	s.NoError(s.tbl.Where(relations...).ReadOne(&u).Run())
	s.Equal("x", u.Name)

	relations = []Relation{Eq("Pk1", 1), In("Pk2", 1, 2), Eq("Ck1", 1), Eq("Ck2", 1)}

	s.NoError(s.tbl.Where(relations...).Update(map[string]interface{}{
		"Name": "y",
	}).Run())

	var users []user
	s.NoError(s.tbl.Where(relations...).Read(&users).Run())
	for _, u := range users {
		s.Equal("y", u.Name)
	}
}

func (s *MockSuite) TestTableDeleteOne() {
	s.insertUsers()

	relations := []Relation{Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 2)}
	s.NoError(s.tbl.Where(relations...).Delete().Run())

	var users []user
	s.NoError(s.tbl.Where(relations...).Read(&users).Run())
	s.Empty(users)
}

func (s *MockSuite) TestTableDeleteWithIn() {
	s.insertUsers()

	relations := []Relation{Eq("Pk1", 1), In("Pk2", 1, 2), Eq("Ck1", 1), Eq("Ck2", 1)}
	s.NoError(s.tbl.Where(relations...).Delete().Run())

	var users []user
	s.NoError(s.tbl.Where(relations...).Read(&users).Run())
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

func (s *MockSuite) TestMapModifiers() {
	tbl := s.ks.MapTable("user342135", "Id", UserWithMap{})
	createIf(tbl.(TableChanger), s.T())
	c := UserWithMap{
		Id: "1",
		Map: map[string]interface{}{
			"3": "Is Odd",
			"6": "Is Even",
		},
	}
	if err := tbl.Set(c).Run(); err != nil {
		s.T().Fatal(err)
	}
	if err := tbl.Update("1", map[string]interface{}{
		"Map": MapSetFields(map[string]interface{}{
			"2": "Two",
			"4": "Four",
		}),
	}).Run(); err != nil {
		s.T().Fatal(err)
	}

	// Read back into a new struct (see #83)
	var c2 UserWithMap
	if err := tbl.Read("1", &c2).Run(); err != nil {
		s.T().Fatal(err)
	}
	if !reflect.DeepEqual(c2, UserWithMap{
		Id: "1",
		Map: map[string]interface{}{
			"2": "Two",
			"3": "Is Odd",
			"4": "Four",
			"6": "Is Even",
		},
	}) {
		s.T().Fatal(c2)
	}
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

	// Offset 0, limit 10
	s.NoError(s.mmapTbl.List(1, 0, 10, &users).Run())
	s.Len(users, 2)
	s.Equal("Jane", users[0].Name)
	s.Equal("Joe", users[1].Name)

	// Offset 1, limit 1
	s.NoError(s.mmapTbl.List(1, 1, 1, &users).Run())
	s.Len(users, 1)
	s.Equal("Jane", users[0].Name)

	// Offset 2, limit 1
	s.NoError(s.mmapTbl.List(1, 2, 1, &users).Run())
	s.Len(users, 1)
	s.Equal("Joe", users[0].Name)
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

// TimeSeriesTable tests
func (s *MockSuite) TestTimeSeriesTableRead() {
	points := s.insertPoints()

	var p point
	s.NoError(s.tsTbl.Read(points[0].Time, points[0].Id, &p).Run())
	s.Equal(points[0], p)
}

func (s *MockSuite) TestTimeSeriesTableList() {
	points := s.insertPoints()

	// First two points
	var ps []point
	s.NoError(s.tsTbl.List(points[0].Time, points[1].Time, &ps).Run())
	s.Len(ps, 2)
	s.Equal(points[0], ps[0])
	s.Equal(points[1], ps[1])

	// Last two points
	s.NoError(s.tsTbl.List(points[1].Time, points[2].Time, &ps).Run())
	s.Len(ps, 2)
	s.Equal(points[1], ps[0])
	s.Equal(points[2], ps[1])
}

func (s *MockSuite) TestTimeSeriesTableUpdate() {
	points := s.insertPoints()

	s.NoError(s.tsTbl.Update(points[0].Time, points[0].Id, map[string]interface{}{
		"X": 42.0,
		"Y": 43.0,
	}).Run())
	var p point
	s.NoError(s.tsTbl.Read(points[0].Time, points[0].Id, &p).Run())
	s.Equal(42.0, p.X)
	s.Equal(43.0, p.Y)
}

func (s *MockSuite) TestTimeSeriesTableDelete() {
	points := s.insertPoints()

	var p point
	s.NoError(s.tsTbl.Delete(points[0].Time, points[0].Id).Run())
	s.Equal(RowNotFoundError{}, s.tsTbl.Read(points[0].Time, points[0].Id, &p).Run())
}

// MultiTimeSeriesTable tests
func (s *MockSuite) TestMultiTimeSeriesTableRead() {
	points := s.insertPoints()

	var p point
	s.NoError(s.mtsTbl.Read("John", points[0].Time, points[0].Id, &p).Run())
	s.Equal(points[0], p)
}

func (s *MockSuite) TestMultiTimeSeriesTableList() {
	points := s.insertPoints()

	var ps []point
	s.NoError(s.mtsTbl.List("John", points[0].Time, points[2].Time, &ps).Run())
	s.Len(ps, 2)
	s.Equal(points[0], ps[0])
	s.Equal(points[2], ps[1])

	s.NoError(s.mtsTbl.List("Jane", points[0].Time, points[2].Time, &ps).Run())
	s.Len(ps, 1)
	s.Equal(points[1], ps[0])
}

func (s *MockSuite) TestMultiTimeSeriesTableUpdate() {
	points := s.insertPoints()

	s.NoError(s.mtsTbl.Update("John", points[0].Time, points[0].Id, map[string]interface{}{
		"X": 42.0,
	}).Run())

	var p point
	s.NoError(s.mtsTbl.Read("John", points[0].Time, points[0].Id, &p).Run())
	s.Equal(42.0, p.X)
}

func (s *MockSuite) TestMultiTimeSeriesTableDelete() {
	points := s.insertPoints()

	s.NoError(s.mtsTbl.Delete("John", points[0].Time, points[0].Id).Run())

	var p point
	s.Equal(RowNotFoundError{}, s.mtsTbl.Read("John", points[0].Time, points[0].Id, &p).Run())
}

func (s *MockSuite) TestNoop() {
	s.insertUsers()
	var users []user
	op := Noop()
	op = op.Add(s.mapTbl.MultiRead([]interface{}{1, 2}, &users))
	s.NoError(op.Run())
	s.Len(users, 2)
	s.Equal("Jane", users[0].Name)
	s.Equal("Jill", users[1].Name)
}

func (s *MockSuite) TestEmbedMapRead() {
	expectedAddresses := s.insertAddresses()

	var actualAddress address
	s.NoError(s.embMapTbl.Read("1", &actualAddress).Run())
	s.Equal(expectedAddresses[0], actualAddress)

	s.NoError(s.embMapTbl.Read("2", &actualAddress).Run())
	s.Equal(expectedAddresses[1], actualAddress)
}

// Helper functions
func (s *MockSuite) insertPoints() []point {
	points := []point{
		point{
			Time: s.parseTime("2015-04-01 15:41:00"),
			Id:   1,
			User: "John",
			X:    1.1,
			Y:    1.2,
		},
		point{
			Time: s.parseTime("2015-04-01 15:41:05"),
			Id:   2,
			User: "Jane",
			X:    5.1,
			Y:    5.2,
		},
		point{
			Time: s.parseTime("2015-04-01 15:41:10"),
			Id:   3,
			User: "John",
			X:    1.1,
			Y:    1.3,
		},
	}

	for _, p := range points {
		s.NoError(s.tsTbl.Set(p).Run())
		s.NoError(s.mtsTbl.Set(p).Run())
	}

	return points
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

func (s *MockSuite) insertAddresses() []address {
	addresses := []address{
		address{
			Id:              "1",
			Time:            s.parseTime("2015-01-01 00:00:00"),
			LocationPrice:   map[string]int{"A": 1},
			LocationHistory: map[time.Time]string{time.Now().UTC(): "A"},
			PostCode:        "ABC",
		},
		address{
			Id:              "2",
			Time:            s.parseTime("2015-01-02 00:00:00"),
			LocationPrice:   map[string]int{"F": 1},
			LocationHistory: map[time.Time]string{time.Now().UTC(): "F"},
			PostCode:        "FGH",
		},
	}

	for _, addr := range addresses {
		s.NoError(s.embMapTbl.Set(addr).Run())
		s.NoError(s.embTsTbl.Set(addr).Run())
	}

	return addresses
}

func (s *MockSuite) parseTime(value string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", value)
	s.NoError(err)
	return t
}
