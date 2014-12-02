SUCH MAGIC, MUCH CASSANDRA WOW
===

#### What is cmagic?

A cassandra object mapper using gocql under the hood.

#### Wow, cool, how does it work?

Here is a code snippet which may get you started:

```go
package main 

import(
	"github.com/hailocab/cmagic"
	"fmt"
)

type Customer struct {
	Id string 
	Firstname string
	Lastname string
	Nbtravel int  		// Number of times the customer travelled wit us
}

type LastNameUpdate struct {
	Lastname 
}

func main() {
	nameSpace, err := cmagic.New("cmagic", "", "", []string{"10.12.12.170", "10.12.21.83", "10.12.4.102"})
	if err != nil {
		panic(err)
	}
	coll := nameSpace.Collection("customer", Customer{})
	customer := Customer{
		Id: "194",
		Firstname: "Crufter",
		Nbtravel: 42,
	}
	fmt.Println(coll.Create(customer))
}
```

The above snippet actually works in staging - go and try!

#### What is the progress like?

This tool has support for CRUD operations now.

Although, even basic design decisions are not final yet:

##### The update debate

As it currently stands, one can Update by either supplying a map, or a struct:

```go
coll.Update(Customer{
	Id: "194",
	Firstname: "Crufter",
	Nbtravel: 42,
})
```

or
```go
coll.Update(cmagic.M{
	"Id": "194",
	"Firstname": "Crufter",
	"Nbtravel": 42,
})
```

This is in line with what one of the most popular DB drivers do in Go land, the mgo driver (https://github.com/go-mgo/mgo).

So where is the debate, you may ask, rightfully.
Our lovely DBAs are concerned by the following scenario: if someone creates an instance of a struct type with only a subset of the fields specified in the literal, like this:

```go
coll.Update(Customer{
	Id: "194",
	Nbtravel: 43,
})
```

The fields Firstname and Lastname will be overwritten by empty strings, since:

```go
dat, err := json.Marshal(Customer{
	Id: "194",
	Nbtravel: 43,
})
fmt.Println(string(dat), err)
```
(playground link: http://play.golang.org/p/_KS9HFJkc0)

Prints:

```go
{"Id":"194","Firstname":"","Lastname":"","Nbtravel":43} <nil>
```

The problem here is the library can not differentiate between "intentional zero values" and "field wasn't specified in struct literal so it was given a zero value" - the end result will be the same: the fields in the database will get overwritten by zero values.  

Possible solutions:

##### Leave it as it is

Since everyone programming in Go must now that struct fields are initialized with zero values if the are not specified in the struct literal, we can trust them to not make mistakes.

Pros:
- Keeps the API elegant and hassle free for people who now what are they using.

Cons:
- ...

##### Replace/Update

By renaming the Update method to Replace, it may be enough to make people remember that their whole row will get replaced.
We can possibly reintroduce Update but with a stricter requirements - allowing only maps to be used. The type signature would look something like this:

```go
type Collection interface {
	// ...
	Replace(v interface{}) error
	Update(m map[string]iterface{}) error
	// ...
}
```

#####

Force people to use structs with pointer fields - similarly to what protocol buffers does

#### Anything else?

For those who wonder - this project will be a merge of hailocab/om and hailocab/erdos.
Using the API of erdos with the CQL backend of om.

API is not final! Please hate as much as possible, it will probably hurt my feelings a lot but at least we will have a usable library.