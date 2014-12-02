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

Here are possible solutions:

###### Leave it as it is

Since everyone programming in Go must now that struct fields are initialized with zero values if the are not specified in the struct literal, we can trust them to not make mistakes.

Pros:
- Keeps the API elegant and hassle free for people who now what are they using.

Cons:
- Trusts the user

###### Replace/Update

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

Pros:
- Keeps the API reasonable elegant
- Might be enough to avoid accidental replaces

Cons:
- We have two methods instead of one for update now

###### The nil pointers approach

Force people to use structs with pointer fields - similarly to what protocol buffers does. This way we can differentiate between zero value, or lack of a value altogether (nil pointer).

Pros:
- Let's us use structs, but prevents the aforementioned scenario

Cons:
- People have to use structs with pointer fields just for the sake of this library - even if they have no intention to do it otherwise.
- Increases boilerplate, one can not take the address of primitive literal in go (eg. &"Joe", or &42), rather methods like proto.String() or proto.Int() must be used

###### The explicit approach

We could force people to list the fields they want to update, or specify "ALL" (this is not a final design, only direction):

```go
var All = "" // A special value signifying we want to update all fields.

type Collection interface {
	// ...
	Update(i iterface{}, ...string) error
	// ...
}
```

And then people could:

```go
coll.Update(Customer{
	Id: "194",
	Nbtravel: 42,
}, "Nbtravel")
```

or: 

```go
coll.Update(Customer{
	Id: "194",
	Nbtravel: 42,
}, "Nbtravel")
```

Pros:
- Reminds people to not f*ck up

Cons:
- Optional parameters can be ignored, but using slices would further increase boilerplate.
- This only serves as a reminder to the users - it is not a type safe solution, people can mistype fieldnames and there data won't be saved.

###### Introduce a record type

We could potentially tweak the API a bit to introduce a record type. We could even embed our record type in custom struct types for a relly native feel. Given a record:

```go

```

#### Anything else?

For those who wonder - this project will be a merge of hailocab/om and hailocab/erdos.
Using the API of erdos with the CQL backend of om.

API is not final! Please hate as much as possible, it will probably hurt my feelings a lot but at least we will have a usable library.