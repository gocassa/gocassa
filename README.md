SUCH MAGIC, MUCH CASSANDRA WOW
===

#### What is cmagic?

A cassandra object mapper using gocql under the hood.


#### Wow, cool, how does it work?

Here is a code snippet which may get you started:

```
package main 

import(
	"github.com/hailocab/cmagic"
	"fmt"
)

type Customer struct {
	Id string 
	Firstname string
	Lastname string
	Nbtravel int
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
	err = coll.Create(customer)
	fmt.Println(err)
}
```

The above snippet actually works in staging - go and try!

#### Anything else?

For those who wonder - this project will be a merge of hailocab/om and hailocab/erdos.
Using the API of erdos with the CQL backend of om.

API is not final! Please hate as much as possible, it will probably hurt my feelings a lot but at least we will have a usable library.