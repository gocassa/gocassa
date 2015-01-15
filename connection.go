package cmagic

import(
	"github.com/gocql/gocql"
)

type connection struct {
	s *gocql.Session
}

func Connect(nodeIps []string, username, password string) Connection {
	cluster := gocql.NewCluster(nodeIps...)
	cluster.Keyspace = nameSp
	cluster.Consistency = gocql.One
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return connection{
		s: session,
	}
}

func (c *connection) CreateKeySpace(name string) error {

}

func (c *connection) DropKeySpace(name string) error {

}

func (c *connection) KeySpace(name string) KeySpace {
	return &K{
		session: c.sess,
		name:    name,
		// nodeIps: nodeIps,
	}, nil
}