package cmagic

import(
	"github.com/gocql/gocql"
)

type connection struct {
	s *gocql.Session
	nodeIps []string
	userName string
	pass string
}

func Connect(nodeIps []string, username, password string) (Connection, error) {
	cluster := gocql.NewCluster(nodeIps...)
	cluster.Consistency = gocql.One
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &connection{
		s: sess,
		nodeIps: nodeIps,
		userName: username,
		pass: password,
	}, nil
}

func (c *connection) CreateKeySpace(name string) error {
	return nil
}

func (c *connection) DropKeySpace(name string) error {
	return nil
}