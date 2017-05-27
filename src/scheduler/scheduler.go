package scheduler

type Driver interface {
	// Init initializes this driver.  Parameters are provided as env variables.
	Init() error
	// GetNodes returns an array of all nodes in the cluster.
	GetNodes() ([]string, error)
}

func Register(name string, d Driver) error {

}

func Get(name string) (Driver, error) {

}
