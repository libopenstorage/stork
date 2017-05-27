package scheduler

type Driver interface {
	// GetNodes returns an array of all nodes in the cluster.
	GetNodes() ([]string, error)
}

func Register(name string, d Driver) error {

}

func Get(name string) (Driver, error) {

}
