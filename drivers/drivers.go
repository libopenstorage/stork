package drivers

// Driver specifies the most basic methods to be implemented by a Torpedo driver.
type Driver interface {
	// Init the driver.
	Init() error
}
