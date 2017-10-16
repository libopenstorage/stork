package drivers

// Driver specifies the most basic methods to be implemented by a Stork driver.
type Driver interface {
	// Init the driver.
	Init() error
}
