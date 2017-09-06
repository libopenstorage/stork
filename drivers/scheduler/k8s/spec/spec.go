package spec

// AppSpec defines a k8s application specification
type AppSpec interface {
	// Key is used by applications to register to the factory
	Key() string
	// Core gives access to the core components of a spec
	Core(instanceID string) []interface{}
	// Storage gives access to storage components of a spec
	Storage(instanceID string) []interface{}
	// IsEnabled indicates if the application is enabled in the factory
	IsEnabled() bool
}
