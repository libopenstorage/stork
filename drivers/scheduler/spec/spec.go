package spec

// Parser provides operations for parsing application specs
type Parser interface {
	ParseCoreSpecs(specDir string) ([]interface{}, error)
	ParseStorageSpecs(specDir string) ([]interface{}, error)
}

// AppSpec defines a k8s application specification
type AppSpec struct {
	// Key is used by applications to register to the factory
	Key string
	// Core gives access to the core components of a spec
	Core []interface{}
	// Storage gives access to storage components of a spec
	Storage []interface{}
	// Enabled indicates if the application is enabled in the factory
	Enabled bool
}
