package spec

import "fmt"

// Parser provides operations for parsing application specs
type Parser interface {
	ParseSpecs(specDir string) ([]interface{}, error)
}

// AppSpec defines a k8s application specification
type AppSpec struct {
	// Key is used by applications to register to the factory
	Key string
	// List of k8s spec objects
	SpecList []interface{}
	// Enabled indicates if the application is enabled in the factory
	Enabled bool
}

// GetID returns the unique ID for the app specs
func (in *AppSpec) GetID(instanceID string) string {
	return fmt.Sprintf("%s-%s", in.Key, instanceID)
}

// DeepCopy Creates a copy of the AppSpec
func (in *AppSpec) DeepCopy() *AppSpec {
	if in == nil {
		return nil
	}
	out := new(AppSpec)
	out.Key = in.Key
	out.Enabled = in.Enabled
	out.SpecList = make([]interface{}, 0)
	for _, spec := range in.SpecList {
		out.SpecList = append(out.SpecList, spec)
	}
	return out
}
