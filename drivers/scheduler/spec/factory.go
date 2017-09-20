package spec

import (
	"fmt"
	"io/ioutil"
	"path"

	"github.com/Sirupsen/logrus"
	"github.com/portworx/torpedo/pkg/errors"
	"k8s.io/client-go/kubernetes/scheme"
)

// Factory is an application spec factory
type Factory struct {
	specDir    string
	specParser Parser
}

var appSpecFactory = make(map[string]*AppSpec)

// register registers a new spec with the factory
func (f *Factory) register(id string, app *AppSpec) {
	logrus.Infof("Registering app: %v", id)
	appSpecFactory[id] = app
}

// Get returns a registered application
func (f *Factory) Get(id string) (*AppSpec, error) {
	if d, ok := appSpecFactory[id]; ok && d.Enabled {
		dCopy, err := scheme.Scheme.DeepCopy(d)
		if err != nil {
			logrus.Errorf("Failed to create a copy of spec object: %#v Err: %v", d, err)
			return nil, err
		}

		return dCopy.(*AppSpec), nil
	}

	return nil, &errors.ErrNotFound{
		ID:   id,
		Type: "AppSpec",
	}
}

// GetAll returns all registered enabled applications
func (f *Factory) GetAll() []*AppSpec {
	var specs []*AppSpec
	for _, val := range appSpecFactory {
		if val.Enabled {
			valCopy, err := scheme.Scheme.DeepCopy(val)
			if err != nil {
				logrus.Errorf("Failed to create a copy of spec object: %#v Err: %v", val, err)
				return nil
			}

			specs = append(specs, valCopy.(*AppSpec))
		}
	}

	return specs
}

// NewFactory creates a new spec factory
func NewFactory(specDir string, parser Parser) (*Factory, error) {
	f := &Factory{
		specDir:    specDir,
		specParser: parser,
	}

	files, err := ioutil.ReadDir(f.specDir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			specID := file.Name()

			logrus.Infof("Parsing: %v...", path.Join(f.specDir, specID))
			coreComponents, err := f.specParser.ParseCoreSpecs(path.Join(f.specDir, file.Name()))
			if err != nil {
				return nil, err
			}

			storageComponents, err := f.specParser.ParseStorageSpecs(path.Join(f.specDir, file.Name()))
			if err != nil {
				return nil, err
			}

			if (coreComponents == nil || len(coreComponents) == 0) &&
				(storageComponents == nil || len(storageComponents) == 0) {
				continue
			}

			// Register the spec
			f.register(specID, &AppSpec{
				Key:     specID,
				Core:    coreComponents,
				Storage: storageComponents,
				Enabled: true,
			})
		}
	}

	if apps := f.GetAll(); apps == nil || len(apps) == 0 {
		return nil, fmt.Errorf("found 0 supported applications in given specDir: %v", specDir)
	}

	return f, nil
}
