//go:build unittest
// +build unittest

package storkctl

import (
	"k8s.io/client-go/rest"
	cmdtesting "k8s.io/kubectl/pkg/cmd/testing"
)

type TestFactory struct {
	cmdtesting.TestFactory
	Factory
}

func NewTestFactory() *TestFactory {
	return &TestFactory{
		TestFactory: *cmdtesting.NewTestFactory(),
		Factory:     NewFactory(),
	}
}

func (t *TestFactory) GetConfig() (*rest.Config, error) {
	return t.ToRESTConfig()
}

func (t *TestFactory) UpdateConfig() error {
	return nil
}
