package storkctl

import (
	cmdtesting "k8s.io/kubernetes/pkg/kubectl/cmd/testing"
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
