package k8sutils

import (
	"errors"
)

// ErrK8SApiAccountNotSet is returned when the account used to talk to k8s api
// is not setup
var ErrK8SApiAccountNotSet = errors.New("k8s api account is not setup")
