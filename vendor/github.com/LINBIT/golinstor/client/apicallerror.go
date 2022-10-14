package client

import (
	"strings"
)

type ApiCallError []ApiCallRc

func (e ApiCallError) Error() string {
	var finalErr string
	for i, r := range e {
		finalErr += strings.TrimSpace(r.String())
		if i < len(e)-1 {
			finalErr += " next error: "
		}
	}
	return finalErr
}

// Is is a shorthand for checking all ApiCallRcs of an ApiCallError against
// a given mask.
func (e ApiCallError) Is(mask uint64) bool {
	for _, r := range e {
		if r.Is(mask) {
			return true
		}
	}

	return false
}

// Is can be used to check the return code against a given mask. Since LINSTOR
// return codes are designed to be machine readable, this can be used to check
// for a very specific type of error.
// Refer to package apiconsts.go in package linstor for a list of possible
// mask values.
func (r ApiCallRc) Is(mask uint64) bool { return (uint64(r.RetCode) & mask) == mask }

// IsApiCallError checks if an error is a specific type of LINSTOR error.
func IsApiCallError(err error, mask uint64) bool {
	e, ok := err.(ApiCallError)
	if !ok {
		return false
	}

	return e.Is(mask)
}
