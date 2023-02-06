//go:build tools
// +build tools

package tools

import (
    // Needed because of https://github.com/golang/mock/tree/v1.6.0#reflect-vendoring-error
    _ "github.com/golang/mock/mockgen/model"
)
