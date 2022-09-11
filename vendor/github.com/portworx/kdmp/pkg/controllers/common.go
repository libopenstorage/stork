package controllers

import (
	"time"
)

var (
	// ResyncPeriod controller resync period
	ResyncPeriod = 10 * time.Second
	// RequeuePeriod controller requeue period
	RequeuePeriod = 5 * time.Second
	// ValidateCRDInterval CRD validation interval
	ValidateCRDInterval time.Duration = 10 * time.Second
	// ValidateCRDTimeout CRD validation timeout
	ValidateCRDTimeout time.Duration = 2 * time.Minute
	// CleanupFinalizer cleanup finalizer
	CleanupFinalizer = "kdmp.portworx.com/finalizer-cleanup"
)
