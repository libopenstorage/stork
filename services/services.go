package services

// Service allows an implementation to be controlled by the Torpedo engine.
type Service interface {
	// Start the service.
	Start() error

	// Stop the service.
	Stop() error
}
