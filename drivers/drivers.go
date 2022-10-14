package drivers

const (
	// KdmpConfigmapName kdmp config map name
	KdmpConfigmapName = "kdmp-config"
	// KdmpConfigmapNamespace kdmp config map ns
	KdmpConfigmapNamespace = "kube-system"
)

// Driver specifies the most basic methods to be implemented by a Stork driver.
type Driver interface {
	// Init the driver.
	Init() error
}
