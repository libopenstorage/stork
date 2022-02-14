package portworx

const (
	// PureDriverName is the name of the portworx-pure driver implementation
	PureDriverName = "pure"
)

// pure is essentially the same as the portworx volume driver, just different in name. This way,
// we can have separate specs for pure volumes vs. normal portworx ones
type pure struct {
	portworx
}

func (p *pure) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	return p.portworx.init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap, PureDriverName)
}

func (p *pure) String() string {
	return PureDriverName
}
