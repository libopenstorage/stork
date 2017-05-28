package volume

type driver struct {
}

func (d *driver) String() string {
	return "pxd"
}

func (d *driver) Exit(Ip string) error {
	return nil
}

func (d *driver) Start(Ip string) error {
	return nil
}

func init() {
	register("pxd", &driver{})
}
