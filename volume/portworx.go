package volume

type driver struct {
}

func (d *driver) String() string {
	return "pxd"
}

func (d *driver) Exit(string Ip) error {
	return nil
}

func init() {
	register("pxd", &driver{})
}
