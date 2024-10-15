package schedops

// This is a subclass of k8sSchedOps
// This is needed to differentiate k8s and oracle scheduler
type okeSchedOps struct {
	k8sSchedOps
}

func init() {
	o := &okeSchedOps{}
	Register("oke", o)
}
