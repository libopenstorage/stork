package schedops

// This is a subclass of k8sSchedOps
// This is needed to differentiate k8s and RKE scheduler
type rkeSchedOps struct {
	k8sSchedOps
}

func init() {
	r := &rkeSchedOps{}
	Register("rke", r)
}
