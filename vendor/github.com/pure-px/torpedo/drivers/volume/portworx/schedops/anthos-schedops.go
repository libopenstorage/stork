package schedops

// This is a subclass of k8sSchedOps
// This is needed to differentiate k8s and anthos scheduler
type anthosSchedOps struct {
	k8sSchedOps
}

func init() {
	k := &anthosSchedOps{}
	Register("anthos", k)
}
