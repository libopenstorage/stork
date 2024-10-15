package schedops

// This is a subclass of k8sSchedOps
// This is needed to differentiate k8s and OCP scheduler
type ocpSchedOps struct {
	k8sSchedOps
}

func init() {
	k := &ocpSchedOps{}
	Register("openshift", k)
}
