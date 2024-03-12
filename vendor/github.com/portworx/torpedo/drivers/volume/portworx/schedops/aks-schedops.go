package schedops

// This is a subclass of k8sSchedOps
// This is needed to differentiate k8s and Azure scheduler
type aksSchedOps struct {
	k8sSchedOps
}

func init() {
	a := &aksSchedOps{}
	Register("aks", a)
}
