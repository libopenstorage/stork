package schedops

// This is a subclass of k8sSchedOps
// This is needed to differentiate k8s and Amazon EKS scheduler
type eksSchedOps struct {
	k8sSchedOps
}

func init() {
	e := &eksSchedOps{}
	Register("eks", e)
}
