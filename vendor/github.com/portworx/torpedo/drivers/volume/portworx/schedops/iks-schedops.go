package schedops

// This is a subclass of k8sSchedOps
// This is needed to differentiate k8s and IKS scheduler
type ibmSchedOps struct {
	k8sSchedOps
}

func init() {
	i := &ibmSchedOps{}
	Register("iks", i)
}
