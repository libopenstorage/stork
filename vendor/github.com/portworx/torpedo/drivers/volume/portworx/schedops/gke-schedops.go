package schedops

// This is a subclass of k8sSchedOps
// This is needed to differentiate k8s and gke scheduler
type gkeSchedOps struct {
	k8sSchedOps
}

func init() {
	g := &gkeSchedOps{}
	Register("gke", g)
}
