package clonestatus

type CloneStatus string

const (
	Failed   CloneStatus = "FAILED"
	Cloning  CloneStatus = "CLONING"
	Complete CloneStatus = "COMPLETE"
)
