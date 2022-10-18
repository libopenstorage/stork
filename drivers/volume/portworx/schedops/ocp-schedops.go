package schedops

import (
	"github.com/sirupsen/logrus"
)

// This is a subclass of k8sSchedOps
// This is needed to differentiate k8s and OCP scheduler
type ocpSchedOps struct {
	k8sSchedOps
	log *logrus.Logger
}

func init() {
	k := &ocpSchedOps{}
	Register("openshift", k)
}

func (o *ocpSchedOps) Init(logger *logrus.Logger) {
	o.log = logger
	o.k8sSchedOps.log = logger
}
