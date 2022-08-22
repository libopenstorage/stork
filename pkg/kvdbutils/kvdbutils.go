package kvdbutils

import (
	"fmt"

	"github.com/portworx/torpedo/drivers/volume"
	"github.com/sirupsen/logrus"
)

// ValidateKVDBMembers health and availability.
func ValidateKVDBMembers(kvdbMembers map[string]*volume.MetadataNode) error {
	logrus.Infof("Current KVDB members: %v", kvdbMembers)
	if len(kvdbMembers) < 3 {
		err := fmt.Errorf("No KVDB membes to validate or less than 3 members to validate")
		logrus.Warn(err.Error())
		return err
	}
	for id, m := range kvdbMembers {
		if !m.IsHealthy {
			err := fmt.Errorf("kvdb member node: %v is not healthy", id)
			logrus.Warn(err.Error())
			return err
		}
		logrus.Infof("KVDB member node %v is healthy", id)
	}
	return nil
}
