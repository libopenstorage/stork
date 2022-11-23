package kvdbutils

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/log"

	"github.com/portworx/torpedo/drivers/volume"
)

// ValidateKVDBMembers health and availability.
func ValidateKVDBMembers(kvdbMembers map[string]*volume.MetadataNode) error {
	log.Infof("Current KVDB members: %v", kvdbMembers)
	if len(kvdbMembers) < 3 {
		err := fmt.Errorf("No KVDB membes to validate or less than 3 members to validate")
		log.Warn(err.Error())
		return err
	}
	for id, m := range kvdbMembers {
		if !m.IsHealthy {
			err := fmt.Errorf("kvdb member node: %v is not healthy", id)
			log.Warn(err.Error())
			return err
		}
		log.Infof("KVDB member node %v is healthy", id)
	}
	return nil
}
