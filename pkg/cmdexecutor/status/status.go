package status

import (
	"fmt"

	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// statusConfigMapName is name of the config map the command executor uses to persist failed statuses
	statusConfigMapName = "cmdexecutor-status"
)

// Persist persists the status for the given key in the config map
func Persist(key, statusToPersist string) error {
	var err error
	if len(key) == 0 {
		return fmt.Errorf("no key provided to persist status")
	}

	cm, err := core.Instance().GetConfigMap(statusConfigMapName, meta_v1.NamespaceSystem)
	if err != nil {
		if errors.IsNotFound(err) {
			// create one
			defaultData := map[string]string{
				key: "",
			}
			cm = &v1.ConfigMap{
				ObjectMeta: meta_v1.ObjectMeta{
					Namespace: meta_v1.NamespaceSystem,
					Name:      statusConfigMapName,
				},
				Data: defaultData,
			}
			cm, err = core.Instance().CreateConfigMap(cm)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	cm.Data[key] = statusToPersist
	_, err = core.Instance().UpdateConfigMap(cm)
	if err != nil {
		return err
	}

	return nil
}

// Get fetches the status using the given key from the config map
func Get(key string) (string, error) {
	if len(key) == 0 {
		return "", fmt.Errorf("no key provided to get status")
	}

	cm, err := core.Instance().GetConfigMap(statusConfigMapName, meta_v1.NamespaceSystem)
	if err != nil {
		return "", err
	}

	status := cm.Data[key]
	if len(status) == 0 {
		return "", fmt.Errorf("found empty failure status for key: %s in config map", key)
	}

	logrus.Errorf("%v cmd executor failed because: %s", key, status)

	delete(cm.Data, key)
	_, cmUpdateErr := core.Instance().UpdateConfigMap(cm)
	if cmUpdateErr != nil {
		logrus.Warnf("failed to cleanup command executor status config map due to: %v", cmUpdateErr)
	}
	return status, nil
}
