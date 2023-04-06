package pluralmap

import (
	"fmt"
	"strings"
	"sync"

	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type pluralMap struct {
	crdPluralLock      sync.Mutex
	crdKindToPluralMap map[string]string
}

// PluralMap has getters and setters for crdKindToPluralMap.
type PluralMap interface {
	// GetCRDKindToPluralMap returns the crdKindToPluralMap from pluralmap singleton.
	GetCRDKindToPluralMap() map[string]string

	// SetPluralForCRDKind will set a new CRD's plural in the map
	SetPluralForCRDKind(kind, plural string)
}

var (
	crdPluralMap        *pluralMap
	crdPluralGlobalLock sync.Mutex
)

func Instance() *pluralMap {
	if crdPluralMap == nil {
		CreateCRDPlurals()
	}
	crdPluralGlobalLock.Lock()
	defer crdPluralGlobalLock.Unlock()
	return crdPluralMap
}

func getCRDKindToPluralMap() map[string]string {
	kindToPluralMap := make(map[string]string)
	crdv1List, err := apiextensions.Instance().ListCRDs()
	if err == nil {
		for _, crd := range crdv1List.Items {
			kindToPluralMap[strings.ToLower(crd.Spec.Names.Kind)] = crd.Spec.Names.Plural
		}
	} else if apierrors.IsNotFound(err) {
		// list and register crds via v1beta1 apis
		crdv1beta1List, err := apiextensions.Instance().ListCRDsV1beta1()
		if err != nil {
			logrus.Warnf("unable to list v1beta1 crds: %v", err)
			return kindToPluralMap
		}
		for _, crd := range crdv1beta1List.Items {
			if len(crd.Spec.Names.Kind) > 0 {
				kindToPluralMap[strings.ToLower(crd.Spec.Names.Kind)] = crd.Spec.Names.Plural
			}
		}
	}
	return kindToPluralMap
}

func CreateCRDPlurals() error {
	crdPluralGlobalLock.Lock()
	defer crdPluralGlobalLock.Unlock()

	if crdPluralMap != nil {
		return fmt.Errorf("plural map has already been initialized")
	}
	crdPluralMap = &pluralMap{}
	crdPluralMap.crdKindToPluralMap = getCRDKindToPluralMap()
	logrus.Debugf("Current crd plural map: %v", crdPluralMap.crdKindToPluralMap)

	return nil
}

func (p *pluralMap) GetCRDKindToPluralMap() map[string]string {
	p.crdPluralLock.Lock()
	defer p.crdPluralLock.Unlock()
	// Making a new copy of map and returning it to avoid concurrent map read and map write
	crdKindToPluralMapCopy := make(map[string]string, len(p.crdKindToPluralMap))
	for k, v := range p.crdKindToPluralMap {
		crdKindToPluralMapCopy[k] = v
	}
	return crdKindToPluralMapCopy
}

func (p *pluralMap) SetPluralForCRDKind(kind, plural string) {
	p.crdPluralLock.Lock()
	defer p.crdPluralLock.Unlock()

	p.crdKindToPluralMap[strings.ToLower(kind)] = plural
}
