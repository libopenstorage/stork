package controllers

import (
	"strings"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// IBMApp registration name
	IBMApp = "ibm"
	// CouchBaseApp registration name
	CouchBaseApp = "couchbase"
	// RedisClusterApp registration name
	RedisClusterApp = "redis"
	// CassandraApp registration name
	CassandraApp = "cassandra"
	// WeblogicDomainApp registration name
	WeblogicDomainApp = "weblogic"
)

func getSupportedCRD() map[string][]stork_api.ApplicationResource {
	// supported CRD registration
	defCRD := make(map[string][]stork_api.ApplicationResource)
	// IBM CRD's
	defCRD[IBMApp] = []stork_api.ApplicationResource{
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "IBPCA",
				Group:   "ibp.com",
				Version: "v1alpha1",
			},
			KeepStatus: false,
			SuspendOptions: stork_api.SuspendOptions{
				Path: "spec.replicas",
				Type: "int",
			},
		},
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "IBPConsole",
				Group:   "ibp.com",
				Version: "v1alpha1",
			},
			KeepStatus: false,
			SuspendOptions: stork_api.SuspendOptions{
				Path: "spec.replicas",
				Type: "int",
			},
		},
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "IBPOrderer",
				Group:   "ibp.com",
				Version: "v1alpha1",
			},
			KeepStatus: false,
			SuspendOptions: stork_api.SuspendOptions{
				Path: "spec.replicas",
				Type: "int",
			},
		},
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "IBPPeer",
				Group:   "ibp.com",
				Version: "v1alpha1",
			},
			KeepStatus: false,
			SuspendOptions: stork_api.SuspendOptions{
				Path: "spec.replicas",
				Type: "int",
			},
		},
	}
	//CouchBase CRD's
	defCRD[CouchBaseApp] = []stork_api.ApplicationResource{
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "CouchbaseBucket",
				Group:   "couchbase.com",
				Version: "v2",
			},
			KeepStatus: false,
		},
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "CouchbaseCluster",
				Group:   "couchbase.com",
				Version: "v2",
			},
			KeepStatus: false,
			SuspendOptions: stork_api.SuspendOptions{
				Path: "spec.paused",
				Type: "bool",
			},
			PodsPath: "status.members.ready",
		},
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "CouchbaseEphemeralBucket",
				Group:   "couchbase.com",
				Version: "v2",
			},
			KeepStatus: false,
		},
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "CouchbaseMemcachedBucket",
				Group:   "couchbase.com",
				Version: "v2",
			},
			KeepStatus: false,
		},
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "CouchbaseReplication",
				Group:   "couchbase.com",
				Version: "v2",
			},
			KeepStatus: false,
		},
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "CouchbaseUser",
				Group:   "couchbase.com",
				Version: "v2",
			},
			KeepStatus: false,
		},
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "CouchbaseGroup",
				Group:   "couchbase.com",
				Version: "v2",
			},
			KeepStatus: false,
		},
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "CouchbaseRoleBinding",
				Group:   "couchbase.com",
				Version: "v2",
			},
			KeepStatus: false,
		},
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "CouchbaseBackup",
				Group:   "couchbase.com",
				Version: "v2",
			},
			KeepStatus: false,
		},
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "CouchbaseBackupRestore",
				Group:   "couchbase.com",
				Version: "v2",
			},
			KeepStatus: false,
		},
	}
	// datastax/Cassandra CRD's
	defCRD[CassandraApp] = []stork_api.ApplicationResource{
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "CassandraDatacenter",
				Group:   "cassandra.datastax.com",
				Version: "v1beta1",
			},
			KeepStatus: false,
			SuspendOptions: stork_api.SuspendOptions{
				Path: "spec.stopped",
				Type: "bool",
			},
		},
	}
	// redis cluster CRD's
	defCRD[RedisClusterApp] =
		[]stork_api.ApplicationResource{
			{
				GroupVersionKind: metav1.GroupVersionKind{
					Kind:    "RedisEnterpriseCluster",
					Group:   "app.redislabs.com",
					Version: "v1",
				},
				KeepStatus: false,
			},
			{
				GroupVersionKind: metav1.GroupVersionKind{
					Kind:    "RedisEnterpriseDatabase",
					Group:   "app.redislabs.com",
					Version: "v1",
				},
				KeepStatus: false,
			},
		}
		// weblogic domain crds
	defCRD[WeblogicDomainApp] = []stork_api.ApplicationResource{
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "Domain",
				Group:   "weblogic.oracle",
				Version: "v8",
			},
			KeepStatus: false,
			SuspendOptions: stork_api.SuspendOptions{
				Path:  "spec.serverStartPolicy",
				Type:  "string",
				Value: "NEVER",
			},
		},
	}
	return defCRD
}

func getRegisteredCRD() (map[string][]stork_api.ApplicationResource, error) {
	// skipCrds grp:version map
	skipCrds := map[string]string{
		"autopilot.libopenstorage.org":           "",
		"core.libopenstorage.org":                "",
		"volumesnapshot.external-storage.k8s.io": "",
		"stork.libopenstorage.org":               "",
	}
	regCRD := make(map[string][]stork_api.ApplicationResource)
	crds, err := apiextensions.Instance().ListCRDs()
	if err != nil {
		return regCRD, err
	}

	for _, crd := range crds.Items {
		// skip stork/volumesnap crd registration
		if _, ok := skipCrds[crd.Spec.Group]; ok {
			continue
		}
		var appList []stork_api.ApplicationResource
		for _, version := range crd.Spec.Versions {
			appRes := stork_api.ApplicationResource{
				GroupVersionKind: metav1.GroupVersionKind{
					Group:   crd.Spec.Group,
					Version: version.Name,
					Kind:    crd.Spec.Names.Kind,
				},
			}
			appList = append(appList, appRes)
		}
		regCRD[strings.ToLower(crd.Spec.Names.Kind)] = appList
	}
	return regCRD, nil
}

// RegisterDefaultCRDs  registered already supported CRDs
func RegisterDefaultCRDs() error {
	for name, res := range getSupportedCRD() {
		appReg := &stork_api.ApplicationRegistration{
			Resources: res,
		}
		appReg.Name = name
		if _, err := stork.Instance().CreateApplicationRegistration(appReg); err != nil && !errors.IsAlreadyExists(err) {
			logrus.Errorf("unable to register app %v, err: %v", appReg, err)
			return err
		}
	}

	regCrds, err := getRegisteredCRD()
	if err != nil {
		return err
	}
	// register all crds on found on k8s server
	for name, res := range regCrds {
		appReg := &stork_api.ApplicationRegistration{
			Resources: res,
		}
		appReg.Name = name
		if _, err := stork.Instance().CreateApplicationRegistration(appReg); err != nil && !errors.IsAlreadyExists(err) {
			logrus.Errorf("unable to register app %v, err: %v", appReg, err)
			return err
		}
	}
	return nil
}
