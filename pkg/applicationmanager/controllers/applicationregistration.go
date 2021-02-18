package controllers

import (
	"fmt"
	"strings"
	"time"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
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

// WatchFunc is a callback provided to the Watch functions
type WatchFunc func(object runtime.Object)

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

func registerCRD(crd apiextensionsv1beta1.CustomResourceDefinition) error {
	regCRD := make(map[string][]stork_api.ApplicationResource)
	var appList []stork_api.ApplicationResource
	// read all CRs from crdv1beta versions
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
	return createAppReg(regCRD)
}

func registerCRDV1(crd apiextensionsv1.CustomResourceDefinition) error {
	regCRD := make(map[string][]stork_api.ApplicationResource)
	var appList []stork_api.ApplicationResource
	// read all CRs from crdv1 versions
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
	return createAppReg(regCRD)
}

func createAppReg(regCRD map[string][]stork_api.ApplicationResource) error {
	// register all CR found in CRD
	for name, res := range regCRD {
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

// RegisterDefaultCRDs  registered already supported CRDs
func RegisterDefaultCRDs() error {
	// skipCrds grp:version map
	skipCrds := map[string]string{
		"autopilot.libopenstorage.org":           "",
		"core.libopenstorage.org":                "",
		"volumesnapshot.external-storage.k8s.io": "",
		"stork.libopenstorage.org":               "",
	}
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

	// create appreg for already registered crd
	crds, err := apiextensions.Instance().ListCRDs()
	if err != nil {
		return err
	}

	for _, crd := range crds.Items {
		// skip stork/volumesnap crd registration
		if _, ok := skipCrds[crd.Spec.Group]; ok {
			continue
		}
		if err := registerCRD(crd); err != nil {
			return err
		}
	}
	fn := func(object runtime.Object) {
		if crd, ok := object.(*apiextensionsv1.CustomResourceDefinition); ok {
			if _, ok := skipCrds[crd.Spec.Group]; ok {
				return
			}
			if err := registerCRDV1(*crd); err != nil {
				logrus.WithError(err).Error("unable to create appreg for v1 crd")
			}
		} else if crd, ok := object.(*apiextensionsv1beta1.CustomResourceDefinition); ok {
			if _, ok := skipCrds[crd.Spec.Group]; ok {
				return
			}
			if err := registerCRD(*crd); err != nil {
				logrus.WithError(err).Error("unable to create appreg for v1beta1 crd")
			}

		} else {
			err := fmt.Errorf("invalid object type on crd watch: %v", object)
			logrus.WithError(err).Error("unable to start watch")
		}
	}
	// watch and registered newly created CRD's on the fly
	return watchCRDs(fn)
}

func watchCRDs(fn WatchFunc) error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error getting cluster config: %v", err)
	}

	srcClnt, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return err
	}
	listOptions := metav1.ListOptions{Watch: true}
	watchInterface, err := srcClnt.ApiextensionsV1beta1().CustomResourceDefinitions().Watch(listOptions)
	if err != nil {
		logrus.WithError(err).Error("error invoking the watch api for crds", err)
		return err
	}
	// fire of watch interface
	go handleCRDWatch(watchInterface, fn)
	return nil
}
func handleCRDWatch(watchInterface watch.Interface, fn WatchFunc) {
	defer watchInterface.Stop()
	for {
		event, more := <-watchInterface.ResultChan()
		if !more {
			logrus.Debug("CRD watch closed (attempting to re-establish)")
			t := func() (interface{}, bool, error) {
				return "", true, watchCRDs(fn)
			}
			if _, err := task.DoRetryWithTimeout(t, 10*time.Minute, 10*time.Second); err != nil {
				logrus.WithError(err).Error("Could not re-establish the watch")
			} else {
				logrus.Debug("watch re-established")
			}
			return
		}
		fn(event.Object)

	}
}
