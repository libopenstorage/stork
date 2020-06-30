package controllers

import (
	"strings"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
)

const (
	// IBMApp registration name
	IBMApp = "ibm-app-reg"
	// CouchBaseApp registration name
	CouchBaseApp = "couchbase-app-reg"
	// RedisClusterApp registration name
	RedisClusterApp = "redis-app-reg"
	// CassandraApp registration name
	CassandraApp = "cassandra-app-reg"
)

func getSupportedCRD() map[string][]string {
	// Default CRD registration of format,
	// {kind, crdName, suspendPath,suspendType(bool/int)}
	defCRD := make(map[string][]string)
	// IBM CRD's
	defCRD[IBMApp] = []string{"IBPCA,ibpcas.ibp.com,ibp.com/v1alpha1",
		"IBPConsole,ibpconsoles.ibp.com,ibp.com/v1alpha1",
		"IBPOrderer,ibporderers.ibp.com,ibp.com/v1alpha1",
		"IBPPeer,ibppeers.ibp.com,ibp.com/v1alpha1",
	}
	//CouchBase CRD's
	defCRD[CouchBaseApp] = []string{"CouchbaseBucket,couchbasebuckets.couchbase.com,couchbase.com/v2",
		"CouchbaseCluster,couchbaseclusters.couchbase.com,couchbase.com/v1,spec.suspend,true",
		"CouchbaseCluster,couchbaseclusters.couchbase.com,couchbase.com/v2,spec.suspend,true",
		"CouchbaseEphemeralBucket,couchbaseephemeralbuckets.couchbase.com,couchbase.com/v2",
		"CouchbaseMemcachedBucket,couchbasememcachedbuckets.couchbase.com,couchbase.com/v2",
		"CouchbaseReplication,couchbasereplications.couchbase.com,couchbase.com/v2",
		"CouchbaseUser,couchbaseusers.couchbase.com,couchbase.com/v2",
		"CouchbaseGroup,couchbasegroups.couchbase.com,couchbase.com/v2",
		"CouchbaseRoleBinding,couchbaserolebindings.couchbase.com,couchbase.com/v2",
		"CouchbaseBackup,couchbasebackups.couchbase.com,couchbase.com/v2",
		"CouchbaseBackupRestore,couchbasebackuprestores.couchbase.com,couchbase.com/v2",
	}
	// datastax/Cassandra CRD's
	defCRD[CassandraApp] = []string{"CassandraDatacenter,cassandradatacenters.cassandra.datastax.com,cassandra.datastax.com/v1beta1,spec.stopped,bool"}
	// redis cluster CRD's
	defCRD[RedisClusterApp] = []string{"RedisEnterpriseCluster,redisenterpriseclusters.app.redislabs.com,app.redislabs.com/v1",
		"RedisEnterpriseDatabase,redisenterprisedatabases.app.redislabs.com,app.redislabs.com/v1"}

	return defCRD
}

// RegisterDefaultCRDs  registered already supported CRDs
func RegisterDefaultCRDs() error {
	for name, res := range getSupportedCRD() {
		var resources []stork_api.ApplicationResource
		for _, reg := range res {
			cr := strings.Split(reg, ",")
			if len(cr) < 3 {
				continue
			}
			var appReg stork_api.ApplicationResource
			appReg.Kind = cr[0]
			appReg.Group = cr[1]
			appReg.Version = cr[2]
			if len(cr) == 5 {
				appReg.SuspendOptions = stork_api.SuspendOptions{Path: cr[3], Type: cr[4]}
			}
			resources = append(resources, appReg)
		}

		appReg := &stork_api.ApplicationRegistration{
			Resources: resources,
		}
		appReg.Name = name
		if _, err := stork.Instance().CreateApplicationRegistration(appReg); err != nil && !errors.IsAlreadyExists(err) {
			logrus.Errorf("unable to register app %v, err: %v", appReg, err)
			return err
		}
	}
	return nil
}
