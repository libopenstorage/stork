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
	defCRD[IBMApp] = []string{"IBPCA,ibpcas.ibp.com",
		"IBPConsole,ibpconsoles.ibp.com",
		"IBPOrderer,ibporderers.ibp.com",
		"IBPPeer,ibppeers.ibp.com",
	}
	//CouchBase CRD's
	defCRD[CouchBaseApp] = []string{"CouchbaseBucket,couchbasebuckets.couchbase.com",
		"CouchbaseCluster,couchbaseclusters.couchbase.com,spec.suspend,true",
		"CouchbaseEphemeralBucket,couchbaseephemeralbuckets.couchbase.com",
		"CouchbaseMemcachedBucket,couchbasememcachedbuckets.couchbase.com",
		"CouchbaseReplication,couchbasereplications.couchbase.com",
		"CouchbaseUser,couchbaseusers.couchbase.com",
		"CouchbaseGroup,couchbasegroups.couchbase.com",
		"CouchbaseRoleBinding,couchbaserolebindings.couchbase.com",
		"CouchbaseBackup,couchbasebackups.couchbase.com",
		"CouchbaseBackupRestore,couchbasebackuprestores.couchbase.com",
	}
	// datastax/Cassandra CRD's
	defCRD[CassandraApp] = []string{"CassandraDatacenter,cassandradatacenters.cassandra.datastax.com,spec.stopped,bool"}
	// redis cluster CRD's
	defCRD[RedisClusterApp] = []string{"RedisEnterpriseCluster,redisenterpriseclusters.app.redislabs.com",
		"RedisEnterpriseDatabase,redisenterprisedatabases.app.redislabs.com"}

	return defCRD
}

// RegisterDefaultCRDs  registered already supported CRDs
func RegisterDefaultCRDs() error {
	for name, res := range getSupportedCRD() {
		var resources []stork_api.ApplicationResource
		for _, reg := range res {
			cr := strings.Split(reg, ",")
			if len(cr) < 2 {
				continue
			}
			var appReg stork_api.ApplicationResource
			appReg.Kind = cr[0]
			appReg.Group = cr[1]
			if len(cr) == 4 {
				appReg.SuspendOptions = stork_api.SuspendOptions{Path: cr[2], Type: cr[3]}
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
