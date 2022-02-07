package appregistration

import (
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	// RabbitMQApp registration name
	RabbitMQApp = "rabbitmq"
	// PerconaDBApp registration name
	PerconaDBApp = "perconadb"
	// PrometheusApp registration name
	PrometheusApp = "prometheus"
	// KafkaApp registration name
	KafkaApp = "kafka"
	// PostgressApp registration name
	PostgressApp = "postgress"
	// MongoDBCommunityApp registration name
	MongoDBCommunityApp = "mongodbcommunity"
)

// GetSupportedCRD returns the list of supported CRDs.
// Note: If you add support for a new CRD add it to the supported
// GroupVersionResource list as well.
func GetSupportedCRD() map[string][]stork_api.ApplicationResource {
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
	// rabbitmq app crds
	defCRD[RabbitMQApp] = []stork_api.ApplicationResource{
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "RabbitmqCluster",
				Group:   "rabbitmq.com",
				Version: "v1beta1",
			},
			KeepStatus: false,
			SuspendOptions: stork_api.SuspendOptions{
				Path: "spec.replicas",
				Type: "int",
			},
		},
	}

	// perconadb app crds
	defCRD[PerconaDBApp] = []stork_api.ApplicationResource{
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "PerconaXtraDBCluster",
				Group:   "pxc.percona.com",
				Version: "v1",
			},
			KeepStatus: false,
			SuspendOptions: stork_api.SuspendOptions{
				Path: "spec.pause",
				Type: "bool",
			},
		},
	}
	// prometheus app crds
	defCRD[PrometheusApp] = []stork_api.ApplicationResource{
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "Prometheus",
				Group:   "monitoring.coreos.com",
				Version: "v1",
			},
			KeepStatus: false,
			SuspendOptions: stork_api.SuspendOptions{
				Path: "spec.replicas",
				Type: "int",
			},
		},
	}

	// kafka app crds
	defCRD[KafkaApp] = []stork_api.ApplicationResource{
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "Kafka",
				Group:   "kafka.strimzi.io",
				Version: "v1beta2",
			},
			KeepStatus: false,
			SuspendOptions: stork_api.SuspendOptions{
				Path: "spec.kafka.replicas",
				Type: "int",
			},
		},
	}
	// postgress app crds
	defCRD[PostgressApp] = []stork_api.ApplicationResource{
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "postgresql",
				Group:   "acid.zalan.do",
				Version: "v1",
			},
			KeepStatus: false,
			SuspendOptions: stork_api.SuspendOptions{
				Path: "spec.numberOfInstances",
				Type: "int",
			},
		},
	}
	// mongdb app crds
	defCRD[MongoDBCommunityApp] = []stork_api.ApplicationResource{
		{
			GroupVersionKind: metav1.GroupVersionKind{
				Kind:    "MongoDBCommunity",
				Group:   "mongodbcommunity.mongodb.com",
				Version: "v1",
			},
			KeepStatus: false,
			SuspendOptions: stork_api.SuspendOptions{
				Path: "spec.members",
				Type: "int",
			},
		},
	}

	return defCRD
}

// GetSupportedGVR returns a list of GroupVersionResource schema for the supported CRDs
func GetSupportedGVR() map[schema.GroupVersionResource]string {

	return map[schema.GroupVersionResource]string{
		// IBM CRD's
		{
			Resource: "ibpcas",
			Group:    "ibp.com",
			Version:  "v1alpha1",
		}: "IBPCAsList",
		{
			Resource: "ibpconsoles",
			Group:    "ibp.com",
			Version:  "v1alpha1",
		}: "IBPConsolesList",
		{
			Resource: "ibporderers",
			Group:    "ibp.com",
			Version:  "v1alpha1",
		}: "IPBOrderersList",
		{
			Resource: "ibppeers",
			Group:    "ibp.com",
			Version:  "v1alpha1",
		}: "IBPeersList",
		{
			Resource: "couchbasebuckets",
			Group:    "couchbase.com",
			Version:  "v2",
		}: "CouchbaseBucketsList",
		{
			Resource: "couchbaseclusters",
			Group:    "couchbase.com",
			Version:  "v2",
		}: "CouchbaseClustersList",
		{
			Resource: "couchbaseephemeralbuckets",
			Group:    "couchbase.com",
			Version:  "v2",
		}: "CouchbaseEphemeralBucketsList",
		{
			Resource: "couchbasememcachedbuckets",
			Group:    "couchbase.com",
			Version:  "v2",
		}: "CouchbaseMemcachedBucketsList",
		{
			Resource: "couchbasereplications",
			Group:    "couchbase.com",
			Version:  "v2",
		}: "CouchbaseReplicationsList",
		{
			Resource: "couchbaseusers",
			Group:    "couchbase.com",
			Version:  "v2",
		}: "CouchbaseUsersList",
		{
			Resource: "couchbasegroups",
			Group:    "couchbase.com",
			Version:  "v2",
		}: "CouchbaseGroupsList",
		{
			Resource: "couchbaserolebindings",
			Group:    "couchbase.com",
			Version:  "v2",
		}: "CouchbaseRoleBindingsList",
		{
			Resource: "couchbasebackups",
			Group:    "couchbase.com",
			Version:  "v2",
		}: "CouchbaseBackupsList",
		{
			Resource: "couchbasebackuprestores",
			Group:    "couchbase.com",
			Version:  "v2",
		}: "CouchbaseBackupRestoresList",
		// datastax/Cassandra CRD's
		{
			Resource: "cassandradatacenters",
			Group:    "cassandra.datastax.com",
			Version:  "v1beta1",
		}: "CassandraDatacentersList",
		// redis cluster CRD's
		{
			Resource: "redisenterpriseclusters",
			Group:    "app.redislabs.com",
			Version:  "v1",
		}: "RedisEnterpriseClustersList",
		{
			Resource: "redisenterprisedatabases",
			Group:    "app.redislabs.com",
			Version:  "v1",
		}: "RedisEnterpriseDatabasesList",
		// weblogic domain crds
		{
			Resource: "domains",
			Group:    "weblogic.oracle",
			Version:  "v8",
		}: "DomainsList",
		// mongodb crds
		{
			Resource: "MongoDBCommunity",
			Group:    "mongodbcommunity.mongodb.com",
			Version:  "v1",
		}: "MongoDBCommunityList",
	}
}
