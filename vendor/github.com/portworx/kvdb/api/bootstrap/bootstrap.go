package api

import (
	"time"
)

// BootstrapEntry identifies a node with its IP and ID which is part of a kvdb cluster
type BootstrapEntry struct {
	// IP of the kvdb node
	IP string
	// ID of the kvdb node
	ID string
	// Index of the kvdb node
	Index int
	// State indicates the state of the Node in the bootstrap state machine
	State NodeState
	// Type indicates the type of kvdb node
	Type NodeType
	// Ts is the last updated timestamp of this bootstrap entry
	Ts time.Time
	// Version is the bootstrap entry version
	Version string
	// used only for tests
	// PeerPort is the peer port for kvdb node
	PeerPort string `json:"peerport,omitemty"`
	// ClientPort is the client port for kvdb node
	ClientPort string `json:"clientport,omitempty"`
	// Domain is the domain name advertised in the peer urls for this kvdb node
	// The DomainName is only used for kvdb's peer urls.
	// This enables us to change the actual peer IP being used by the nodes while keeping the domain
	// name the same. The client url is always based of an IP.
	Domain DomainName
	// DataDirType is the type of data directory being used by internal kvdb on
	// this node
	DataDirType Type
}

// NodeState is the state of a kvdb bootstrap node
type NodeState int

const (
	// BootstrapNodeStateNone indicates the init/none state
	BootstrapNodeStateNone NodeState = iota
	// BootstrapNodeStateInProgress bootstrap is in progress
	BootstrapNodeStateInProgress
	// BootstrapNodeStateOperational kvdb node is operational
	BootstrapNodeStateOperational
	// BootstrapNodeStateSuspectDown node is down
	BootstrapNodeStateSuspectDown
)

// NodeType is a type that identifies the bootstrap node type
type NodeType int

const (
	// BootstrapNodeTypeNone in default none type
	BootstrapNodeTypeNone NodeType = iota
	// BootstrapNodeTypeLeader node is a kvdb cluster leader
	BootstrapNodeTypeLeader
	// BootstrapNodeTypeMember node is a kvdb cluster member
	BootstrapNodeTypeMember
)

// Type of datadir
type Type string

// DomainName identifies the DNS name internal kvdb node will use in the peer urls
type DomainName string

const (
	// MetadataDevice when using a metadata device for internal kvdb data
	MetadataDevice Type = "MetadataDevice"
	// KvdbDevice when using a dedicated kvdb device for internal kvdb data
	KvdbDevice Type = "KvdbDevice"
	// BtrfsSubvolume when using btrfs subvolume from PX data drives for internal kvdb data
	BtrfsSubvolume Type = "BtrfsSubvolume"
)
