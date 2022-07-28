// +kubebuilder:object:generate=true
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PXBucketClaim is a user's request for a bucket
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=pbc
// +groupName=object.portworx.io
// +kubebuilder:printcolumn:name="Provisioned",type=string,JSONPath=`.status.provisioned`,description="Indicates whether the bucket has been provisioned for this claim"
// +kubebuilder:printcolumn:name="BucketID",type=string,JSONPath=`.status.bucketId`,description="Indicates the bucket ID for this provisioned bucketclaim"
// +kubebuilder:printcolumn:name="BackendType",type=string,JSONPath=`.status.backendType`,description="Indicates the backend type for this provisioned bucketclaim"
type PXBucketClaim struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec defines the desired characteristics of a bucket requested by a user.
	// Required.
	Spec BucketClaimSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status represents the current information of a bucket.
	// +optional
	Status *BucketClaimStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// PXBucketClaimList is a list of PXBucketClaim objects
type PXBucketClaimList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// List of PXBucketClaims
	Items []PXBucketClaim `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// BucketClaimSpec describes the common attributes of a bucket claim.
type BucketClaimSpec struct {
	// BucketClassName is the name of the PXBucketClass
	// requested by the PXBucketClaim.
	// Required.
	BucketClassName string `json:"bucketClassName,omitempty" protobuf:"bytes,1,opt,name=bucketClassName"`
}

// BucketStatus is the status of the PXBucketClaim
type BucketClaimStatus struct {
	// provisioned indicates if the bucket is created.
	// +optional
	Provisioned bool `json:"provisioned,omitempty" protobuf:"varint,1,opt,name=provisioned"`

	// bucketId indicates the bucket ID
	// +optional
	BucketID string `json:"bucketId,omitempty" protobuf:"varint,2,opt,name=bucketId"`

	// region indicates the region where the bucket is created.
	// +optional
	Region string `json:"region,omitempty" protobuf:"varint,3,opt,name=region"`

	// DeletionPolicy is the deletion policy that the PXBucketClaim was created with
	// +optional
	DeletionPolicy DeletionPolicy `json:"deletionPolicy" protobuf:"bytes,4,opt,name=deletionPolicy"`

	// BackendType is the backend type that this PXBucketClaim was created with
	// +optional
	BackendType string `json:"backendType" protobuf:"varint,5,opt,name=backendType"`

	// Endpoint is the endpoint that this bucket was provisioned with
	// +optional
	Endpoint string `json:"endpoint" protobuf:"varint,6,opt,name=endpoint"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PXBucketClass is a user's template for a bucket
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=pbclass
// +groupName=object.portworx.io
// +kubebuilder:printcolumn:name="DeletionPolicy",type=string,JSONPath=`.deletionPolicy`,description="The deletion policy for this bucket class"
type PXBucketClass struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Region defines the region to use
	// +optional
	Region string `json:"region" protobuf:"bytes,2,opt,name=region"`

	// deletionPolicy determines whether the underlying bucket should be deleted
	// when a PXBucketClaim is deleted.
	// "Retain" means that the underyling storage bucket is kept.
	// "Delete" means that the underlying storage bucket is deleted.
	// Required.
	DeletionPolicy DeletionPolicy `json:"deletionPolicy" protobuf:"bytes,3,opt,name=deletionPolicy"`

	// parameters is a key-value map with storage driver specific parameters for creating snapshots.
	// These values are opaque to Kubernetes.
	// +optional
	Parameters map[string]string `json:"parameters,omitempty" protobuf:"bytes,4,rep,name=parameters"`
}

// DeletionPolicy describes a policy for end-of-life maintenance of volume snapshot contents
// +kubebuilder:validation:Enum=Delete;Retain
type DeletionPolicy string

const (
	// PXBucketClaimDelete means the underlying bucket will be deleted
	PXBucketClaimDelete DeletionPolicy = "Delete"

	// PXBucketClaimRetain means the underyling bucket will be kept
	PXBucketClaimRetain DeletionPolicy = "Retain"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// PXBucketClassList is a list of PXBucketClass objects
type PXBucketClassList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// List of PXBucketClaims
	Items []PXBucketClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PXBucketAccess is a user's request to access a bucket
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=pba
// +groupName=object.portworx.io
// +kubebuilder:printcolumn:name="AccessGranted",type=boolean,JSONPath=`.status.accessGranted`,description="Indicates if access has been granted for a given bucket"
// +kubebuilder:printcolumn:name="CredentialsSecretName",type=string,JSONPath=`.status.credentialsSecretName`,description="The secret with connection info for the bucket"
// +kubebuilder:printcolumn:name="BucketID",type=string,JSONPath=`.status.bucketId`,description="The bucket ID for this access object"
// +kubebuilder:printcolumn:name="BackendType",type=string,JSONPath=`.status.backendType`,description="The backend type for this access object"
type PXBucketAccess struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec defines the desired characteristics of a bucket requested by a user.
	// Required.
	Spec BucketAccessSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// spec defines the desiredPXBucketAccess
	Status *BucketAccessStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// BucketClaimSpec describes the common attributes of a bucket access.
type BucketAccessSpec struct {
	// BucketClassName is the name of the PXBucketClass
	// requested by the PXBucketAccess.
	// Required.
	BucketClassName string `json:"bucketClassName,omitempty" protobuf:"bytes,1,opt,name=bucketClassName"`

	// BucketClaimName is the name of the BucketClaim to
	// provide access to.
	// +optional
	BucketClaimName string `json:"bucketClaimName,omitempty" protobuf:"bytes,2,opt,name=bucketClaimName"`

	// ExistingBucketId is the bucket ID to provide access to.
	// +optional
	ExistingBucketId string `json:"existingBucketId,omitempty" protobuf:"bytes,3,opt,name=existingBucketId"`
}

// BucketStatus is the status of the PXBucketClaim
type BucketAccessStatus struct {
	// accessGranted indicates if the bucket access is created.
	// +optional
	AccessGranted bool `json:"accessGranted,omitempty" protobuf:"varint,1,opt,name=accessGranted"`

	// credentialsSecretName is a reference to the secret name with bucketaccess
	// +optional
	CredentialsSecretName string `json:"credentialsSecretName,omitempty" protobuf:"varint,2,opt,name=credentialsSecretName"`

	// accountId is a reference to the account ID for this access
	// +optional
	AccountId string `json:"accountId,omitempty" protobuf:"varint,3,opt,name=accountId"`

	// bucketId is a reference to the bucket ID for this access
	// +optional
	BucketId string `json:"bucketId,omitempty" protobuf:"varint,4,opt,name=bucketId"`

	// backendType is the backend type that this PXBucketClaim was created with
	// +optional
	BackendType string `json:"backendType" protobuf:"bytes,5,opt,name=backendType"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// PXBucketAccessList is a list of PXBucketAccess objects
type PXBucketAccessList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// List of PXBucketAccess
	Items []PXBucketAccess `json:"items" protobuf:"bytes,2,rep,name=items"`
}
