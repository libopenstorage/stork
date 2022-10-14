package controller

import (
	"context"
	"fmt"
	"strconv"

	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/api/server/sdk"
	"github.com/libopenstorage/openstorage/pkg/grpcserver"
	"github.com/portworx/px-object-controller/client/apis/objectservice/v1alpha1"
	crdv1alpha1 "github.com/portworx/px-object-controller/client/apis/objectservice/v1alpha1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/portworx/px-object-controller/pkg/utils"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	commonObjectServiceKeyPrefix = "object.portworx.io/"
	backendTypeKey               = commonObjectServiceKeyPrefix + "backend-type"
	endpointKey                  = commonObjectServiceKeyPrefix + "endpoint"
	clearBucketKey               = commonObjectServiceKeyPrefix + "clear-bucket"

	commonObjectServiceFinalizerKeyPrefix = "finalizers.object.portworx.io/"
	accessGrantedFinalizer                = commonObjectServiceFinalizerKeyPrefix + "access-granted"
	bucketProvisionedFinalizer            = commonObjectServiceFinalizerKeyPrefix + "bucket-provisioned"
	accessSecretFinalizer                 = commonObjectServiceFinalizerKeyPrefix + "access-secret"
)

var allowedDrivers = map[string]bool{
	"S3Driver":     true,
	"PureFBDriver": true,
}

func (ctrl *Controller) deleteBucket(ctx context.Context, pbc *crdv1alpha1.PXBucketClaim) error {

	if pbc.Status == nil || !pbc.Status.Provisioned {
		logrus.WithContext(ctx).Infof("bucket not yet provisioned. skipping backened delete")
		ctrl.bucketStore.Delete(pbc)
		return nil
	}

	// Issue delete if provisioned and deletionPolicy is delete
	if pbc.Status.DeletionPolicy == crdv1alpha1.PXBucketClaimRetain {
		logrus.WithContext(ctx).Infof("skipping delete bucket as deletionPolicy was retain")

		err := ctrl.removeBucketFinalizers(ctx, pbc)
		if err != nil {
			errMsg := fmt.Sprintf("bucket claim %s/%s remove finalizer failed: %v", pbc.Namespace, pbc.Name, err)
			logrus.WithContext(ctx).Errorf(errMsg)
			ctrl.eventRecorder.Event(pbc, v1.EventTypeWarning, "DeleteBucketError", errMsg)
			return err
		}

		ctrl.bucketStore.Delete(pbc)
		return nil
	}

	clearBucket := false
	if clearBucketVal, ok := pbc.Annotations[clearBucketKey]; ok {
		var err error
		clearBucket, err = strconv.ParseBool(clearBucketVal)
		if err != nil {
			logrus.Errorf("invalid value %s for %s, defaulting to false: %v", clearBucketVal, clearBucketKey, err)
		}
	}

	// Provisioned and deletionPolicy is delete. Delete the bucket here.
	_, err := ctrl.bucketClient.DeleteBucket(ctx, &api.BucketDeleteRequest{
		BucketId:    pbc.Status.BucketID,
		Region:      pbc.Status.Region,
		Endpoint:    pbc.Status.Endpoint,
		ClearBucket: clearBucket,
	})
	if err != nil {
		errMsg := fmt.Sprintf("delete bucket %s failed: %v", pbc.Name, err)
		logrus.WithContext(ctx).Errorf(errMsg)
		ctrl.eventRecorder.Event(pbc, v1.EventTypeWarning, "DeleteBucketError", errMsg)
		return err
	}

	err = ctrl.removeBucketFinalizers(ctx, pbc)
	if err != nil {
		errMsg := fmt.Sprintf("bucket claim %s/%s remove finalizer failed: %v", pbc.Namespace, pbc.Name, err)
		logrus.WithContext(ctx).Errorf(errMsg)
		ctrl.eventRecorder.Event(pbc, v1.EventTypeWarning, "DeleteBucketError", errMsg)
		return err
	}

	ctrl.bucketStore.Delete(pbc)
	logrus.WithContext(ctx).Infof("bucket %q deleted", pbc.Name)

	return nil
}

func (ctrl *Controller) createBucket(ctx context.Context, pbc *crdv1alpha1.PXBucketClaim, pbclass *crdv1alpha1.PXBucketClass) error {
	bucketID := getBucketID(pbc)

	_, err := ctrl.bucketClient.CreateBucket(ctx, &api.BucketCreateRequest{
		Name:     bucketID,
		Region:   pbclass.Region,
		Endpoint: pbclass.Parameters[endpointKey],
	})
	if err != nil {
		logrus.WithContext(ctx).Infof("create bucket %s failed: %v", pbc.Name, err)
		ctrl.eventRecorder.Event(pbc, v1.EventTypeWarning, "CreateBucketError", fmt.Sprintf("failed to create bucket: %v", err))
		return err
	}

	logrus.WithContext(ctx).Infof("bucket %q created", pbc.Name)
	if pbc.Status == nil {
		pbc.Status = &crdv1alpha1.BucketClaimStatus{}
	}
	pbc.Status.Provisioned = true
	pbc.Status.Region = pbclass.Region
	pbc.Status.DeletionPolicy = pbclass.DeletionPolicy
	pbc.Status.BucketID = bucketID
	pbc.Status.BackendType = pbclass.Parameters[backendTypeKey]
	pbc.Status.Endpoint = pbclass.Parameters[endpointKey]
	pbc.Finalizers = append(pbc.Finalizers, bucketProvisionedFinalizer)
	if pbc.Annotations == nil {
		pbc.Annotations = make(map[string]string)
	}
	if clearBucketVal, ok := pbclass.Parameters[clearBucketKey]; ok {
		pbc.Annotations[clearBucketKey] = clearBucketVal
	}
	pbc, err = ctrl.k8sBucketClient.ObjectV1alpha1().PXBucketClaims(pbc.Namespace).Update(ctx, pbc, metav1.UpdateOptions{})
	if err != nil {
		ctrl.eventRecorder.Event(pbc, v1.EventTypeWarning, "CreateBucketError", fmt.Sprintf("failed to update bucket: %v", err))
		return err
	}

	_, err = ctrl.storeBucketUpdate(pbc)
	if err != nil {
		return err
	}

	ctrl.eventRecorder.Event(pbc, v1.EventTypeNormal, "CreateBucketSuccess", fmt.Sprintf("successfully provisioned bucket %v", bucketID))
	return nil
}

func (ctrl *Controller) setupContextFromValue(ctx context.Context, backendType string) context.Context {
	return grpcserver.AddMetadataToContext(ctx, sdk.ContextDriverKey, backendType)
}

func (ctrl *Controller) setupContextFromClass(ctx context.Context, pbclass *crdv1alpha1.PXBucketClass) (context.Context, error) {
	backendTypeValue, ok := pbclass.Parameters[backendTypeKey]
	if !ok {
		err := fmt.Errorf("PXBucketClass parameter %s is unset", backendTypeKey)
		logrus.WithContext(ctx).Error(err)

		return ctx, err
	}

	if _, ok = allowedDrivers[backendTypeValue]; !ok {
		err := fmt.Errorf("PXBucketClass parameter %s is invalid. Possible values are: %v", backendTypeKey, allowedDrivers)
		logrus.WithContext(ctx).Error(err)

		return ctx, err
	}

	logrus.WithContext(ctx).Infof("bucket driver %v selected", backendTypeValue)
	return grpcserver.AddMetadataToContext(ctx, sdk.ContextDriverKey, backendTypeValue), nil
}

func getAccountName(namespace *v1.Namespace) string {
	return fmt.Sprintf("px-os-account-%v", namespace.GetUID())
}

func getCredentialsSecretName(pba *crdv1alpha1.PXBucketAccess) string {
	if pba.Status != nil && pba.Status.CredentialsSecretName != "" {
		return pba.Status.CredentialsSecretName
	}
	return fmt.Sprintf("px-os-credentials-%s", pba.Name)
}

func (ctrl *Controller) createAccess(ctx context.Context, pba *crdv1alpha1.PXBucketAccess, pbclass *crdv1alpha1.PXBucketClass, bucketID string) error {
	// Get namespace UID for multitenancy
	namespace, err := ctrl.k8sClient.CoreV1().Namespaces().Get(ctx, pba.Namespace, metav1.GetOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("failed to get namespace during grant bucket access %s: %v", pba.Name, err)
		logrus.WithContext(ctx).Errorf(errMsg)
		ctrl.eventRecorder.Event(pba, v1.EventTypeWarning, "GrantAccessError", errMsg)
		return err
	}

	resp, err := ctrl.bucketClient.AccessBucket(ctx, &api.BucketGrantAccessRequest{
		BucketId:    bucketID,
		AccountName: getAccountName(namespace),
	})
	if err != nil {
		errMsg := fmt.Sprintf("create bucket access %s failed: %v", pba.Name, err)
		logrus.WithContext(ctx).Errorf(errMsg)
		ctrl.eventRecorder.Event(pba, v1.EventTypeWarning, "GrantAccessError", errMsg)
		return err
	}

	accessData := make(map[string]string)
	accessData["access-key-id"] = resp.Credentials.GetAccessKeyId()
	accessData["secret-access-key"] = resp.Credentials.GetSecretAccessKey()
	accessData["endpoint"] = pbclass.Parameters[endpointKey]
	accessData["region"] = pbclass.Region
	accessData["bucket-id"] = bucketID

	// If secret exists, update it.
	credentialsSecretName := getCredentialsSecretName(pba)
	secret, err := ctrl.k8sClient.CoreV1().Secrets(pba.Namespace).Get(ctx, credentialsSecretName, metav1.GetOptions{})
	if k8s_errors.IsNotFound(err) {
		// Create if it doesn't exist
		secret, err = ctrl.k8sClient.CoreV1().Secrets(pba.Namespace).Create(
			ctx,
			&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:       credentialsSecretName,
					Namespace:  pba.Namespace,
					Finalizers: []string{accessSecretFinalizer},
				},
				StringData: accessData,
			},
			metav1.CreateOptions{},
		)
		if err != nil {
			errMsg := fmt.Sprintf("failed to create access secret for bucket access %s/%s: %v", pba.Namespace, pba.Name, err)
			ctrl.eventRecorder.Event(pba, v1.EventTypeWarning, "GrantAccessError", errMsg)
			return err
		}
	} else if err != nil {
		errMsg := fmt.Sprintf("failed to get secret for bucket access %s/%s: %v", pba.Namespace, pba.Name, err)
		ctrl.eventRecorder.Event(pba, v1.EventTypeWarning, "GrantAccessError", errMsg)
		return err
	}

	logrus.WithContext(ctx).Infof("bucket access %q created", pba.Name)
	if pba.Status == nil {
		pba.Status = &crdv1alpha1.BucketAccessStatus{}
	}
	pba.Status.AccessGranted = true
	pba.Status.CredentialsSecretName = secret.Name
	pba.Status.AccountId = resp.GetAccountId()
	pba.Status.BucketId = bucketID
	pba.Status.BackendType = pbclass.Parameters[backendTypeKey]
	pba.Finalizers = []string{accessGrantedFinalizer}
	pba, err = ctrl.k8sBucketClient.ObjectV1alpha1().PXBucketAccesses(pba.Namespace).Update(ctx, pba, metav1.UpdateOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("failed to update bucket access %s/%s: %v", pba.Namespace, pba.Name, err)
		ctrl.eventRecorder.Event(pba, v1.EventTypeWarning, "GrantAccessError", errMsg)
		return err
	}

	_, err = ctrl.storeAccessUpdate(pba)
	if err != nil {
		return err
	}

	ctrl.eventRecorder.Event(pba, v1.EventTypeNormal, "GrantAccessSuccess", fmt.Sprintf("successfully granted access to BucketClaim %s with BucketAccess %s", pba.Spec.BucketClaimName, pba.Name))
	return nil
}

func (ctrl *Controller) revokeAccess(ctx context.Context, pba *crdv1alpha1.PXBucketAccess) error {

	if pba.Status == nil || !pba.Status.AccessGranted {
		logrus.WithContext(ctx).Infof("bucket not yet provisioned. skipping backened delete")
		err := ctrl.removeAccessFinalizers(ctx, pba)
		if err != nil {
			return err
		}

		return ctrl.accessStore.Delete(pba)
	}

	// Provisioned and deletionPolicy is delte. Delete the bucket here.
	_, err := ctrl.bucketClient.RevokeBucket(ctx, &api.BucketRevokeAccessRequest{
		BucketId:  pba.Status.BucketId,
		AccountId: pba.Status.AccountId,
	})
	if err != nil {
		errMsg := fmt.Sprintf("revoke bucket %s failed: %v", pba.Name, err)
		logrus.WithContext(ctx).Errorf(errMsg)
		ctrl.eventRecorder.Event(pba, v1.EventTypeWarning, "RevokeAccessError", errMsg)
		return err
	}

	err = ctrl.removeSecretFinalizersAndDelete(ctx, pba.Status.CredentialsSecretName, pba.Namespace)
	if err != nil {
		errMsg := fmt.Sprintf("bucket access secret %s delete failed: %v", pba.Status.CredentialsSecretName, err)
		logrus.WithContext(ctx).Errorf(errMsg)
		ctrl.eventRecorder.Event(pba, v1.EventTypeWarning, "RevokeAccessError", errMsg)
		return err
	}

	err = ctrl.removeAccessFinalizers(ctx, pba)
	if err != nil {
		errMsg := fmt.Sprintf("bucket access %s/%s remove finalizer failed: %v", pba.Namespace, pba.Name, err)
		logrus.WithContext(ctx).Errorf(errMsg)
		ctrl.eventRecorder.Event(pba, v1.EventTypeWarning, "RevokeAccessError", errMsg)
		return err
	}

	ctrl.accessStore.Delete(pba)
	logrus.WithContext(ctx).Infof("bucket access %q deleted", pba.Name)

	return nil
}

func (ctrl *Controller) storeBucketUpdate(bucket interface{}) (bool, error) {
	return utils.StoreObjectUpdate(ctrl.bucketStore, bucket, "bucket")
}

func (ctrl *Controller) storeAccessUpdate(access interface{}) (bool, error) {
	return utils.StoreObjectUpdate(ctrl.accessStore, access, "access")
}

func (ctrl *Controller) removeBucketFinalizers(ctx context.Context, pbc *crdv1alpha1.PXBucketClaim) error {
	pbc, err := ctrl.k8sBucketClient.ObjectV1alpha1().PXBucketClaims(pbc.Namespace).Get(ctx, pbc.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	pbc.Finalizers = []string{}
	_, err = ctrl.k8sBucketClient.ObjectV1alpha1().PXBucketClaims(pbc.Namespace).Update(ctx, pbc, metav1.UpdateOptions{})
	return err
}

func (ctrl *Controller) removeAccessFinalizers(ctx context.Context, pba *crdv1alpha1.PXBucketAccess) error {
	pba, err := ctrl.k8sBucketClient.ObjectV1alpha1().PXBucketAccesses(pba.Namespace).Get(ctx, pba.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	pba.Finalizers = []string{}
	_, err = ctrl.k8sBucketClient.ObjectV1alpha1().PXBucketAccesses(pba.Namespace).Update(ctx, pba, metav1.UpdateOptions{})
	return err
}

func (ctrl *Controller) removeSecretFinalizersAndDelete(ctx context.Context, secretName, secretNamespace string) error {
	secret, err := ctrl.k8sClient.CoreV1().Secrets(secretNamespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	secret.Finalizers = []string{}
	secret, err = ctrl.k8sClient.CoreV1().Secrets(secret.Namespace).Update(ctx, secret, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	err = ctrl.k8sClient.CoreV1().Secrets(secret.Namespace).Delete(ctx, secret.Name, metav1.DeleteOptions{})
	if k8s_errors.IsNotFound(err) {
		logrus.WithContext(ctx).Infof("bucket access secret %s/%s already deleted", secretNamespace, secretName)
	} else if err != nil {
		return err
	}

	return nil
}

func getBucketID(pbc *v1alpha1.PXBucketClaim) string {
	return fmt.Sprintf("px-os-%s", pbc.GetUID())
}
