package objectcontroller

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"time"

	"github.com/libopenstorage/openstorage/volume/drivers/pwx"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	register "github.com/portworx/px-object-controller/client/apis/objectservice/v1alpha1"
	pxobjectcontroller "github.com/portworx/px-object-controller/pkg/controller"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	"github.com/zoido/yag-config"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"

	"k8s.io/apimachinery/pkg/api/errors"
)

const (
	envWorkerThreads      = "WORKER_THREADS"
	envRetryIntervalStart = "RETRY_INTERVAL_START"
	envRetryIntervalMax   = "RETRY_INTERVAL_MAX"

	BucketClassResourceName   = "pxbucketclass"
	BucketClassResourcePlural = "pxbucketclasses"
	BucketClassResourceShort  = "pbclass"

	BucketClaimResourceName   = "pxbucketclaim"
	BucketClaimResourcePlural = "pxbucketclaims"
	BucketClaimResourceShort  = "pbc"

	BucketAccessResourceName   = "pxbucketaccess"
	BucketAccessResourcePlural = "pxbucketaccesses"
	BucketAccessResourceShort  = "pba"

	validateCRDInterval time.Duration = 5 * time.Second
	validateCRDTimeout  time.Duration = 1 * time.Minute
)

var (
	workers            = 4
	retryIntervalStart = 1 * time.Second
	retryIntervalMax   = 5 * time.Minute
)

// ObjectController initializes px-object-controller on PX SDK Server
type ObjectController struct {
}

func parseFlags() error {
	y := yag.New()

	y.Int(&workers, envWorkerThreads, "Number of worker threads.")
	y.Duration(&retryIntervalStart, envRetryIntervalStart, "Initial retry interval of failed bucket creation/access or deletion/revoke. It doubles with each failure, up to retry-interval-max. Default is 1 second.")
	y.Duration(&retryIntervalMax, envRetryIntervalMax, "Maximum retry interval of failed bucket/access creation or deletion/revoke. Default is 5 minutes.")

	return y.ParseEnv()
}

func (o *ObjectController) createCRD() error {
	logrus.Infof("Creating px-object-controller CRDs.")
	pbclass := apiextensions.CustomResource{
		Name:       BucketClassResourceName,
		Plural:     BucketClassResourcePlural,
		ShortNames: []string{BucketClassResourceShort},
		Group:      register.GroupName,
		Version:    stork_api.SchemeGroupVersion.Version,
		Scope:      apiextensionsv1beta1.ClusterScoped,
		Kind:       reflect.TypeOf(register.PXBucketClass{}).Name(),
	}
	pbclassPrinterColumns := []apiextensionsv1.CustomResourceColumnDefinition{
		{Name: "DeletionPolicy",
			Type:     "string",
			JSONPath: ".deletionPolicy",
		},
	}
	err := k8sutils.CreateCRDWithAdditionalPrinterColumns(pbclass, pbclassPrinterColumns)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	if err := apiextensions.Instance().ValidateCRD(pbclass.Plural+"."+pbclass.Group, validateCRDTimeout, validateCRDInterval); err != nil {
		return err
	}

	pbc := apiextensions.CustomResource{
		Name:       BucketClaimResourceName,
		Plural:     BucketClaimResourcePlural,
		ShortNames: []string{BucketClaimResourceShort},
		Group:      register.GroupName,
		Version:    stork_api.SchemeGroupVersion.Version,
		Scope:      apiextensionsv1beta1.NamespaceScoped,
		Kind:       reflect.TypeOf(register.PXBucketClaim{}).Name(),
	}
	pbcPrinterColumns := []apiextensionsv1.CustomResourceColumnDefinition{
		{Name: "Provisioned",
			Type:     "string",
			JSONPath: ".status.provisioned",
		},
		{Name: "BucketID",
			Type:     "string",
			JSONPath: ".status.bucketId",
		},
		{Name: "BackendType",
			Type:     "string",
			JSONPath: ".status.backendType",
		},
	}
	err = k8sutils.CreateCRDWithAdditionalPrinterColumns(pbc, pbcPrinterColumns)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	if err := apiextensions.Instance().ValidateCRD(pbc.Plural+"."+pbc.Group, validateCRDTimeout, validateCRDInterval); err != nil {
		return err
	}

	pba := apiextensions.CustomResource{
		Name:       BucketAccessResourceName,
		Plural:     BucketAccessResourcePlural,
		ShortNames: []string{BucketAccessResourceShort},
		Group:      register.GroupName,
		Version:    stork_api.SchemeGroupVersion.Version,
		Scope:      apiextensionsv1beta1.NamespaceScoped,
		Kind:       reflect.TypeOf(register.PXBucketAccess{}).Name(),
	}
	pbaPrinterColumns := []apiextensionsv1.CustomResourceColumnDefinition{
		{Name: "AccessGranted",
			Type:     "boolean",
			JSONPath: ".status.accessGranted",
		},
		{Name: "CredentialsSecretName",
			Type:     "string",
			JSONPath: ".status.credentialsSecretName",
		},
		{Name: "BucketID",
			Type:     "string",
			JSONPath: ".status.bucketId",
		},
		{Name: "BackendType",
			Type:     "string",
			JSONPath: ".status.backendType",
		},
	}
	err = k8sutils.CreateCRDWithAdditionalPrinterColumns(pba, pbaPrinterColumns)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	if err := apiextensions.Instance().ValidateCRD(pba.Plural+"."+pba.Group, validateCRDTimeout, validateCRDInterval); err != nil {
		return err
	}

	return nil
}

func getSdkEndpoint() (string, error) {
	kubeOps := core.Instance()
	connectionConfig := pwx.NewConnectionParamsBuilderDefaultConfig()

	paramsBuilder, err := pwx.NewConnectionParamsBuilder(kubeOps, connectionConfig)
	if err != nil {
		return "", err
	}

	_, sdkEndpoint, err := paramsBuilder.BuildClientsEndpoints()
	if err != nil {
		return "", err
	}
	return sdkEndpoint, nil
}

// Init initializes the PX Object Controller
func (o *ObjectController) Init() error {
	if err := parseFlags(); err != nil {
		return fmt.Errorf("failed to parse configuration variables. %v", err)
	}

	err := o.createCRD()
	if err != nil {
		return fmt.Errorf("px-object-controller CRD generation failed. %v", err)
	}

	sdkEndpoint, err := getSdkEndpoint()
	if err != nil {
		return fmt.Errorf("error getting SDK endpoint for px-object-controller. %v", err)
	}

	logrus.Infof("Creating a new px-object-controller")
	// Create controller object
	ctrl, err := pxobjectcontroller.New(&pxobjectcontroller.Config{
		SdkEndpoint:        sdkEndpoint,
		RetryIntervalStart: retryIntervalStart,
		RetryIntervalMax:   retryIntervalMax,
	})
	if err != nil {
		return fmt.Errorf("error creating px-object-controller with SDK server endpoint: %s. %v", sdkEndpoint, err)
	}

	// Callback to start controller & sdk in goroutine
	run := func(context.Context) {
		// Run controller
		stopCh := make(chan struct{})
		go ctrl.Run(workers, stopCh)

		// Until SIGINT
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		close(stopCh)
	}
	go run(context.Background())
	logrus.Infof("Started px-object-controller with SDK server endpoint : %s", sdkEndpoint)

	return nil
}
