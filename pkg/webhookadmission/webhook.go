package webhookadmission

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/portworx/sched-ops/k8s"
	log "github.com/sirupsen/logrus"
	"k8s.io/api/admission/v1beta1"
	appv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
)

const (
	mutateWebHook            = "/mutate"
	validateWebHook          = "/validate"
	appSchedPrefix           = "/spec/template"
	podSpecSchedPath         = "/spec/schedulerName"
	storkScheduler           = "stork"
	storkAdmissionController = "stork-webhooks-cfg"
	secretName               = "servercert-secret"
	privKey                  = "privKey"
	privCert                 = "privCert"
)

// Controller for admission mutating webhook to initialise resources
// with stork as scheduler, if given resources are using driver supported
// by stork
type Controller struct {
	Recorder record.EventRecorder
	Driver   volume.Driver
	server   *http.Server
	lock     sync.Mutex
	started  bool
}

// Serve method for webhook server
func (c *Controller) serveHTTP(w http.ResponseWriter, req *http.Request) {
	if strings.Contains(req.URL.Path, mutateWebHook) {
		c.processMutateRequest(w, req)
	} else {
		http.Error(w, "Unsupported request", http.StatusNotFound)
	}
}

func (c *Controller) processMutateRequest(w http.ResponseWriter, req *http.Request) {
	var admissionResponse *v1beta1.AdmissionResponse
	var err error
	var schedPath string
	admissionReview := v1beta1.AdmissionReview{}
	isStorkResource := false

	decoder := json.NewDecoder(req.Body)
	defer func() {
		if err := req.Body.Close(); err != nil {
			log.Warnf("Error closing decoder")
		}
	}()
	if err := decoder.Decode(&admissionReview); err != nil {
		log.Errorf("Error decoding admission review request: %v", err)
		c.Recorder.Event(&admissionReview, v1.EventTypeWarning, "invalid admission review request", err.Error())
		http.Error(w, "Decode error", http.StatusBadRequest)
		return
	}

	// TODO: This log does not get reflected in stork pods, check other logger
	arReq := admissionReview.Request
	switch arReq.Kind.Kind {
	case "StatefulSet":
		var ss appv1.StatefulSet
		if err = json.Unmarshal(arReq.Object.Raw, &ss); err != nil {
			log.Errorf("Could not unmarshal admission review object: %v", err)
			c.Recorder.Event(&admissionReview, v1.EventTypeWarning, "could not unmarshal ar object", err.Error())
		}
		isStorkResource, err = c.checkVolumeOwner(ss.Spec.Template.Spec.Volumes, ss.Namespace)
		if err != nil {
			c.Recorder.Event(&admissionReview, v1.EventTypeWarning, "Could not get volume owner info %v", err.Error())
		}
		schedPath = appSchedPrefix + podSpecSchedPath
	case "Deployment":
		var deployment appv1.Deployment
		if err := json.Unmarshal(arReq.Object.Raw, &deployment); err != nil {
			log.Errorf("Could not unmarshal admission review object: %v", err)
			c.Recorder.Event(&admissionReview, v1.EventTypeWarning, "could not unmarshal ar object", err.Error())
		}
		isStorkResource, err = c.checkVolumeOwner(deployment.Spec.Template.Spec.Volumes, deployment.Namespace)
		if err != nil {
			c.Recorder.Event(&admissionReview, v1.EventTypeWarning, "Could not get volume owner info %v", err.Error())
		}
		schedPath = appSchedPrefix + podSpecSchedPath
	case "Pod":
		var pod v1.Pod
		if err := json.Unmarshal(arReq.Object.Raw, &pod); err != nil {
			log.Errorf("Could not unmarshal admission review object: %v", err)
			c.Recorder.Event(&admissionReview, v1.EventTypeWarning, "could not unmarshal ar object", err.Error())
		}
		isStorkResource, err = c.checkVolumeOwner(pod.Spec.Volumes, pod.Namespace)
		if err != nil {
			c.Recorder.Event(&admissionReview, v1.EventTypeWarning, "Could not get volume owner info %v", err.Error())
		}
		schedPath = podSpecSchedPath
	}

	if !isStorkResource {
		// ignore for non driver application + resources other than depoy/ss
		admissionResponse = &v1beta1.AdmissionResponse{
			Result: &metav1.Status{
				Message: "Ignoring backends which are not supported by stork ",
			},
			Allowed: true,
		}
	} else {
		// create patch
		patch := createPatch(schedPath)
		admissionResponse = &v1beta1.AdmissionResponse{
			Result: &metav1.Status{
				Message: "Successful",
			},
			Patch:   patch,
			Allowed: true,
			PatchType: func() *v1beta1.PatchType {
				pt := v1beta1.PatchTypeJSONPatch
				return &pt
			}(),
		}
	}

	admissionResponse.UID = arReq.UID
	admissionReview.Response = admissionResponse
	resp, err := json.Marshal(admissionReview)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not marshal response: %v", err), http.StatusInternalServerError)
	}
	if _, err := w.Write(resp); err != nil {
		http.Error(w, fmt.Sprintf("could not write http response: %v", err), http.StatusInternalServerError)
	}
}

func (c *Controller) checkVolumeOwner(volumes []v1.Volume, namespace string) (bool, error) {
	// check whether pod spec use stork driver volume claims
	for _, v := range volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}
		pvc, err := k8s.Instance().GetPersistentVolumeClaim(v.PersistentVolumeClaim.ClaimName, namespace)
		if err != nil {
			return false, err
		}
		if c.Driver.OwnsPVC(pvc) {
			return true, nil
		}
	}
	return false, nil
}

// Start Starts the Webhook server
func (c *Controller) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	var secretData, key, caBundle []byte
	var err error
	var ok bool
	var tlsCert tls.Certificate
	if c.started {
		return fmt.Errorf("webhook server has already been started")
	}
	certSecrets, err := k8s.Instance().GetSecret(secretName, storkNamespace)
	if err != nil && !k8serr.IsNotFound(err) {
		log.Fatalf("Unable to retrieve %v secret: %v", secretName, err)
	} else if k8serr.IsNotFound(err) {
		caBundle, key, err = GenerateCertificate()
		if err != nil {
			log.Fatalf("Unable to generate x509 certificate: %v", err)
		}
		tlsCert, err = GetTLSCertificate(caBundle, key)
		if err != nil {
			log.Fatalf("Unable to create tls certificate: %v", tlsCert)
		}
		_, err = CreateCertSecrets(caBundle, key)
		if err != nil {
			log.Fatalf("unable to create secrets for cert details: %v", err)
		}
	} else {
		if secretData, ok = certSecrets.Data[privKey]; !ok {
			log.Fatalf("invalid secret key data")
		}
		if caBundle, ok = certSecrets.Data[privCert]; !ok {
			log.Fatalf("invalid secret certificate")
		}

		tlsCert, err = GetTLSCertificate(caBundle, secretData)
		if err != nil {
			log.Fatalf("unable to generate tls certs: %v", err)
		}
	}
	c.server = &http.Server{Addr: ":443",
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{tlsCert}}}

	http.HandleFunc("/mutate", c.serveHTTP)
	go func() {
		if err := c.server.ListenAndServeTLS("", ""); err != http.ErrServerClosed {
			log.Panicf("Error starting webhook server: %v", err)
		}
	}()
	c.started = true
	log.Debugf("Webhook server started")

	return CreateMutateWebhook(caBundle)
}

// Stop Stops the webhook server
func (c *Controller) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.started {
		return fmt.Errorf("webhook server has not been started")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.server.Shutdown(ctx); err != nil {
		return err
	}
	c.started = false
	return nil
}

// createJson patch to update container spec scheduler path
func createPatch(schedpath string) []byte {
	p := []map[string]string{}
	patch := map[string]string{
		"op":    "replace",
		"path":  schedpath,
		"value": storkScheduler,
	}
	p = append(p, patch)
	b, err := json.Marshal(p)
	if err != nil {
		log.Errorf("could not marshal patch: %v", err)
	}
	return b
}
