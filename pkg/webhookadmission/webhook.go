package webhookadmission

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/portworx/sched-ops/k8s/admissionregistration"
	"github.com/portworx/sched-ops/k8s/core"
	log "github.com/sirupsen/logrus"
	"k8s.io/api/admission/v1beta1"
	admissionv1beta1 "k8s.io/api/admissionregistration/v1beta1"
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
	oldSecretName            = "servercert-secret"
	secretName               = "stork-webhook-secret"
	privKey                  = "privKey"
	privCert                 = "privCert"
	defaultSkipAnnotation    = "stork.libopenstorage.org/disable-admission-controller"
)

// Controller for admission mutating webhook to initialise resources
// with stork as scheduler, if given resources are using driver supported
// by stork
type Controller struct {
	Recorder     record.EventRecorder
	Driver       volume.Driver
	server       *http.Server
	lock         sync.Mutex
	started      bool
	SkipResource string
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
	skipHookAnnotation := defaultSkipAnnotation
	if c.SkipResource != "" {
		skipHookAnnotation = c.SkipResource
	}
	webhookConfig := &admissionv1beta1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: storkAdmissionController,
		},
	}

	decoder := json.NewDecoder(req.Body)
	defer func() {
		if err := req.Body.Close(); err != nil {
			log.Warnf("Error closing decoder")
		}
	}()
	if err := decoder.Decode(&admissionReview); err != nil {
		log.Errorf("Error decoding admission review request: %v", err)
		c.Recorder.Event(webhookConfig, v1.EventTypeWarning, "invalid admission review request", err.Error())
		http.Error(w, "Decode error", http.StatusBadRequest)
		return
	}

	arReq := admissionReview.Request
	resourceName := ""
	switch arReq.Kind.Kind {
	case "StatefulSet":
		var ss appv1.StatefulSet
		if err = json.Unmarshal(arReq.Object.Raw, &ss); err != nil {
			log.Errorf("Could not unmarshal admission review object: %v", err)
			c.Recorder.Event(webhookConfig, v1.EventTypeWarning, "could not unmarshal ar object", err.Error())
			http.Error(w, "Decode error", http.StatusBadRequest)
			return
		}
		resourceName = ss.GetName()
		log.Debugf("Received admission review request for sts %s,%s", resourceName, arReq.Namespace)
		if !skipSchedulerUpdate(skipHookAnnotation, ss.ObjectMeta.Annotations) {
			isStorkResource, err = c.checkVolumeOwner(ss.Spec.Template.Spec.Volumes, arReq.Namespace)
			if err != nil {
				c.Recorder.Event(&ss, v1.EventTypeWarning, "Could not get volume owner info for ss: %v", err.Error())
				http.Error(w, "Could not get volume owner info", http.StatusInternalServerError)
				return
			}
			schedPath = appSchedPrefix + podSpecSchedPath
		}
	case "Deployment":
		var deployment appv1.Deployment
		if err := json.Unmarshal(arReq.Object.Raw, &deployment); err != nil {
			log.Errorf("Could not unmarshal admission review object: %v", err)
			c.Recorder.Event(webhookConfig, v1.EventTypeWarning, "could not unmarshal ar object", err.Error())
			http.Error(w, "Decode error", http.StatusBadRequest)
			return
		}
		resourceName = deployment.GetName()
		log.Debugf("Received admission review request for deployment %s,%s", resourceName, arReq.Namespace)
		if !skipSchedulerUpdate(skipHookAnnotation, deployment.ObjectMeta.Annotations) {
			isStorkResource, err = c.checkVolumeOwner(deployment.Spec.Template.Spec.Volumes, arReq.Namespace)
			if err != nil {
				c.Recorder.Event(&deployment, v1.EventTypeWarning, "Could not get volume owner info deployment: %v", err.Error())
				http.Error(w, "Could not get volume owner info", http.StatusInternalServerError)
				return
			}
			schedPath = appSchedPrefix + podSpecSchedPath
		}
	case "Pod":
		var pod v1.Pod
		if err := json.Unmarshal(arReq.Object.Raw, &pod); err != nil {
			log.Errorf("Could not unmarshal admission review object: %v", err)
			c.Recorder.Event(webhookConfig, v1.EventTypeWarning, "could not unmarshal ar object", err.Error())
			http.Error(w, "Decode error", http.StatusBadRequest)
			return
		}
		resourceName = pod.GetName()
		log.Debugf("Received admission review request for pod %s,%s", resourceName, arReq.Namespace)
		if !skipSchedulerUpdate(skipHookAnnotation, pod.ObjectMeta.Annotations) {
			isStorkResource, err = c.checkVolumeOwner(pod.Spec.Volumes, arReq.Namespace)
			if err != nil {
				c.Recorder.Event(&pod, v1.EventTypeWarning, "Could not get volume owner info for pod: %v", err.Error())
				http.Error(w, "Could not get volume owner info", http.StatusInternalServerError)
				return
			}
			schedPath = podSpecSchedPath
		}
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
		log.Debugf("Updating scheduler to stork for Resource:%s, Name: %s, Namespace:%s", arReq.Kind.Kind, resourceName, arReq.Namespace)
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
		pvc, err := core.Instance().GetPersistentVolumeClaim(v.PersistentVolumeClaim.ClaimName, namespace)
		if err != nil {
			return false, err
		}
		if c.Driver.OwnsPVC(core.Instance(), pvc) {
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

	ns := os.Getenv(storkNamespaceEnv)
	if ns == "" {
		ns = defaultNamespace
	}
	certSecrets, err := core.Instance().GetSecret(secretName, ns)
	if err != nil && !k8serr.IsNotFound(err) {
		log.Errorf("Unable to retrieve %v secret: %v", secretName, err)
		return err
	} else if k8serr.IsNotFound(err) {
		// create CN string
		dnsName := storkService + "." + ns + ".svc"
		caBundle, key, err = GenerateCertificate("Stork CA", dnsName)
		if err != nil {
			log.Errorf("Unable to generate x509 certificate: %v", err)
			return err
		}
		tlsCert, err = GetTLSCertificate(caBundle, key)
		if err != nil {
			log.Errorf("Unable to create tls certificate: %v", err)
			return err
		}
		_, err = CreateCertSecrets(caBundle, key, ns)
		if err != nil {
			log.Errorf("unable to create secrets for cert details: %v", err)
			return err
		}
	} else {
		if secretData, ok = certSecrets.Data[privKey]; !ok {
			return fmt.Errorf("invalid secret key data")
		}
		if caBundle, ok = certSecrets.Data[privCert]; !ok {
			return fmt.Errorf("invalid secret certificate")
		}

		tlsCert, err = GetTLSCertificate(caBundle, secretData)
		if err != nil {
			log.Errorf("unable to generate tls certs: %v", err)
			return err
		}
	}
	// Cleanup the old webhook cert
	err = core.Instance().DeleteSecret(oldSecretName, ns)
	if err != nil && !k8serr.IsNotFound(err) {
		log.Warnf("Failed to delete old webhook secret: %v", err)
	}
	c.server = &http.Server{Addr: ":443",
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{tlsCert}}}

	http.HandleFunc("/mutate", c.serveHTTP)
	go func() {
		if err := c.server.ListenAndServeTLS("", ""); err != http.ErrServerClosed {
			log.Errorf("Error starting webhook server: %v", err)
		}
	}()
	c.started = true
	log.Debugf("Webhook server started")
	return CreateMutateWebhook(caBundle, ns)
}

// Stop Stops the webhook server
func (c *Controller) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.started {
		return fmt.Errorf("webhook server has not been started")
	}
	if err := admissionregistration.Instance().DeleteMutatingWebhookConfiguration(storkAdmissionController); err != nil {
		log.Errorf("unable to delete webhook configuration, %v", err)
		return err
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

func skipSchedulerUpdate(skipHookAnnotation string, annotations map[string]string) bool {
	if annotations != nil {
		if value, ok := annotations[skipHookAnnotation]; ok {
			if skip, err := strconv.ParseBool(value); err == nil && skip {
				return true
			}
		}
	}
	return false
}
