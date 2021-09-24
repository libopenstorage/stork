package webhookadmission

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"time"

	"github.com/libopenstorage/stork/pkg/version"
	"github.com/portworx/sched-ops/k8s/admissionregistration"
	"github.com/portworx/sched-ops/k8s/core"
	log "github.com/sirupsen/logrus"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	admissionv1beta1 "k8s.io/api/admissionregistration/v1beta1"
	v1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	webhookName       = "webhook.stork.libopenstorage.org"
	storkService      = "stork-service"
	storkNamespaceEnv = "STORK-NAMESPACE"
	defaultNamespace  = "kube-system"
)

var (
	webhookPath = "/mutate"
)

// CreateMutateWebhook create new webhookconfig for stork if not exist already
func CreateMutateWebhook(caBundle []byte, ns string) error {

	ok, err := version.RequiresV1Registration()
	if err != nil {
		return err
	}
	if ok {
		// register v1 crds
		return createWebhookV1(caBundle, ns)
	}
	// We make best efforts to change incoming apps scheduler to stork, if application is
	// using stork supported storage drivers.
	sideEffect := admissionv1beta1.SideEffectClassNoneOnDryRun
	webhook := admissionv1beta1.MutatingWebhook{
		Name: webhookName,
		ClientConfig: admissionv1beta1.WebhookClientConfig{
			Service: &admissionv1beta1.ServiceReference{
				Name:      storkService,
				Namespace: ns,
				Path:      &webhookPath,
			},
			CABundle: caBundle,
		},
		Rules: []admissionv1beta1.RuleWithOperations{
			{
				Operations: []admissionv1beta1.OperationType{admissionv1beta1.Create},
				Rule: admissionv1beta1.Rule{
					APIGroups:   []string{"apps", ""},
					APIVersions: []string{"v1"},
					Resources:   []string{"deployments", "statefulsets", "pods"},
				},
			},
		},
		SideEffects: &sideEffect,
	}
	req := &admissionv1beta1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: storkAdmissionController,
		},
		Webhooks: []admissionv1beta1.MutatingWebhook{webhook},
	}

	resp, err := admissionregistration.Instance().GetMutatingWebhookConfigurationV1beta1(storkAdmissionController)
	if err != nil {
		if k8serr.IsNotFound(err) {
			_, err = admissionregistration.Instance().CreateMutatingWebhookConfigurationV1beta1(req)
		}
		return err
	}
	req.ResourceVersion = resp.ResourceVersion
	if _, err := admissionregistration.Instance().UpdateMutatingWebhookConfigurationV1beta1(req); err != nil {
		log.Errorf("unable to update webhook configuration: %v", err)
		return err
	}
	log.Debugf("stork webhook v1beta1 configured: %v", webhookName)
	return nil
}

// GenerateCertificate Self Signed certificate using given CN, returns x509 cert
// and priv key in PEM format
func GenerateCertificate(cn string, dnsName string) ([]byte, []byte, error) {
	var err error
	pemCert := &bytes.Buffer{}

	// generate private key
	priv, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		log.Errorf("error generating crypt keys: %v", err)
		return nil, nil, err
	}

	// create certificate
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: cn,
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{dnsName},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		log.Errorf("Failed to create x509 certificate: %s", err)
		return nil, nil, err
	}

	err = pem.Encode(pemCert, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	if err != nil {
		log.Errorf("Unable to encode x509 certificate to PEM format: %v", err)
		return nil, nil, err
	}

	b, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		log.Errorf("Unable to marshal ECDSA private key: %v", err)
		return nil, nil, err
	}
	privBlock := pem.Block{
		Type:    "RSA PRIVATE KEY",
		Headers: nil,
		Bytes:   b,
	}

	return pemCert.Bytes(), pem.EncodeToMemory(&privBlock), nil
}

// GetTLSCertificate from pub and priv key
func GetTLSCertificate(cert, priv []byte) (tls.Certificate, error) {
	return tls.X509KeyPair(cert, priv)
}

// CreateCertSecrets creates k8s secret to store self signed cert
// data
func CreateCertSecrets(cert, key []byte, ns string) (*v1.Secret, error) {
	secretData := make(map[string][]byte)
	secretData[privKey] = key
	secretData[privCert] = cert
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: ns,
		},
		Data: secretData,
	}
	return core.Instance().CreateSecret(secret)
}

func createWebhookV1(caBundle []byte, ns string) error {
	// We make best efforts to change incoming apps scheduler to stork, if application is
	// using stork supported storage drivers.
	sideEffect := admissionv1.SideEffectClassNoneOnDryRun
	failurePolicy := admissionv1.Ignore
	matchPolicy := admissionv1.Exact
	webhook := admissionv1.MutatingWebhook{
		Name: webhookName,
		ClientConfig: admissionv1.WebhookClientConfig{
			Service: &admissionv1.ServiceReference{
				Name:      storkService,
				Namespace: ns,
				Path:      &webhookPath,
			},
			CABundle: caBundle,
		},
		Rules: []admissionv1.RuleWithOperations{
			{
				Operations: []admissionv1.OperationType{admissionv1.Create},
				Rule: admissionv1.Rule{
					APIGroups:   []string{"apps", ""},
					APIVersions: []string{"v1"},
					Resources:   []string{"deployments", "statefulsets", "pods"},
				},
			},
		},
		SideEffects:             &sideEffect,
		FailurePolicy:           &failurePolicy,
		AdmissionReviewVersions: []string{"v1"},
		MatchPolicy:             &matchPolicy,
	}
	req := &admissionv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: storkAdmissionController,
		},
		Webhooks: []admissionv1.MutatingWebhook{webhook},
	}

	// recreate webhook
	err := admissionregistration.Instance().DeleteMutatingWebhookConfiguration(storkAdmissionController)
	if err != nil && !k8serr.IsNotFound(err) {
		return err
	}
	if _, err := admissionregistration.Instance().CreateMutatingWebhookConfiguration(req); err != nil {
		log.Errorf("unable to create webhook configuration: %v", err)
		return err
	}
	log.Debugf("stork webhook v1 configured: %v", webhookName)
	return nil
}
