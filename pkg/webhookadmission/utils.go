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

	"github.com/portworx/sched-ops/k8s/admissionregistration"
	"github.com/portworx/sched-ops/k8s/core"
	log "github.com/sirupsen/logrus"
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

// CreateMutateWebhook create new webhookconfig for stork if not exist already
func CreateMutateWebhook(caBundle []byte, ns string) error {
	path := "/mutate"
	// We make best efforts to change incoming apps scheduler to stork, if application is
	// using stork supported storage drivers.
	sideEffect := admissionv1beta1.SideEffectClassNoneOnDryRun
	webhook := admissionv1beta1.MutatingWebhook{
		Name: webhookName,
		ClientConfig: admissionv1beta1.WebhookClientConfig{
			Service: &admissionv1beta1.ServiceReference{
				Name:      storkService,
				Namespace: ns,
				Path:      &path,
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

	resp, err := admissionregistration.Instance().GetMutatingWebhookConfiguration(storkAdmissionController)
	if err != nil {
		if k8serr.IsNotFound(err) {
			_, err = admissionregistration.Instance().CreateMutatingWebhookConfiguration(req)
		}
		return err
	}
	req.ResourceVersion = resp.ResourceVersion
	if _, err := admissionregistration.Instance().UpdateMutatingWebhookConfiguration(req); err != nil {
		log.Errorf("unable to update webhook configuration: %v", err)
		return err
	}
	log.Debugf("stork webhook configured: %v", webhookName)
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
