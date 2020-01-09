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

	"github.com/portworx/sched-ops/k8s"
	log "github.com/sirupsen/logrus"
	hook "k8s.io/api/admissionregistration/v1beta1"
	v1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	webhookName        = "stork.webhook.com"
	storkService       = "stork-service"
	storkNamespace     = "kube-system"
	storkServiceAccess = "stork-service.kube-system.svc"
)

// CreateMutateWebhook create new webhookconfig for stork if not exist already
func CreateMutateWebhook(caBundle []byte) error {
	path := "/mutate"

	webhook := hook.Webhook{
		Name: webhookName,
		ClientConfig: hook.WebhookClientConfig{
			Service: &hook.ServiceReference{
				Name:      storkService,
				Namespace: storkNamespace,
				Path:      &path,
			},
			CABundle: caBundle,
		},
		Rules: []hook.RuleWithOperations{
			{
				Operations: []hook.OperationType{hook.Create},
				Rule: hook.Rule{
					APIGroups:   []string{"apps", ""},
					APIVersions: []string{"v1"},
					Resources:   []string{"deployments", "statefulset", "pods"},
				},
			},
		},
	}
	req := &hook.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: storkAdmissionController,
		},
		Webhooks: []hook.Webhook{webhook},
	}
	_, err := k8s.Instance().CreateMutatingWebhookConfiguration(req)
	if !k8serr.IsAlreadyExists(err) && err != nil {
		log.Errorf("unable to create mutate webhook: %v", err)
		return err
	}
	log.Debugf("Created mutating webhook configuration: %v", webhookName)
	return nil
}

// GenerateCertificate Self Signed certificate , returns x509 cert
// and priv key in PEM format
func GenerateCertificate() ([]byte, []byte, error) {
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
			CommonName: storkServiceAccess,
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		log.Fatalf("Failed to create x509 certificate: %s", err)
		return nil, nil, err
	}

	err = pem.Encode(pemCert, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	if err != nil {
		log.Errorf("Unable to encode x509 certificate to PEM format: %v", err)
		return nil, nil, err
	}

	b, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		log.Fatalf("Unable to marshal ECDSA private key: %v", err)
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
func CreateCertSecrets(cert, key []byte) (*v1.Secret, error) {
	secretData := make(map[string][]byte)
	secretData[privKey] = key
	secretData[privCert] = cert
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: storkNamespace,
		},
		Data: secretData,
	}
	return k8s.Instance().CreateSecret(secret)
}
