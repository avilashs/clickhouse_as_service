package agent

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
)

const (
	caSecretNamespace = "basalt-system"
	caSecretName      = "basalt-cell-ca"
	sanHashAnnotation = "basalt.io/san-sha256"
	certValidity      = 90 * 24 * time.Hour
	certRenewBefore   = 30 * 24 * time.Hour
)

// CA is the cell's certificate authority. It never leaves the cell: the control plane only
// learns its public certificate (to hand to tenants), so a control-plane compromise cannot
// mint server certificates. cert-manager would take this role, but its images (quay.io) are
// not reachable from this build environment; see docs/m1-report.md.
type CA struct {
	Cert    *x509.Certificate
	Key     *ecdsa.PrivateKey
	CertPEM []byte
}

func randSerial() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
}

// EnsureCA loads the cell CA from its Secret or creates it once.
func EnsureCA(ctx context.Context, kc kubernetes.Interface, cellID string) (*CA, error) {
	s, err := kc.CoreV1().Secrets(caSecretNamespace).Get(ctx, caSecretName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, err
		}
		serial, err := randSerial()
		if err != nil {
			return nil, err
		}
		tmpl := &x509.Certificate{
			SerialNumber: serial, Subject: pkix.Name{CommonName: "basalt " + cellID + " CA"},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(10, 0, 0),
			IsCA: true, BasicConstraintsValid: true, MaxPathLenZero: true,
			KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			return nil, err
		}
		kb, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			return nil, err
		}
		s = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: caSecretName, Namespace: caSecretNamespace},
			Type:       corev1.SecretTypeTLS,
			Data: map[string][]byte{
				"tls.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
				"tls.key": pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}),
			},
		}
		if _, err := kc.CoreV1().Secrets(caSecretNamespace).Create(ctx, s, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		// re-read: another agent replica may have won the create race
		return EnsureCA(ctx, kc, cellID)
	}
	if err != nil {
		return nil, err
	}
	cb, _ := pem.Decode(s.Data["tls.crt"])
	kb, _ := pem.Decode(s.Data["tls.key"])
	if cb == nil || kb == nil {
		return nil, errors.New("cell CA secret is malformed")
	}
	cert, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, err
	}
	key, err := x509.ParseECPrivateKey(kb.Bytes)
	if err != nil {
		return nil, err
	}
	return &CA{Cert: cert, Key: key, CertPEM: s.Data["tls.crt"]}, nil
}

func sanHash(names []string) string {
	s := append([]string{}, names...)
	sort.Strings(s)
	h := sha256.Sum256([]byte(strings.Join(s, ",")))
	return hex.EncodeToString(h[:])
}

// EnsureCert keeps the requested TLS Secret valid: it (re)issues only when the Secret is
// missing, covers different names, or expires within certRenewBefore. Re-issuing in place
// lets ClickHouse pick up new files without a restart.
func (ca *CA) EnsureCert(ctx context.Context, kc kubernetes.Interface, req engine.CertRequest, labels map[string]string) error {
	want := sanHash(req.DNSNames)
	cur, err := kc.CoreV1().Secrets(req.Namespace).Get(ctx, req.SecretName, metav1.GetOptions{})
	exists := err == nil
	if exists && cur.Annotations[sanHashAnnotation] == want {
		if b, _ := pem.Decode(cur.Data["server.crt"]); b != nil {
			if c, err := x509.ParseCertificate(b.Bytes); err == nil && time.Until(c.NotAfter) > certRenewBefore {
				return nil
			}
		}
	} else if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, err := randSerial()
	if err != nil {
		return err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: req.DNSNames[0]}, DNSNames: req.DNSNames,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(certValidity),
		KeyUsage: x509.KeyUsageDigitalSignature,
		// clientAuth too: replicas and Keeper peers dial each other with the same identity
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		return err
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: req.SecretName, Namespace: req.Namespace, Labels: labels,
			Annotations: map[string]string{sanHashAnnotation: want}},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"ca.crt":     ca.CertPEM,
			"server.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
			"server.key": pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}),
		},
	}
	if exists {
		s.ResourceVersion = cur.ResourceVersion
		_, err = kc.CoreV1().Secrets(req.Namespace).Update(ctx, s, metav1.UpdateOptions{})
	} else {
		_, err = kc.CoreV1().Secrets(req.Namespace).Create(ctx, s, metav1.CreateOptions{})
	}
	if err != nil {
		return fmt.Errorf("write %s/%s: %w", req.Namespace, req.SecretName, err)
	}
	return nil
}

// Pool returns a cert pool containing the CA.
func (ca *CA) Pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(ca.Cert)
	return p
}
