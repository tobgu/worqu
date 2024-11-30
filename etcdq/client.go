package etcdq

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	etcdlogutil "go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"os"
	"strings"
	"time"
)

func NewClient(c Config) (*clientv3.Client, error) {
	var clientCerts []tls.Certificate
	if c.TLSKeyFile != "" && c.TLSCertFile != "" {
		// Load client cert
		cert, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("unable to load cert: %w", err)
		}

		clientCerts = []tls.Certificate{cert}
	}

	var clientCertPool *x509.CertPool
	if c.TLSCACertFile != "" {
		// Load our CA certificate
		caBytes, err := os.ReadFile(c.TLSCACertFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read CA cert file: %w", err)
		}
		clientCertPool = x509.NewCertPool()
		clientCertPool.AppendCertsFromPEM(caBytes)
	}

	var tlsConfig *tls.Config
	if clientCerts != nil || clientCertPool != nil {
		tlsConfig = &tls.Config{
			Certificates: clientCerts,
			RootCAs:      clientCertPool,
			MinVersion:   tls.VersionTLS12,
		}
	}

	hosts := strings.Split(c.EtcdHosts, ",")
	etcdlogutil.DefaultZapLoggerConfig.Encoding = "console" // Default is json, keep all other settings
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   hosts,
		DialTimeout: time.Duration(c.EtcdTimeoutSeconds) * time.Second,
		TLS:         tlsConfig,
	})

	if err != nil {
		return nil, fmt.Errorf("connecting to ETCD hosts %w", err)
	}

	return client, nil
}
