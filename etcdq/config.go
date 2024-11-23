package etcdq

type Config struct {
	EtcdHosts          string
	EtcdTimeoutSeconds int
	TLSCertFile        string
	TLSKeyFile         string
	TLSCACertFile      string
	MaxTaskConcurrency int
}
