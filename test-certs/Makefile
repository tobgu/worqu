generate-ca:
	cfssl genkey -initca ca-csr.json | cfssljson -bare ca

generate-cert:
	cfssl gencert -ca ca.pem -ca-key ca-key.pem -config ca-conf.json csr.json | cfssljson -bare host
	mv host.pem cert.pem
	mv host-key.pem key.pem

delete-unused-files:
	rm *.csr
	rm ca-key.pem

all: generate-ca generate-cert delete-unused-files
