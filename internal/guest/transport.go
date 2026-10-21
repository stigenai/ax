package guest

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// TransportCredentials turns an AX_SERVER or AX_ROUTER address into a gRPC
// target and the credentials to dial it.
//
// An https:// address uses TLS and port 443 if it names no port. A bare
// host:port with port 443 also uses TLS. An http:// address and any other
// bare address stay plaintext. TLS trusts the system roots plus every
// certificate in the PEM file named by AX_CA_FILE. A bad AX_CA_FILE is an
// error. TLS never skips certificate verification.
func TransportCredentials(addr string) (string, credentials.TransportCredentials, error) {
	var host, port string
	switch {
	case strings.HasPrefix(addr, "https://"):
		u, err := url.Parse(addr)
		if err != nil || u.Hostname() == "" {
			return "", nil, fmt.Errorf("invalid address %q", addr)
		}
		host, port = u.Hostname(), u.Port()
		if port == "" {
			port = "443"
		}
	case strings.HasPrefix(addr, "http://"):
		return strings.TrimPrefix(addr, "http://"), insecure.NewCredentials(), nil
	default:
		h, p, err := net.SplitHostPort(addr)
		if err != nil || p != "443" {
			return addr, insecure.NewCredentials(), nil
		}
		host, port = h, p
	}

	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool()
	}
	if f := os.Getenv("AX_CA_FILE"); f != "" {
		pem, err := os.ReadFile(f)
		if err != nil {
			return "", nil, fmt.Errorf("reading AX_CA_FILE %s: %w", f, err)
		}
		if !roots.AppendCertsFromPEM(pem) {
			return "", nil, fmt.Errorf("AX_CA_FILE %s holds no PEM certificate", f)
		}
	}
	creds := credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: host, MinVersion: tls.VersionTLS12})
	return net.JoinHostPort(host, port), creds, nil
}
