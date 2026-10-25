package servers

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func testTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}

// syncBuffer collects log output written from the server's goroutines.
type syncBuffer struct {
	lock sync.Mutex
	buf  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.buf.String()
}

// TestMochiwebTargetConnOverTLS checks a TLS listener whose requests are
// rewritten above TLS: a lone "%" is kept, the request carries its TLS
// state, a client sending plain HTTP gets no answer, as on Couchbase
// Server 7.0.5, and a failed handshake is logged as net/http logs it.
func TestMochiwebTargetConnOverTLS(t *testing.T) {
	logs := &syncBuffer{}
	previous := log.Writer()
	log.SetOutput(logs)
	t.Cleanup(func() { log.SetOutput(previous) })

	lsnr, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w, _ = prepareMochiwebRequest(w, req)
			_, _ = fmt.Fprintf(w, "%s tls=%v", req.URL.Path, req.TLS != nil && req.TLS.HandshakeComplete)
		}),
		ConnContext: mochiwebTargetConnContext,
	}
	go func() { _ = srv.Serve(mochiwebTargetListener{Listener: lsnr, tlsConfig: testTLSConfig(t)}) }()
	t.Cleanup(func() { _ = srv.Close() })
	addr := lsnr.Addr().String()

	conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	if _, err := io.WriteString(conn, "GET /a% HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("failed to read the response: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = conn.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "/a% tls=true" {
		t.Errorf("expected 200 \"/a%% tls=true\", got %d %q", resp.StatusCode, body)
	}

	plain, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	if _, err := io.WriteString(plain, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	answer, _ := io.ReadAll(plain)
	_ = plain.Close()
	if len(answer) != 0 {
		t.Errorf("plain HTTP: unexpected answer %q", answer)
	}

	garbage, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	_, _ = garbage.Write([]byte("\x16\x03\x01\x00\x05hello"))
	_, _ = io.ReadAll(garbage)
	_ = garbage.Close()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), "http: TLS handshake error from ") {
		if time.Now().After(deadline) {
			t.Fatalf("expected the handshake error to be logged, got %q", logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
