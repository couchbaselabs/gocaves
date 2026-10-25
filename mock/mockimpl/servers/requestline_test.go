package servers

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestMochiwebTargetConnFollowsRequests sends requests one after another
// on a connection and checks that each reaches the handler with a "%" that
// starts no escape kept in its target, and its body intact, whatever the
// body holds.
func TestMochiwebTargetConnFollowsRequests(t *testing.T) {
	lsnr, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		_, _ = fmt.Fprintf(w, "%s %s|%s", req.Method, req.URL.Path, body)
	})}
	go func() { _ = srv.Serve(mochiwebTargetListener{Listener: lsnr}) }()
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := net.Dial("tcp", lsnr.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	reader := bufio.NewReader(conn)

	tests := []struct {
		request string
		want    string
	}{
		{"DELETE /a% HTTP/1.1\r\nHost: x\r\n\r\n", "DELETE /a%|"},
		{"DELETE /a%4 HTTP/1.1\r\nHost: x\r\n\r\n", "DELETE /a%4|"},
		{"DELETE /a%41+b%zz HTTP/1.1\r\nHost: x\r\n\r\n", "DELETE /aA+b%zz|"},
		{"POST /p% HTTP/1.1\r\nHost: x\r\nContent-Length: 26\r\n\r\nGET /b% HTTP/1.1\r\nX: y\r\n\r\n",
			"POST /p%|GET /b% HTTP/1.1\r\nX: y\r\n\r\n"},
		{"POST /c% HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n" +
			"4;ext=1\r\n% %%\r\n3\r\n\r\n\n\r\n0\r\nTrailer: z\r\n\r\n",
			"POST /c%|% %%\r\n\n"},
		{"GET /after% HTTP/1.1\r\nHost: x\r\n\r\n", "GET /after%|"},
	}
	for _, tt := range tests {
		if _, err := conn.Write([]byte(tt.request)); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		resp, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatalf("%q: failed to read the response: %v", tt.request, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(body) != tt.want {
			t.Errorf("%q: expected 200 %q, got %d %q", tt.request, tt.want, resp.StatusCode, body)
		}
	}
}

// startMochiwebTargetServer serves handler on a loopback listener whose
// connections are mochiwebTargetConns, as the HTTP server does.
func startMochiwebTargetServer(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	return startMochiwebTargetServerWith(t, nil, handler)
}

// startMochiwebTargetServerWith is startMochiwebTargetServer serving TLS
// with tlsConfig, if it is not nil.
func startMochiwebTargetServerWith(t *testing.T, tlsConfig *tls.Config, handler http.HandlerFunc) string {
	t.Helper()
	lsnr, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if w, answered := prepareMochiwebRequest(w, req); !answered {
				handler(w, req)
			}
		}),
		ConnContext: mochiwebTargetConnContext,
	}
	readMochiwebHeads(srv)
	go func() { _ = srv.Serve(mochiwebTargetListener{Listener: lsnr, tlsConfig: tlsConfig}) }()
	t.Cleanup(func() { _ = srv.Close() })
	return lsnr.Addr().String()
}

// TestMochiwebTargetConnUnreadBody checks that a response to a request
// whose large body the handler does not read reaches the client: net/http
// half-closes such a connection, which needs CloseWrite.
func TestMochiwebTargetConnUnreadBody(t *testing.T) {
	addr := startMochiwebTargetServer(t, func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	resp, err := http.Post("http://"+addr+"/", "application/octet-stream", bytes.NewReader(make([]byte, 10<<20)))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("expected 200 ok, got %d %q %v", resp.StatusCode, body, err)
	}
}

// TestMochiwebTargetConnBodyAllocations checks that a body passes through
// the conn without being copied into buffers of its own.
func TestMochiwebTargetConnBodyAllocations(t *testing.T) {
	client, server := net.Pipe()
	const size = 32 << 20
	go func() {
		_, _ = fmt.Fprintf(client, "POST /p%% HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n", size)
		chunk := make([]byte, 64<<10)
		for sent := 0; sent < size; sent += len(chunk) {
			if _, err := client.Write(chunk); err != nil {
				return
			}
		}
		_ = client.Close()
	}()
	conn := &mochiwebTargetConn{Conn: server}
	p := make([]byte, 32<<10)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	total := 0
	for {
		n, err := conn.Read(p)
		total += n
		if err != nil {
			break
		}
	}
	runtime.ReadMemStats(&after)
	if total < size {
		t.Fatalf("read %d bytes, expected at least %d", total, size)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Fatalf("reading a %d byte body allocated %d bytes", size, allocated)
	}
}

// TestMochiwebTargetConnFraming checks the framing cases where net/http
// and the conn must agree on where a request ends.
func TestMochiwebTargetConnFraming(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"HTTP/1.0 ignores Transfer-Encoding",
			"POST /a% HTTP/1.0\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\nx% GET /b% HTTP/1.1\r\n",
			"POST /a%25 HTTP/1.0\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\nX-Mochiweb-Target: t /a%\r\n\r\nx% GET /b%25 HTTP/1.1\r\nX-Mochiweb-Target: t /b%\r\n"},
		{"folded header line",
			"GET /a% HTTP/1.1\r\nX: y\r\n z\r\n\r\nGET /b% HTTP/1.1\r\n",
			"GET /a%25 HTTP/1.1\r\nX: y\r\n z\r\nHost: localhost\r\nX-Mochiweb-Target: t /a%\r\n\r\nGET /b%25 HTTP/1.1\r\nX-Mochiweb-Target: t /b%\r\n"},
		{"folded Content-Length",
			"POST /a% HTTP/1.1\r\nContent-Length:\r\n 3\r\n\r\nx% GET /b% HTTP/1.1\r\n",
			"POST /a%25 HTTP/1.1\r\nContent-Length:\r\n 3\r\nHost: localhost\r\nX-Mochiweb-Target: t /a%\r\n\r\nx% GET /b%25 HTTP/1.1\r\nX-Mochiweb-Target: t /b%\r\n"},
		{"partial line at the end",
			"GET /a% HTTP/1.1\r\n\r\nGET /b%",
			"GET /a%25 HTTP/1.1\r\nHost: localhost\r\nX-Mochiweb-Target: t /a%\r\n\r\nGET /b%"},
		{"empty lines before a request line",
			"\r\n\n\r\nGET /a HTTP/1.1\r\n\r\n\nGET /b HTTP/1.1\r\n\r\n",
			"GET /a HTTP/1.1\r\nHost: localhost\r\n\r\nGET /b HTTP/1.1\r\nHost: localhost\r\n\r\n"},
		{"target net/http cannot parse",
			"GET pools/a% HTTP/1.1\r\n\r\nGET http://x%41/p?q=1 HTTP/1.1\r\n\r\n",
			"GET / HTTP/1.1\r\nHost: localhost\r\nX-Mochiweb-Target: t pools/a%\r\n\r\nGET /p?q=1 HTTP/1.1\r\nHost: localhost\r\nX-Mochiweb-Target: t http://x%41/p?q=1\r\n\r\n"},
	}
	for _, tt := range tests {
		client, server := net.Pipe()
		go func() {
			_, _ = client.Write([]byte(tt.in))
			_ = client.Close()
		}()
		conn := &mochiwebTargetConn{Conn: server, token: "t"}
		if n, err := conn.Read(nil); n != 0 || err != nil {
			t.Fatalf("%s: zero-length read returned %d %v", tt.name, n, err)
		}
		got, _ := io.ReadAll(conn)
		if string(got) != tt.want {
			t.Errorf("%s:\n got %q\nwant %q", tt.name, got, tt.want)
		}
	}
}

// TestMochiwebTargetConnBackgroundReadAbort sends a request together with
// the start of the next one. net/http reads on in the background while the
// first is handled, and aborts that read with a deadline in the past; the
// rest of the next request line, sent afterwards, must still be rewritten.
func TestMochiwebTargetConnBackgroundReadAbort(t *testing.T) {
	addr := startMochiwebTargetServer(t, func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		_, _ = fmt.Fprintf(w, "%s %s|%s", req.Method, req.URL.Path, body)
	})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	reader := bufio.NewReader(conn)

	parts := []struct {
		data string
		want string
	}{
		{"GET /a% HTTP/1.1\r\nHost: x\r\n\r\nDELETE /b% HT", "GET /a%|"},
		{"TP/1.1\r\nHost: x\r\n\r\nPOST /c% HTTP/1.1\r\nHost: x\r\nContent-Le", "DELETE /b%|"},
		{"ngth: 3\r\n\r\n%zz", "POST /c%|%zz"},
	}
	for _, part := range parts {
		if _, err := conn.Write([]byte(part.data)); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		resp, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatalf("failed to read the response: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(body) != part.want {
			t.Fatalf("expected 200 %q, got %d %q", part.want, resp.StatusCode, body)
		}
	}
}

// TestMochiwebTargetConnReadTimeout checks that a read that times out
// keeps a partly read request line for the reads after it.
func TestMochiwebTargetConnReadTimeout(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	conn := &mochiwebTargetConn{Conn: server, token: "t"}
	go func() { _, _ = client.Write([]byte("GET /a% HTTP/1.1\r\n\r\nDELETE /b% HT")) }()
	p := make([]byte, 128)
	n, err := conn.Read(p)
	if err != nil || string(p[:n]) != "GET /a%25 HTTP/1.1\r\nHost: localhost\r\nX-Mochiweb-Target: t /a%\r\n\r\n" {
		t.Fatalf("first read returned %q %v", p[:n], err)
	}
	_ = conn.SetReadDeadline(time.Unix(1, 0))
	n, err = conn.Read(p)
	var netErr net.Error
	if n != 0 || !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected a timeout and no data, got %q %v", p[:n], err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	go func() { _, _ = client.Write([]byte("TP/1.1\r\n\r\n")) }()
	n, err = conn.Read(p)
	if err != nil || string(p[:n]) != "DELETE /b%25 HTTP/1.1\r\nHost: localhost\r\nX-Mochiweb-Target: t /b%\r\n\r\n" {
		t.Fatalf("read after the timeout returned %q %v", p[:n], err)
	}
}

// TestMochiwebTargetConnRefusals checks the requests mochiweb refuses
// against Couchbase Server 7.0.5: on the plain port, a request line over
// 8192 bytes or a header, folded lines included, over 8191, terminators
// included; on either port, a request with 1000 headers. Each is answered
// 400 with no body, keeping the HTTP version of the request and saying
// Connection: close where mochiweb_request:should_close does for the
// headers read before, and the connection ends without a reset.
func TestMochiwebTargetConnRefusals(t *testing.T) {
	handler := func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte("ok" + req.Header.Get(refusalHeader)))
	}
	plainAddr := startMochiwebTargetServer(t, handler)
	tlsAddr := startMochiwebTargetServerWith(t, testTLSConfig(t), handler)

	requestLine := func(size int, eol string) string {
		pre, post := "GET /", " HTTP/1.1"
		return pre + strings.Repeat("a", size-len(pre)-len(post)-len(eol)) + post + eol
	}
	header := func(size int, eol string) string {
		pre := "X-Long: "
		return pre + strings.Repeat("b", size-len(pre)-len(eol)) + eol
	}
	folded := func(first, second int) string {
		return "X-F: " + strings.Repeat("a", first-7) + "\r\n " + strings.Repeat("b", second-3) + "\r\n"
	}
	headers := func(n int) string {
		var b strings.Builder
		for i := 0; i < n; i++ {
			fmt.Fprintf(&b, "X-H%d: v\r\n", i)
		}
		return b.String()
	}
	const (
		ok = iota
		refusedOnPlain
		refused
	)
	tests := []struct {
		name    string
		request string
		outcome int
		proto   string
		close   bool
	}{
		{"request line of 8192", requestLine(8192, "\r\n") + "Host: x\r\n\r\n", ok, "", false},
		{"request line of 8193", requestLine(8193, "\r\n") + "Host: x\r\n\r\n", refusedOnPlain, "HTTP/1.1", true},
		{"request line of 8192 with LF", requestLine(8192, "\n") + "Host: x\n\n", ok, "", false},
		{"request line of 8193 with LF", requestLine(8193, "\n") + "Host: x\n\n", refusedOnPlain, "HTTP/1.1", true},
		{"request line of 200000", requestLine(200000, "\r\n") + "Host: x\r\n\r\n", refusedOnPlain, "HTTP/1.1", true},
		{"header of 8191", "GET / HTTP/1.1\r\nHost: x\r\n" + header(8191, "\r\n") + "\r\n", ok, "", false},
		{"header of 8192", "GET / HTTP/1.1\r\nHost: x\r\n" + header(8192, "\r\n") + "\r\n", refusedOnPlain, "HTTP/1.1", false},
		{"header of 8192 with LF", "GET / HTTP/1.1\nHost: x\n" + header(8192, "\n") + "\n", refusedOnPlain, "HTTP/1.1", false},
		{"header of 8192 before a body", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n" +
			header(8192, "\r\n") + "\r\nname=", refusedOnPlain, "HTTP/1.1", true},
		{"header of 8192 after Connection: close", "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n" +
			header(8192, "\r\n") + "\r\n", refusedOnPlain, "HTTP/1.1", true},
		{"header of 8192 in HTTP/1.0", "GET / HTTP/1.0\r\n" + header(8192, "\r\n") + "\r\n", refusedOnPlain, "HTTP/1.0", true},
		{"header of 8192 in HTTP/2.0", "GET / HTTP/2.0\r\n" + header(8192, "\r\n") + "\r\n", refusedOnPlain, "HTTP/1.1", false},
		{"folded header of 8191", "GET / HTTP/1.1\r\nHost: x\r\n" + folded(8000, 191) + "\r\n", ok, "", false},
		{"folded header of 8192", "GET / HTTP/1.1\r\nHost: x\r\n" + folded(4096, 4096) + "\r\n", refusedOnPlain, "HTTP/1.1", false},
		{"999 headers", "GET / HTTP/1.1\r\nHost: x\r\n" + headers(998) + "\r\n", ok, "", false},
		{"1000 headers", "GET / HTTP/1.1\r\nHost: x\r\n" + headers(999) + "\r\n", refused, "HTTP/1.1", false},
		{"1000 headers, the last Connection: close", "GET / HTTP/1.1\r\nHost: x\r\n" + headers(998) +
			"Connection: close\r\n\r\n", refused, "HTTP/1.1", true},
		{"header of 8192 in HTTP/1.0 with Connection: Keep-Alive", "GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n" +
			header(8192, "\r\n") + "\r\n", refusedOnPlain, "HTTP/1.0", false},
		{"header of 8192 in HTTP/1.0 with Connection: keep-alive", "GET / HTTP/1.0\r\nConnection: keep-alive\r\n" +
			header(8192, "\r\n") + "\r\n", refusedOnPlain, "HTTP/1.0", true},
		{"header of 8192 after Connection: CLOSE", "GET / HTTP/1.1\r\nHost: x\r\nConnection: CLOSE\r\n" +
			header(8192, "\r\n") + "\r\n", refusedOnPlain, "HTTP/1.1", true},
		{"header of 8192 after Connection: keep-alive, close", "GET / HTTP/1.1\r\nHost: x\r\nConnection: keep-alive, close\r\n" +
			header(8192, "\r\n") + "\r\n", refusedOnPlain, "HTTP/1.1", false},
		{"header of 8192 after Transfer-Encoding: chunked", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n" +
			header(8192, "\r\n") + "\r\n0\r\n\r\n", refusedOnPlain, "HTTP/1.1", true},
		{"header of 8192 after Transfer-Encoding: Chunked", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: Chunked\r\n" +
			header(8192, "\r\n") + "\r\n0\r\n\r\n", refusedOnPlain, "HTTP/1.1", false},
		{"Content-Length folded into a header of 8192", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length:\r\n 5" +
			strings.Repeat(" ", 8200) + "\r\n\r\nname=", refusedOnPlain, "HTTP/1.1", false},
		{"200 headers of 8000", "GET / HTTP/1.1\r\nHost: x\r\n" + strings.Repeat(header(8000, "\r\n"), 200) + "\r\n", ok, "", false},
		{"empty lines before the request line", "\r\n\n\r\nGET / HTTP/1.1\r\nHost: x\r\n\r\n", ok, "", false},
		{"a refusal header from the client", "GET / HTTP/1.1\r\nHost: x\r\n" + refusalHeader + ": x header\r\n\r\n", ok, "", false},
	}
	next := "GET /next HTTP/1.1\r\nHost: x\r\n\r\n"
	for _, port := range []string{"plain", "TLS"} {
		for _, tt := range tests {
			if port == "TLS" && strings.Contains(tt.request, "HTTP/2.0") {
				// net/http answers the request itself.
				continue
			}
			var conn net.Conn
			var err error
			if port == "plain" {
				conn, err = net.Dial("tcp", plainAddr)
			} else {
				conn, err = tls.Dial("tcp", tlsAddr, &tls.Config{InsecureSkipVerify: true})
			}
			if err != nil {
				t.Fatalf("failed to dial: %v", err)
			}
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			if _, err := conn.Write([]byte(tt.request + next)); err != nil {
				t.Fatalf("%s %s: failed to write: %v", port, tt.name, err)
			}
			// net/http reads Connection: close into resp.Close, which it also
			// sets for an HTTP/1.0 response without the header.
			var raw bytes.Buffer
			reader := bufio.NewReader(io.TeeReader(conn, &raw))
			resp, err := http.ReadResponse(reader, nil)
			if err != nil {
				t.Fatalf("%s %s: failed to read the response: %v", port, tt.name, err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if tt.outcome == refused || tt.outcome == refusedOnPlain && port == "plain" {
				head, _, _ := strings.Cut(raw.String(), "\r\n\r\n")
				closes := strings.Contains(head, "\r\nConnection: close\r\n") || strings.HasSuffix(head, "\r\nConnection: close")
				if resp.StatusCode != http.StatusBadRequest || len(body) != 0 || resp.ContentLength != 0 ||
					resp.Proto != tt.proto || closes != tt.close {
					t.Errorf("%s %s: expected %s 400 with no body, Connection: close %t, got %s %d %q length %d, %q",
						port, tt.name, tt.proto, tt.close, resp.Proto, resp.StatusCode, body, resp.ContentLength, head)
				}
				if _, err := http.ReadResponse(reader, nil); err == nil || errors.Is(err, syscall.ECONNRESET) {
					t.Errorf("%s %s: expected the connection to end cleanly after the refusal, got %v", port, tt.name, err)
				}
			} else if resp.StatusCode != http.StatusOK || string(body) != "ok" {
				t.Errorf("%s %s: expected 200 ok, got %d %q", port, tt.name, resp.StatusCode, body)
			}
			_ = conn.Close()
		}
	}
}

// TestMochiwebTargetConnUnparsedLines checks lines mochiweb cannot parse
// against Couchbase Server 7.0.5: neither gets an answer. A request line
// leaves the connection open, as mochiweb waits five minutes for one; a
// header line ends it after the five seconds mochiweb waits for a header,
// as an unfinished header does.
func TestMochiwebTargetConnUnparsedLines(t *testing.T) {
	addr := startMochiwebTargetServer(t, func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	tests := []struct {
		name    string
		request string
		ends    bool
	}{
		{"request line without a version", "GARBAGE\r\nHost: x\r\n\r\n", false},
		{"method with a character outside a token", "G@T / HTTP/1.1\r\nHost: x\r\n\r\n", false},
		{"version without a minor number", "GET / HTTP/1\r\nHost: x\r\n\r\n", false},
		{"header line without a colon", "GET / HTTP/1.1\r\nHost: x\r\nBad Header\r\n\r\n", true},
		{"header name with a space", "GET / HTTP/1.1\r\nHost: x\r\nX A: v\r\n\r\n", true},
		{"folded line before any header", "GET / HTTP/1.1\r\n folded\r\nHost: x\r\n\r\n", true},
		{"unfinished headers", "GET / HTTP/1.1\r\nHost: x\r\n", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("failed to dial: %v", err)
			}
			defer func() { _ = conn.Close() }()
			if _, err := conn.Write([]byte(tt.request)); err != nil {
				t.Fatalf("failed to write: %v", err)
			}
			start := time.Now()
			_ = conn.SetReadDeadline(start.Add(headersRecvTimeout + 2*time.Second))
			n, err := conn.Read(make([]byte, 1))
			elapsed := time.Since(start)
			var netErr net.Error
			switch {
			case n != 0:
				t.Errorf("expected no answer, got one")
			case tt.ends && (!errors.Is(err, io.EOF) || elapsed < headersRecvTimeout-time.Second):
				t.Errorf("expected the connection to end after %v, got %v after %v", headersRecvTimeout, err, elapsed)
			case !tt.ends && (!errors.As(err, &netErr) || !netErr.Timeout()):
				t.Errorf("expected the connection to stay open, got %v", err)
			}
		})
	}
}

// TestMochiwebTargetConnHTTP10KeepAlive checks that an HTTP/1.0 connection
// with Connection: Keep-Alive, as sent, stays open without the response
// saying so, and that any other closes, saying so, as
// mochiweb_request:should_close decides.
func TestMochiwebTargetConnHTTP10KeepAlive(t *testing.T) {
	addr := startMochiwebTargetServer(t, func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	for _, tt := range []struct {
		connection string
		open       bool
	}{
		{"Keep-Alive", true},
		{"keep-alive", false},
	} {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("failed to dial: %v", err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		request := "GET / HTTP/1.0\r\nConnection: " + tt.connection + "\r\n\r\n"
		if _, err := conn.Write([]byte(request + request)); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		var raw bytes.Buffer
		reader := bufio.NewReader(io.TeeReader(conn, &raw))
		answers := 0
		for ; answers < 2; answers++ {
			resp, err := http.ReadResponse(reader, nil)
			if err != nil {
				break
			}
			_, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		heads := raw.String()
		if tt.open && (answers != 2 || strings.Contains(heads, "Connection:")) ||
			!tt.open && (answers != 1 || !strings.Contains(heads, "Connection: close")) {
			t.Errorf("Connection: %s: got %d answers, %q", tt.connection, answers, heads)
		}
		_ = conn.Close()
	}
}

// TestMochiwebTargetConnLongHead checks that a head with many folded lines
// is read in linear time and memory, and that a head longer than the
// server reads is passed on to net/http, which refuses it.
func TestMochiwebTargetConnLongHead(t *testing.T) {
	addr := startMochiwebTargetServerWith(t, testTLSConfig(t), func(w http.ResponseWriter, req *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	send := func(request string) *http.Response {
		t.Helper()
		conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
		if err != nil {
			t.Fatalf("failed to dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		go func() { _, _ = conn.Write([]byte(request)) }()
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatalf("failed to read the response: %v", err)
		}
		_ = resp.Body.Close()
		return resp
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	resp := send("GET / HTTP/1.1\r\nHost: x\r\nX-F: a\r\n" + strings.Repeat(" a\r\n", 200000) + "\r\n")
	runtime.ReadMemStats(&after)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("200000 folded lines: expected 200, got %d", resp.StatusCode)
	}
	if elapsed, allocated := time.Since(start), after.TotalAlloc-before.TotalAlloc; elapsed > 5*time.Second || allocated > 256<<20 {
		t.Errorf("200000 folded lines took %v and allocated %d bytes", elapsed, allocated)
	}

	if resp := send("GET / HTTP/1.1\r\nHost: x\r\nX-Long: " + strings.Repeat("a", 2*maxHeadBytes) + "\r\n\r\n"); resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Errorf("a head over the limit: expected 431, got %d", resp.StatusCode)
	}
}

// TestMochiwebTargetConnShouldClose checks against Couchbase Server 7.0.5
// when a connection closes after a request mochiweb answers: when the
// handler left a body unread, at once, and when Connection is just
// "close"; an HTTP/0.9 request is answered as HTTP/1.1 and closes.
func TestMochiwebTargetConnShouldClose(t *testing.T) {
	addr := startMochiwebTargetServer(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/read" {
			_, _ = io.ReadAll(req.Body)
		}
		_, _ = w.Write([]byte("ok"))
	})
	tests := []struct {
		name    string
		request string
		answers int
		proto   string
		close   bool
	}{
		{"body read", "POST /read HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\nab", 2, "HTTP/1.1", false},
		{"body left unread", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\nab", 1, "HTTP/1.1", true},
		{"chunked GET without a body", "GET / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n", 1, "HTTP/1.1", true},
		{"Connection: keep-alive, close", "GET / HTTP/1.1\r\nHost: x\r\nConnection: keep-alive, close\r\n\r\n", 2, "HTTP/1.1", false},
		{"Connection: CLOSE", "GET / HTTP/1.1\r\nHost: x\r\nConnection: CLOSE\r\n\r\n", 1, "HTTP/1.1", true},
		{"HTTP/1.1 without Host", "GET / HTTP/1.1\r\n\r\n", 2, "HTTP/1.1", false},
		{"HTTP/0.9", "GET /\r\n\r\n", 1, "HTTP/1.1", true},
	}
	for _, tt := range tests {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("failed to dial: %v", err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		next := "GET / HTTP/1.1\r\nHost: x\r\n\r\n"
		if _, err := conn.Write([]byte(tt.request + next)); err != nil {
			t.Fatalf("%s: failed to write: %v", tt.name, err)
		}
		var raw bytes.Buffer
		reader := bufio.NewReader(io.TeeReader(conn, &raw))
		answers := 0
		var proto string
		for ; answers < 2; answers++ {
			resp, err := http.ReadResponse(reader, nil)
			if err != nil {
				break
			}
			if answers == 0 {
				proto = resp.Proto
			}
			_, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		head, _, _ := strings.Cut(raw.String(), "\r\n\r\n")
		closes := strings.Contains(head, "\r\nConnection: close")
		if answers != tt.answers || proto != tt.proto || closes != tt.close {
			t.Errorf("%s: expected %d answers in %s, Connection: close %t, got %d in %s, %q",
				tt.name, tt.answers, tt.proto, tt.close, answers, proto, raw.String())
		}
		_ = conn.Close()
	}
}
