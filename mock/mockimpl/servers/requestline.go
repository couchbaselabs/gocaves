package servers

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// mochiwebTargetListener hands out connections whose request lines are
// rewritten by mochiwebTargetConn. With a TLS config, it serves TLS itself,
// so the requests can be rewritten above it; net/http then sees a plain
// connection, and the conn does the TLS handling net/http would do.
type mochiwebTargetListener struct {
	net.Listener
	tlsConfig *tls.Config
}

func (l mochiwebTargetListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		_ = conn.Close()
		return nil, err
	}
	c := &mochiwebTargetConn{Conn: conn, token: hex.EncodeToString(token)}
	if l.tlsConfig != nil {
		c.tlsConn = tls.Server(conn, l.tlsConfig)
		c.Conn = c.tlsConn
	}
	return c, nil
}

type mochiwebTargetConnKey struct{}

// mochiwebTargetConnContext keeps a conn in the context of its requests,
// for prepareMochiwebRequest.
func mochiwebTargetConnContext(ctx context.Context, conn net.Conn) context.Context {
	if c, ok := conn.(*mochiwebTargetConn); ok {
		return context.WithValue(ctx, mochiwebTargetConnKey{}, c)
	}
	return ctx
}

// The headers a conn adds to a request, each with the conn's token first.
// refusalHeader marks the stand-in for a request mochiweb refuses, with the
// kind of refusal; targetHeader carries the target the client sent when
// the conn rewrote it.
const (
	refusalHeader = "X-Mochiweb-Refusal"
	targetHeader  = "X-Mochiweb-Target"
)

// prepareMochiwebRequest readies a request read through a conn. It
// answers the stand-in for a request mochiweb refuses as mochiweb does,
// 400 with no body, and reports whether it did; the conn ends after it.
// Otherwise it restores the target the client sent, drops the headers the
// conn adds, as a client cannot set them, and sets req.TLS, which net/http
// sets only for a connection it serves TLS on itself.
func prepareMochiwebRequest(w http.ResponseWriter, req *http.Request) (http.ResponseWriter, bool) {
	c, ok := req.Context().Value(mochiwebTargetConnKey{}).(*mochiwebTargetConn)
	if !ok {
		return w, false
	}
	refusal, refused := tokenHeader(req.Header, refusalHeader, c.token)
	target, rewritten := tokenHeader(req.Header, targetHeader, c.token)
	req.Header.Del(refusalHeader)
	req.Header.Del(targetHeader)
	if refused {
		// The stand-in asks to close the connection where mochiweb answers
		// so; the connection ends either way.
		if refusal == "request-line" || req.Header.Get("Connection") == "close" {
			w.Header().Set("Connection", "close")
		}
		w.WriteHeader(http.StatusBadRequest)
		return w, true
	}
	if rewritten {
		req.RequestURI = target
	}
	// mochiweb_request:should_close closes the connection after a request
	// before HTTP/1.1 without Connection: Keep-Alive, as sent, and says so.
	// mochiweb keeps an HTTP/1.0 connection alive without saying so, where
	// net/http says Connection: keep-alive unless the header is present.
	connection := req.Header.Get("Connection")
	switch {
	case req.ProtoMajor == 1 && req.ProtoMinor == 0 && connection != "Keep-Alive",
		connection == "close", len(connection) == 5 && strings.EqualFold(connection, "close"):
		w.Header().Set("Connection", "close")
	case req.ProtoMajor == 1 && req.ProtoMinor == 0:
		w.Header()["Connection"] = nil
	}
	if c.tlsConn != nil && req.TLS == nil {
		state := c.tlsConn.ConnectionState()
		req.TLS = &state
	}
	body := &trackedBody{ReadCloser: req.Body}
	req.Body = body
	return &mochiwebResponseWriter{ResponseWriter: w, req: req, body: body, conn: c}, false
}

// trackedBody notes whether a handler read the body of a request.
type trackedBody struct {
	io.ReadCloser
	read bool
}

func (b *trackedBody) Read(p []byte) (int, error) {
	b.read = true
	return b.ReadCloser.Read(p)
}

// mochiwebResponseWriter says Connection: close, as
// mochiweb_request:should_close does, when the response starts and the
// handler left a body unread, and ends the stream of the connection, so
// net/http closes it at once, where it would otherwise read the body off
// first.
type mochiwebResponseWriter struct {
	http.ResponseWriter
	req         *http.Request
	body        *trackedBody
	conn        *mochiwebTargetConn
	wroteHeader bool
}

func (w *mochiwebResponseWriter) WriteHeader(statusCode int) {
	if !w.wroteHeader && !w.body.read && (w.req.ContentLength > 0 || slices.Contains(w.req.TransferEncoding, "chunked")) {
		w.Header().Set("Connection", "close")
		w.conn.abandoned.Store(true)
	}
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *mochiwebResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}

func (w *mochiwebResponseWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// tokenHeader returns the value of the header name that starts with token.
func tokenHeader(h http.Header, name, token string) (string, bool) {
	for _, value := range h.Values(name) {
		if rest, ok := strings.CutPrefix(value, token+" "); ok {
			return rest, true
		}
	}
	return "", false
}

// mochiwebTargetConn hands net/http the requests of a connection in a form
// it reads as mochiweb reads them. To find each request, it follows their
// framing as net/http reads it: their headers, and bodies sized by
// Content-Length or, from HTTP/1.1 on, chunked. It holds the request line
// and headers of a request until they end, then passes them on with the
// target rewritten where net/http would refuse it: a "%" that starts no
// escape is escaped as "%25", which decodes to the "%" mochiweb keeps, and
// a target net/http cannot parse is replaced. A request mochiweb refuses is
// replaced by a stand-in that prepareMochiwebRequest answers, and ends the
// connection; a request line or header mochiweb cannot parse gets no
// answer, and the connection ends when mochiweb stops waiting. Once the
// conn meets a request it cannot follow, it passes the rest of the
// connection through unchanged.
type mochiwebTargetConn struct {
	net.Conn
	tlsConn    *tls.Conn
	token      string
	handshaken bool
	ended      bool
	refused    atomic.Bool
	// abandoned ends the stream once a body is left unread.
	abandoned atomic.Bool
	stalled   bool
	// timer ends the connection when mochiweb would stop waiting; Close
	// stops it from another goroutine.
	timerLock sync.Mutex
	timer     *time.Timer
	state     int
	line      []byte
	// head holds the request line and the headers read so far, and
	// targetLine the header carrying the target sent, if it was rewritten,
	// to end them with.
	head       []byte
	targetLine []byte
	// The request as mochiweb reads it: its version, the Connection and
	// Transfer-Encoding headers, their values joined as mochiweb_headers
	// joins them, and whether it declares a body by Content-Length.
	major, minor     int
	connection       string
	transferEncoding string
	contentLength    bool
	host             bool
	// header is the header being read, its folded lines joined, groupLen
	// its length as sent and headers the number of headers.
	header     []byte
	groupLen   int
	headers    int
	dropGroup  bool
	remaining  int64
	chunked    bool
	out        []byte
	buf        [4096]byte
	passedOver bool
}

const (
	stateRequestLine = iota
	stateHeaders
	stateBody
	stateChunkSize
	stateChunkData
	stateChunkEnd
	stateTrailers
)

// maxLineLength bounds a buffered line; net/http refuses a header block
// larger than its default limit of 1 MiB anyway.
const maxLineLength = 1 << 20

// On the plain port, mochiweb reads the request line and each header,
// folded lines and terminators included, with Erlang's HTTP packet parser
// into a receive buffer of 8192 bytes; a header needs one byte more to
// tell whether a folded line follows. A longer one is refused. Over TLS
// the parser has no such limit. On either port, mochiweb_http refuses a
// request with MAX_HEADERS headers.
const (
	maxRequestLineLength = 8192
	maxHeaderLength      = 8191
	maxHeaders           = 1000
)

// maxHeadBytes is the longest request line and headers mochiweb reads on
// the plain port, and what the HTTP server reads. The conn passes a request
// on unchanged once the lines it holds come to more; the lines of a header
// without a name, which it drops, do not count.
const maxHeadBytes = maxRequestLineLength + (maxHeaders-1)*maxHeaderLength + len("\r\n")

// readMochiwebHeads lets srv read the request heads a conn hands on: as
// many headers and bytes as mochiweb reads, and a header of the conn's
// own, carrying a target of up to a request line.
func readMochiwebHeads(srv *http.Server) {
	allowHeaderValues(srv, maxHeaders+1)
	srv.MaxHeaderBytes = maxHeadBytes + maxRequestLineLength
}

// mochiweb_http waits REQUEST_RECV_TIMEOUT for a request line and
// HEADERS_RECV_TIMEOUT for each header; a line it cannot parse matches
// nothing it waits for, so the connection ends when the wait does.
const (
	requestRecvTimeout = 300 * time.Second
	headersRecvTimeout = 5 * time.Second
)

// CloseWrite lets net/http half-close the connection, as it does after a
// response to a request whose body it did not read.
func (c *mochiwebTargetConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

// Close ends a connection after a refusal as mochiweb does, without
// resetting it: the client gets the end of the stream, and what it still
// sends is read off before the connection closes.
func (c *mochiwebTargetConn) Close() error {
	c.stopTimer()
	if c.refused.Load() {
		_ = c.CloseWrite()
		_ = c.Conn.SetReadDeadline(time.Now().Add(time.Second))
		_, _ = io.Copy(io.Discard, c.Conn)
	}
	return c.Conn.Close()
}

func (c *mochiwebTargetConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if c.tlsConn != nil && !c.handshaken {
		if err := c.handshake(); err != nil {
			return 0, err
		}
	}
	if c.abandoned.Load() {
		return 0, io.EOF
	}
	for len(c.out) == 0 {
		if c.stalled {
			return c.readStalled()
		}
		if c.ended {
			return 0, io.EOF
		}
		// A body is read straight into p.
		if c.passedOver || c.state == stateBody || c.state == stateChunkData {
			want := len(p)
			if !c.passedOver && int64(want) > c.remaining {
				want = int(c.remaining)
			}
			n, err := c.Conn.Read(p[:want])
			if !c.passedOver {
				c.remaining -= int64(n)
				c.endBody()
			}
			return n, err
		}
		n, err := c.Conn.Read(c.buf[:])
		c.feed(c.buf[:n])
		if err != nil {
			// A read that timed out, as net/http's background read does when
			// it is aborted, leaves the stream to be read on; a request cut
			// short by the end of the stream goes on as it is.
			var netErr net.Error
			if !errors.As(err, &netErr) || !netErr.Timeout() {
				c.out = append(c.out, c.head...)
				c.out = append(c.out, c.targetLine...)
				c.out = append(c.out, c.line...)
				c.head = nil
				c.targetLine = nil
				c.line = nil
			}
			if len(c.out) == 0 {
				return 0, err
			}
			break
		}
	}
	n := copy(p, c.out)
	c.out = c.out[n:]
	return n, nil
}

// readStalled reads off what the client sends while the conn waits, as
// mochiweb does, for the end of a wait a line it cannot parse left it in.
// A read net/http aborts returns its timeout; the end of the wait closes
// the connection, which ends the stream for net/http.
func (c *mochiwebTargetConn) readStalled() (int, error) {
	for {
		_, err := c.Conn.Read(c.buf[:])
		if err == nil {
			continue
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return 0, err
		}
		return 0, io.EOF
	}
}

// stall leaves the request being read unanswered: the connection ends
// after the wait mochiweb is in.
func (c *mochiwebTargetConn) stall(wait time.Duration) {
	c.head = nil
	c.targetLine = nil
	c.line = nil
	c.stalled = true
	c.closeAfter(wait)
}

// closeAfter ends the connection after wait, unless the timer is stopped
// or set again before.
func (c *mochiwebTargetConn) closeAfter(wait time.Duration) {
	c.timerLock.Lock()
	defer c.timerLock.Unlock()
	if c.timer != nil {
		c.timer.Stop()
	}
	c.timer = time.AfterFunc(wait, func() { _ = c.Conn.Close() })
}

func (c *mochiwebTargetConn) stopTimer() {
	c.timerLock.Lock()
	defer c.timerLock.Unlock()
	if c.timer != nil {
		c.timer.Stop()
	}
}

// handshake completes the TLS handshake. As on Couchbase Server, a client
// that fails it, one that sent plain HTTP included, gets no answer; the
// failure is logged as net/http logs it, and the connection then ends, as
// io.EOF tells net/http.
func (c *mochiwebTargetConn) handshake() error {
	err := c.tlsConn.Handshake()
	if err == nil {
		c.handshaken = true
		return nil
	}
	log.Printf("http: TLS handshake error from %s: %v", c.RemoteAddr(), err)
	return io.EOF
}

func (c *mochiwebTargetConn) endBody() {
	if c.remaining > 0 {
		return
	}
	if c.state == stateBody {
		c.state = stateRequestLine
	} else {
		c.state = stateChunkEnd
	}
}

func (c *mochiwebTargetConn) feed(data []byte) {
	for len(data) > 0 && !c.ended && !c.stalled {
		if c.passedOver {
			c.out = append(c.out, data...)
			return
		}
		switch c.state {
		case stateBody, stateChunkData:
			n := int64(len(data))
			if n > c.remaining {
				n = c.remaining
			}
			c.out = append(c.out, data[:n]...)
			data = data[n:]
			c.remaining -= n
			c.endBody()
		default:
			i := bytes.IndexByte(data, '\n')
			if i < 0 {
				c.line = append(c.line, data...)
				if c.tooLong(c.line, false) {
					c.refuseLine()
				}
				return
			}
			c.line = append(c.line, data[:i+1]...)
			data = data[i+1:]
			line := c.line
			c.line = nil
			if c.tooLong(line, true) {
				c.line = line
				c.refuseLine()
				return
			}
			c.endLine(line)
		}
	}
}

// tooLong reports whether a line, read whole or in part, is longer than
// the conn reads.
func (c *mochiwebTargetConn) tooLong(line []byte, whole bool) bool {
	n := len(line)
	if !whole {
		// The terminator is still to come.
		n++
	}
	if c.tlsConn != nil && (c.state == stateRequestLine || c.state == stateHeaders) {
		return n > maxHeadBytes
	}
	if c.tlsConn != nil {
		return n > maxLineLength
	}
	switch c.state {
	case stateRequestLine:
		return n > maxRequestLineLength
	case stateHeaders:
		if len(line) > 0 && (line[0] == ' ' || line[0] == '\t') {
			n += c.groupLen
		}
		return n > maxHeaderLength
	}
	return n > maxLineLength
}

// refuseLine ends the connection at a line that is too long: on the plain
// port, a request line or header mochiweb refuses; any other line is
// passed on to net/http, which refuses it.
func (c *mochiwebTargetConn) refuseLine() {
	switch {
	case c.tlsConn == nil && c.state == stateRequestLine:
		c.refuse("request-line")
	case c.tlsConn == nil && c.state == stateHeaders:
		// The headers before it are read, and the one it folds into, if
		// any, is not.
		if len(c.line) > 0 && c.line[0] != ' ' && c.line[0] != '\t' {
			c.endHeader()
		}
		c.refuse("header")
	default:
		c.passOver()
	}
}

// refuse replaces the request being read by a stand-in for
// prepareMochiwebRequest, and ends the connection. A refused request line
// leaves nothing of the request known; a refused header keeps the HTTP
// version, and whether mochiweb closes the connection, from the headers
// read before it.
func (c *mochiwebTargetConn) refuse(kind string) {
	version, connection := "HTTP/1.1", "Connection: close\r\n"
	if kind == "header" {
		if c.major == 1 && c.minor == 0 {
			version = "HTTP/1.0"
		}
		if !c.mochiwebCloses() {
			connection = ""
		}
	}
	c.stopTimer()
	c.head = nil
	c.targetLine = nil
	c.line = nil
	c.out = append(c.out, "GET / "+version+"\r\nHost: x\r\n"+connection+refusalHeader+": "+c.token+" "+kind+"\r\n\r\n"...)
	c.ended = true
	c.refused.Store(true)
}

// mochiwebCloses is mochiweb_request:should_close for a request whose body
// is not read.
func (c *mochiwebTargetConn) mochiwebCloses() bool {
	isClose := c.connection == "close" || len(c.connection) == 5 && strings.EqualFold(c.connection, "close")
	return c.major < 1 || isClose ||
		c.major == 1 && c.minor == 0 && c.connection != "Keep-Alive" ||
		c.contentLength || c.transferEncoding == "chunked"
}

// passOver stops the rewriting and forwards what is held.
func (c *mochiwebTargetConn) passOver() {
	c.stopTimer()
	c.passedOver = true
	c.out = append(c.out, c.head...)
	c.out = append(c.out, c.targetLine...)
	c.targetLine = nil
	c.out = append(c.out, c.line...)
	c.head = nil
	c.line = nil
}

func (c *mochiwebTargetConn) endLine(line []byte) {
	text := strings.TrimRight(string(line), "\r\n")
	switch c.state {
	case stateRequestLine:
		// mochiweb skips empty lines before a request line.
		if text == "" {
			return
		}
		major, minor, ok := mochiwebRequestLine(text)
		if !ok {
			c.stall(requestRecvTimeout)
			return
		}
		c.state = stateHeaders
		c.remaining = 0
		c.chunked = false
		c.header = c.header[:0]
		c.groupLen = 0
		c.headers = 0
		c.major, c.minor = major, minor
		c.connection = ""
		c.transferEncoding = ""
		c.contentLength = false
		c.host = false
		c.dropGroup = false
		c.head, c.targetLine = c.rewriteRequestLine(line, major, minor)
		c.closeAfter(headersRecvTimeout)
		return
	case stateHeaders:
		if text != "" && (text[0] == ' ' || text[0] == '\t') {
			if c.headers == 0 {
				// A folded line with no header before it.
				c.stall(headersRecvTimeout)
				return
			}
			// net/http joins a folded line to the header line before it.
			c.header = append(c.header, ' ')
			c.header = append(c.header, strings.TrimSpace(text)...)
			c.groupLen += len(line)
			if !c.dropGroup {
				c.appendHead(line)
			}
			return
		}
		if text != "" && !mochiwebHeaderLine(text) {
			c.stall(headersRecvTimeout)
			return
		}
		c.closeAfter(headersRecvTimeout)
		if !c.endHeader() {
			c.head = append(c.head, line...)
			c.passOver()
			return
		}
		c.header = append(c.header[:0], text...)
		c.groupLen = len(line)
		if text == "" {
			c.stopTimer()
			c.out = append(c.out, c.finishHead()...)
			c.out = append(c.out, c.targetLine...)
			c.out = append(c.out, line...)
			c.head = nil
			c.targetLine = nil
			switch {
			case c.chunked:
				c.state = stateChunkSize
			case c.remaining > 0:
				c.state = stateBody
			default:
				c.state = stateRequestLine
			}
			return
		}
		c.headers++
		if c.headers >= maxHeaders {
			c.endHeader()
			c.refuse("header")
			return
		}
		// Erlang's parser drops the spaces before the colon, which net/http
		// refuses, and ns_server ignores a header with no name, which
		// net/http refuses too.
		name, value, _ := strings.Cut(text, ":")
		c.dropGroup = strings.TrimRight(name, " \t") == ""
		switch {
		case c.dropGroup:
		case strings.TrimRight(name, " \t") != name:
			c.appendHead([]byte(strings.TrimRight(name, " \t") + ":" + value + string(line[len(text):])))
		default:
			c.appendHead(line)
		}
		return
	case stateChunkSize:
		size, _, _ := strings.Cut(text, ";")
		n, err := strconv.ParseInt(strings.TrimSpace(size), 16, 64)
		switch {
		case err != nil || n < 0:
			c.out = append(c.out, line...)
			c.passOver()
			return
		case n == 0:
			c.state = stateTrailers
		default:
			c.remaining = n
			c.state = stateChunkData
		}
	case stateChunkEnd:
		c.state = stateChunkSize
	case stateTrailers:
		if text == "" {
			c.state = stateRequestLine
		}
	}
	c.out = append(c.out, line...)
}

// finishHead returns the head held, ready for net/http to read as
// mochiweb reads it. net/http closes the connection on a close option
// anywhere in Connection, where mochiweb closes it only on a value of just
// "close", so other close options are dropped. net/http requires a Host
// header from HTTP/1.1 on, which mochiweb does not; one is added where
// there is none. mochiweb answers an HTTP/0.9 request as HTTP/1.1 and
// closes the connection after it.
func (c *mochiwebTargetConn) finishHead() []byte {
	head := c.head
	if !c.mochiwebCloses() && c.connection != "" {
		head = dropCloseOptions(head)
	}
	if c.major == 0 {
		head = append(head, "Connection: close\r\n"...)
	}
	if !c.host && !(c.major == 1 && c.minor == 0) {
		head = append(head, "Host: localhost\r\n"...)
	}
	return head
}

// dropCloseOptions removes the close options of the Connection headers in
// head, and a header left with none.
func dropCloseOptions(head []byte) []byte {
	out := make([]byte, 0, len(head))
	for i, line := range bytes.SplitAfter(head, []byte("\n")) {
		name, value, ok := bytes.Cut(line, []byte(":"))
		if i == 0 || !ok || !strings.EqualFold(strings.TrimSpace(string(name)), "connection") {
			out = append(out, line...)
			continue
		}
		var kept []string
		for _, option := range strings.Split(strings.TrimSpace(string(value)), ",") {
			if option = strings.TrimSpace(option); option != "" && !strings.EqualFold(option, "close") {
				kept = append(kept, option)
			}
		}
		if len(kept) > 0 {
			out = append(out, string(name)+": "+strings.Join(kept, ", ")+"\r\n"...)
		}
	}
	return out
}

// appendHead holds a header line, and passes the request on unchanged
// once the held lines come to more than the HTTP server reads, which
// net/http refuses.
func (c *mochiwebTargetConn) appendHead(line []byte) {
	c.head = append(c.head, line...)
	if len(c.head) > maxHeadBytes {
		c.passOver()
	}
}

// endHeader takes the framing, and what mochiweb reads of the request,
// from the header read last, and reports whether the conn can follow the
// framing.
func (c *mochiwebTargetConn) endHeader() bool {
	name, value, ok := strings.Cut(string(c.header), ":")
	if !ok {
		return true
	}
	value = strings.TrimSpace(value)
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "connection":
		c.connection = joinHeaderValue(c.connection, value)
	case "host":
		c.host = true
	case "content-length":
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < 0 {
			return false
		}
		c.remaining = n
		c.contentLength = n > 0
	case "transfer-encoding":
		c.transferEncoding = joinHeaderValue(c.transferEncoding, value)
		// net/http ignores Transfer-Encoding on an HTTP/1.0 request.
		if c.major == 1 && c.minor == 0 {
			return true
		}
		c.chunked = strings.EqualFold(value, "chunked")
		return c.chunked
	}
	return true
}

// joinHeaderValue joins the values of a header sent more than once as
// mochiweb_headers does.
func joinHeaderValue(joined, value string) string {
	if joined == "" {
		return value
	}
	return joined + ", " + value
}

// mochiwebRequestLine reports whether Erlang's HTTP packet parser reads a
// request line: a method of token characters, a target and an HTTP
// version, which it returns.
func mochiwebRequestLine(text string) (major, minor int, ok bool) {
	method, rest, ok1 := strings.Cut(text, " ")
	target, version, ok2 := strings.Cut(rest, " ")
	if !ok1 || method == "" || strings.IndexFunc(method, func(r rune) bool { return !isTokenChar(r) }) >= 0 {
		return 0, 0, false
	}
	if !ok2 {
		// An HTTP/0.9 request line.
		return 0, 9, target != ""
	}
	digits, ok := strings.CutPrefix(version, "HTTP/")
	if !ok {
		return 0, 0, false
	}
	majorText, minorText, ok := strings.Cut(digits, ".")
	major, err1 := strconv.Atoi(majorText)
	minor, err2 := strconv.Atoi(minorText)
	if !ok || err1 != nil || err2 != nil || major < 0 || minor < 0 {
		return 0, 0, false
	}
	return major, minor, true
}

// mochiwebHeaderLine reports whether Erlang's HTTP packet parser reads a
// header line: a name, which may be empty, without spaces inside, then a
// colon.
func mochiwebHeaderLine(text string) bool {
	name, _, ok := strings.Cut(text, ":")
	return ok && !strings.ContainsAny(strings.TrimRight(name, " \t"), " \t")
}

func isTokenChar(r rune) bool {
	return r < 0x80 && (r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' ||
		strings.ContainsRune("!#$%&'*+-.^_`|~", r))
}

// rewriteRequestLine rewrites the target and version of a request line
// where net/http would refuse them, and returns it with, when the target
// changed, the header that carries the target sent.
func (c *mochiwebTargetConn) rewriteRequestLine(line []byte, major, minor int) ([]byte, []byte) {
	method, rest, _ := bytes.Cut(line, []byte(" "))
	sent, version, ok := bytes.Cut(rest, []byte(" "))
	if !ok {
		sent = bytes.TrimRight(rest, "\r\n")
		version = []byte("\r\n")
	}
	target := escapeLonePercents(string(sent))
	if _, err := url.ParseRequestURI(target); err != nil {
		// An absolute-form target whose authority net/http refuses keeps
		// its path; any other target is one ns_server fails on, whatever
		// it is replaced by.
		target = "/"
		if _, after, ok := strings.Cut(string(sent), "://"); ok {
			if i := strings.IndexByte(after, '/'); i >= 0 {
				if path := escapeLonePercents(after[i:]); isRequestURI(path) {
					target = path
				}
			}
		}
	}
	// net/http refuses any version but HTTP/1.0 and HTTP/1.1, and mochiweb
	// answers any other as HTTP/1.1.
	var extra []byte
	if major != 1 || minor > 1 {
		version = []byte("HTTP/1.1\r\n")
	}
	out := make([]byte, 0, len(line)+len(target))
	out = append(out, method...)
	out = append(out, ' ')
	out = append(out, target...)
	out = append(out, ' ')
	out = append(out, version...)
	if target != string(sent) {
		extra = append(extra, targetHeader+": "+c.token+" "+string(sent)+"\r\n"...)
	}
	return out, extra
}

func isRequestURI(target string) bool {
	_, err := url.ParseRequestURI(target)
	return err == nil
}

// escapeLonePercents escapes each "%" in a request target that is not
// followed by two hex digits.
func escapeLonePercents(target string) string {
	var out strings.Builder
	for i := 0; i < len(target); i++ {
		out.WriteByte(target[i])
		if target[i] == '%' && (i+2 >= len(target) || !isHex(target[i+1]) || !isHex(target[i+2])) {
			out.WriteString("25")
		}
	}
	return out.String()
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}
