package scramserver

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"
)

// ScramServer is a server implementation of SCRAM auth.
type ScramServer struct {
	n           []byte
	out         bytes.Buffer
	authMsg     bytes.Buffer
	clientNonce []byte
}

var b64 = base64.StdEncoding
var salt = []byte("somesortofsalt")

// NewScramServer creates a new ScramServer
func NewScramServer() (*ScramServer, error) {
	const nonceLen = 6
	buf := make([]byte, nonceLen+b64.EncodedLen(nonceLen))
	if _, err := rand.Read(buf[:nonceLen]); err != nil {
		return nil, fmt.Errorf("cannot read random SCRAM-SHA-1 nonce from operating system: %v", err)
	}

	n := buf[nonceLen:]
	b64.Encode(n, buf[:nonceLen])
	s := &ScramServer{
		n: n,
	}
	s.out.Grow(256)

	return s, nil
}

// Start performs the first step of the process, given request data from a client.
func (s *ScramServer) Start(in []byte) (string, error) {
	fields := bytes.Split(in, []byte(","))
	if len(fields) != 4 {
		return "", fmt.Errorf("expected 4 fields in first SCRAM-SHA-1 client message, got %d: %q", len(fields), in)
	}
	if len(fields[0]) != 1 || fields[0][0] != 'n' {
		return "", fmt.Errorf("client sent an invalid SCRAM-SHA-1 message start: %q", fields[0])
	}
	// We ignore fields[1]
	if !bytes.HasPrefix(fields[2], []byte("n=")) || len(fields[2]) < 2 {
		return "", fmt.Errorf("server sent an invalid SCRAM-SHA-1 username: %q", fields[2])
	}
	if !bytes.HasPrefix(fields[3], []byte("r=")) || len(fields[3]) < 6 {
		return "", fmt.Errorf("client sent an invalid SCRAM-SHA-1 nonce: %q", fields[3])
	}

	username := make([]byte, b64.DecodedLen(len(fields[2][2:])))

	s.clientNonce = fields[3][2:]
	s.out.WriteString("r=")
	s.out.Write(s.clientNonce)
	s.out.Write(s.n)

	encodedSalt := make([]byte, b64.EncodedLen(len(salt)))
	b64.Encode(encodedSalt, salt)
	s.out.WriteString(",s=")
	s.out.Write(encodedSalt)

	s.out.WriteString(",i=")
	s.out.Write([]byte(strconv.Itoa(4096)))

	return string(username), nil
}

// Step1 performs the first "step".
func (s *ScramServer) Step1(in []byte) error {
	fields := bytes.Split(in, []byte(","))
	if len(fields) != 3 {
		return fmt.Errorf("expected 3 fields in first SCRAM-SHA-1 client message, got %d: %q", len(fields), in)
	}
	// We ignore fields[0]
	if !bytes.HasPrefix(fields[1], []byte("r=")) || len(fields[1]) < 6 {
		return fmt.Errorf("client sent an invalid SCRAM-SHA-1 nonce: %q", fields[3])
	}
	if !bytes.HasPrefix(fields[2], []byte("p=")) || len(fields[2]) < 6 {
		return fmt.Errorf("client sent an invalid SCRAM-SHA-1 proof: %q", fields[3])
	}

	// TODO: verify this step
	s.out.WriteString("v=someserverproof")

	return nil
}

// Out returns the current data buffer which can be sent to the client.
func (s *ScramServer) Out() []byte {
	return s.out.Bytes()
}
