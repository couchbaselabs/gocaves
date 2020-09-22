package scramserver

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
)

// scramServer is a server implementation of SCRAM auth.
type scramServer struct {
	n           []byte
	out         bytes.Buffer
	authMsg     bytes.Buffer
	clientNonce []byte

	Username string
}

var b64 = base64.StdEncoding
var salt = []byte("somesortofsalt")

// newScramServer creates a new ScramServer
func newScramServer() (*scramServer, error) {
	const nonceLen = 6
	buf := make([]byte, nonceLen+b64.EncodedLen(nonceLen))
	if _, err := rand.Read(buf[:nonceLen]); err != nil {
		return nil, fmt.Errorf("cannot read random SCRAM-SHA-1 nonce from operating system: %v", err)
	}

	n := buf[nonceLen:]
	b64.Encode(n, buf[:nonceLen])
	s := &scramServer{
		n: n,
	}
	s.out.Grow(256)

	return s, nil
}

// Start performs the first step of the process, given request data from a client.
func (s *scramServer) Start(in []byte) (string, error) {
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

	username := fields[2][2:]

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
func (s *scramServer) Step1(in []byte) error {
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
func (s *scramServer) Out() []byte {
	return s.out.Bytes()
}

// ScramServer is a server implementation of SCRAM auth with a slightly improved interface.
type ScramServer struct {
	srv      *scramServer
	username string
	password string
}

// Start performs the first step of the SCRAM process.  Returns nil if SCRAM completes.
func (s *ScramServer) Start(in []byte) ([]byte, error) {
	srv, err := newScramServer()
	if err != nil {
		return nil, err
	}

	username, err := srv.Start(in)
	if err != nil {
		return nil, err
	}

	s.srv = srv
	s.username = username
	return srv.Out(), nil
}

// Step performs one step of the SCRAM process. Returns nil if SCRAM completes.
func (s *ScramServer) Step(in []byte) ([]byte, error) {
	if s.srv == nil {
		return nil, errors.New("scram must be started first")
	}

	// We blindly call Step1 here since other steps aren't possible to achieve
	// since we clear out the srv after step 1 completes.
	err := s.srv.Step1(in)
	if err != nil {
		s.srv = nil
		return nil, err
	}

	s.srv = nil
	return nil, nil
}

// Username returns the password which was produced by SCRAM.
func (s *ScramServer) Username() string {
	return s.username
}

// Password returns the password which was produced by SCRAM.
func (s *ScramServer) Password() string {
	return s.password
}
