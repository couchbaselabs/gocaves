package mockimpl

import (
	"strings"
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
)

func TestSelectMissingBucketIsAccessError(t *testing.T) {
	c := newTestKvConn(t)

	resp := c.Do(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("missing")})
	if resp.Status != memd.StatusAccessError {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusAccessError), uint16(resp.Status))
	}
}

// kv_engine's select_bucket_executor refuses a connection that has not
// authenticated with no_access and the context "Not authenticated".
func TestSelectBucketNotAuthenticated(t *testing.T) {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	c := dialTestKv(t, cluster, "", memd.FeatureXerror)

	resp := c.Do(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
	if resp.Status != memd.StatusAccessError {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusAccessError), uint16(resp.Status))
	}
	expectErrorContext(t, resp, "Not authenticated")
}

// TestSelectBucketRequests checks SELECT_BUCKET against Couchbase Server
// 7.0.5: select_bucket_validator checks the header before anything else,
// "@no bucket@" leaves the bucket, and kv_engine finds no bucket named "",
// keeping the selected one.
func TestSelectBucketRequests(t *testing.T) {
	c := newTestKvConn(t, memd.FeatureJSON)
	tests := []struct {
		name    string
		req     memd.Packet
		status  memd.StatusCode
		context string
	}{
		{name: "empty name", req: memd.Packet{}, status: memd.StatusKeyNotFound},
		{name: "name too long", req: memd.Packet{Key: []byte(strings.Repeat("b", 101))}, status: memd.StatusInvalidArgs,
			context: "Request key length exceeds maximum"},
		{name: "longest name", req: memd.Packet{Key: []byte(strings.Repeat("b", 100))}, status: memd.StatusAccessError},
		{name: "value", req: memd.Packet{Key: []byte("default"), Value: []byte("x")}, status: memd.StatusInvalidArgs,
			context: "Request must not include value"},
		{name: "extras", req: memd.Packet{Key: []byte("default"), Extras: []byte{1}}, status: memd.StatusInvalidArgs,
			context: "Request must not include extras"},
	}
	for _, tt := range tests {
		req := tt.req
		req.Command = memd.CmdSelectBucket
		resp := c.Do(&req)
		if resp.Status != tt.status {
			t.Fatalf("%s: expected status 0x%02x, got 0x%02x", tt.name, uint16(tt.status), uint16(resp.Status))
		}
		if tt.context != "" {
			expectErrorContext(t, resp, tt.context)
		}
	}
	c.mustSucceed(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("k"), Value: []byte("1"), Extras: make([]byte, 8)})

	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("@no bucket@")})
	if resp := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("k")}); resp.Status != memd.StatusNoBucket {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusNoBucket), uint16(resp.Status))
	}

	unauthenticated := dialTestKv(t, c.cluster, "", memd.FeatureXerror)
	resp := unauthenticated.Do(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte(strings.Repeat("b", 101))})
	if resp.Status != memd.StatusInvalidArgs {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusInvalidArgs), uint16(resp.Status))
	}
	expectErrorContext(t, resp, "Request key length exceeds maximum")
}

// TestSASLStepWithoutStart checks SASL_STEP against Couchbase Server 7.0.5:
// kv_engine's SaslAuthCommandContext refuses a step without the context a
// SASL_AUTH answered with CONTINUE leaves, and the validator a request
// without a mechanism.
func TestSASLStepWithoutStart(t *testing.T) {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	c := dialTestKv(t, cluster, "", memd.FeatureXerror)

	resp := c.Do(&memd.Packet{Command: memd.CmdSASLStep, Key: []byte("SCRAM-SHA512"), Value: []byte("c=biws")})
	if resp.Status != memd.StatusInvalidArgs {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusInvalidArgs), uint16(resp.Status))
	}
	expectErrorContext(t, resp, "Logic error: The server expects the client to start with SASL_START before sending SASL STEP")

	resp = c.Do(&memd.Packet{Command: memd.CmdSASLStep, Value: []byte("c=biws")})
	if resp.Status != memd.StatusInvalidArgs {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusInvalidArgs), uint16(resp.Status))
	}
	expectErrorContext(t, resp, "Request must include key")
}

// expectErrorRef checks that resp has status and an error body with a
// reference only, as kv_engine answers a failed authentication.
func expectErrorRef(t *testing.T, resp *memd.Packet, status memd.StatusCode) {
	t.Helper()
	ref := strings.TrimSuffix(strings.TrimPrefix(string(resp.Value), `{"error":{"ref":"`), `"}}`)
	if resp.Status != status || !accessErrorRef.MatchString(ref) {
		t.Fatalf("expected status 0x%02x with a reference, got 0x%02x with %s", uint16(status), uint16(resp.Status), resp.Value)
	}
}

// TestSASLPlain checks PLAIN SASL_AUTH requests against Couchbase Server
// 7.0.5: cbsasl skips the authorization identity, refuses a message without
// a username ending at a NUL as invalid, and an unknown user or a wrong
// password as an authentication error with a reference.
func TestSASLPlain(t *testing.T) {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })

	tests := []struct {
		msg    string
		status memd.StatusCode
		ref    bool
	}{
		{"abc", memd.StatusInvalidArgs, false},
		{"\x00Administrator", memd.StatusInvalidArgs, false},
		{"", memd.StatusInvalidArgs, false},
		{"\x00Administrator\x00wrong", memd.StatusAuthError, true},
		{"\x00nouser\x00password", memd.StatusAuthError, true},
		{"\x00Administrator\x00", memd.StatusAuthError, true},
		{"\x00\x00password", memd.StatusAuthError, true},
		{"x\x00Administrator\x00password", memd.StatusSuccess, false},
		{"\x00Administrator\x00password\x00x", memd.StatusSuccess, false},
		{"Administrator\x00Administrator\x00password", memd.StatusSuccess, false},
	}
	for _, tt := range tests {
		c := dialTestKv(t, cluster, "", memd.FeatureXerror)
		resp := c.Do(&memd.Packet{Command: memd.CmdSASLAuth, Key: []byte("PLAIN"), Value: []byte(tt.msg)})
		if tt.ref {
			expectErrorRef(t, resp, tt.status)
		} else if resp.Status != tt.status || len(resp.Value) != 0 {
			t.Fatalf("%q: expected status 0x%02x, got 0x%02x with %s", tt.msg, uint16(tt.status), uint16(resp.Status), resp.Value)
		}
	}
}

// TestSASLStepAfterScramContinue checks SASL_STEP after a SCRAM SASL_AUTH
// answered CONTINUE against Couchbase Server 7.0.5: the step goes to the
// SCRAM context whatever mechanism it names, which refuses a message that is
// not a list of attributes with a proof as invalid and a wrong proof as an
// authentication error, with the reference of the context, and releases it.
func TestSASLStepAfterScramContinue(t *testing.T) {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })

	tests := []struct {
		mech, msg string
		status    memd.StatusCode
	}{
		{"PLAIN", "\x00Administrator\x00password", memd.StatusInvalidArgs},
		{"SCRAM-SHA512", "c=biws,r=x", memd.StatusInvalidArgs},
		{"SCRAM-SHA512", "nonsense", memd.StatusInvalidArgs},
		{"SCRAM-SHA512", "c=biws,r=x,p=abcd", memd.StatusAuthError},
		{"SCRAM-SHA512", "c=biws,r=x,p=", memd.StatusAuthError},
		{"SCRAM-SHA512", "c=biws,r=abcdefghijklmnop,p=abcdefgh", memd.StatusAuthError},
		{"SCRAM-SHA512", "c=biws,x=1,r=x,p=abcdefgh", memd.StatusAuthError},
	}
	for _, tt := range tests {
		c := dialTestKv(t, cluster, "", memd.FeatureXerror)
		resp := c.Do(&memd.Packet{Command: memd.CmdSASLAuth, Key: []byte("SCRAM-SHA512"), Value: []byte("n,,n=Administrator,r=abcdefghijkl")})
		if resp.Status != memd.StatusAuthContinue {
			t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusAuthContinue), uint16(resp.Status))
		}
		expectErrorRef(t, c.Do(&memd.Packet{Command: memd.CmdSASLStep, Key: []byte(tt.mech), Value: []byte(tt.msg)}), tt.status)
		resp = c.Do(&memd.Packet{Command: memd.CmdSASLStep, Key: []byte(tt.mech), Value: []byte(tt.msg)})
		expectErrorContext(t, resp, "Logic error: The server expects the client to start with SASL_START before sending SASL STEP")
	}
}

// TestSASLAuthRestartsAuthentication checks SASL_AUTH on an authenticated
// connection against Couchbase Server 7.0.5: kv_engine restarts the
// authentication before the mechanism runs, so a failed or pending one
// leaves the connection not authenticated for SELECT_BUCKET, while commands
// on the bucket selected keep the privileges they had.
func TestSASLAuthRestartsAuthentication(t *testing.T) {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	addTestUser(t, cluster, "dr", "data_reader[default]")

	for _, auth := range []*memd.Packet{
		{Command: memd.CmdSASLAuth, Key: []byte("PLAIN"), Value: []byte("\x00dr\x00wrong")},
		{Command: memd.CmdSASLAuth, Key: []byte("PLAIN"), Value: []byte("abc")},
		{Command: memd.CmdSASLAuth, Key: []byte("SCRAM-SHA512"), Value: []byte("n,,n=dr,r=abcdefghijkl")},
	} {
		c := dialTestKv(t, cluster, "dr", memd.FeatureXerror)
		c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
		c.Do(auth)
		vbID := c.masterVbucket(cluster.Nodes()[0])
		if resp := c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: vbID, Key: []byte("k")}); resp.Status != memd.StatusKeyNotFound {
			t.Fatalf("%q: expected GET to find no key, got 0x%02x", auth.Value, uint16(resp.Status))
		}
		resp := c.Do(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
		if resp.Status != memd.StatusAccessError {
			t.Fatalf("%q: expected status 0x%02x, got 0x%02x", auth.Value, uint16(memd.StatusAccessError), uint16(resp.Status))
		}
		expectErrorContext(t, resp, "Not authenticated")
	}
}

// TestSASLMechanisms checks SASL_AUTH and SASL_STEP requests against
// Couchbase Server 7.0.5: cbsasl takes a mechanism in any case and refuses
// an unknown one, refuses an empty message as invalid without a reference,
// and a malformed SCRAM message with one. For an unknown user it goes on
// with a dummy one, which fails the final step. A SASL_AUTH while a
// CONTINUE is pending is refused, and keeps the exchange.
func TestSASLMechanisms(t *testing.T) {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })

	expectStatus := func(resp *memd.Packet, status memd.StatusCode, body string) {
		t.Helper()
		if resp.Status != status || string(resp.Value) != body {
			t.Fatalf("expected status 0x%02x with %q, got 0x%02x with %q", uint16(status), body, uint16(resp.Status), resp.Value)
		}
	}
	auth := func(mech, msg string) *memd.Packet {
		return &memd.Packet{Command: memd.CmdSASLAuth, Key: []byte(mech), Value: []byte(msg)}
	}
	step := func(msg string) *memd.Packet {
		return &memd.Packet{Command: memd.CmdSASLStep, Key: []byte("SCRAM-SHA512"), Value: []byte(msg)}
	}

	c := dialTestKv(t, cluster, "", memd.FeatureXerror)
	expectStatus(c.Do(auth("FOO", "x")), memd.StatusAuthError, `{"error":{"context":"Requested mechanism \"FOO\" is not supported"}}`)
	expectStatus(c.Do(auth("foo", "x")), memd.StatusAuthError, `{"error":{"context":"Requested mechanism \"FOO\" is not supported"}}`)
	expectStatus(c.Do(auth("SCRAM-SHA512", "")), memd.StatusInvalidArgs, "")
	expectStatus(c.Do(auth("PLAIN", "")), memd.StatusInvalidArgs, "")
	expectErrorRef(t, c.Do(auth("SCRAM-SHA512", "garbage")), memd.StatusInvalidArgs)
	expectStatus(c.Do(auth("plain", "\x00Administrator\x00password")), memd.StatusSuccess, "")

	c = dialTestKv(t, cluster, "", memd.FeatureXerror)
	for _, mech := range []string{"scram-sha512", "Scram-Sha1"} {
		if resp := c.Do(auth(mech, "n,,n=Administrator,r=abcdefghijkl")); resp.Status != memd.StatusAuthContinue {
			t.Fatalf("%s: expected status 0x%02x, got 0x%02x", mech, uint16(memd.StatusAuthContinue), uint16(resp.Status))
		}
		resp := c.Do(auth("PLAIN", "\x00Administrator\x00password"))
		if resp.Status != memd.StatusInvalidArgs {
			t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusInvalidArgs), uint16(resp.Status))
		}
		expectErrorContext(t, resp, "Logic error: The server expects SASL STEP after returning CONTINUE from SASL START")
		expectStatus(c.Do(step("")), memd.StatusInvalidArgs, "")
		resp = c.Do(step("c=biws,r=x,p=abcd"))
		expectErrorContext(t, resp, "Logic error: The server expects the client to start with SASL_START before sending SASL STEP")
	}

	c = dialTestKv(t, cluster, "", memd.FeatureXerror)
	if resp := c.Do(auth("SCRAM-SHA512", "n,,n=nouser,r=abcdefghijkl")); resp.Status != memd.StatusAuthContinue {
		t.Fatalf("expected status 0x%02x for an unknown user, got 0x%02x", uint16(memd.StatusAuthContinue), uint16(resp.Status))
	}
	expectErrorRef(t, c.Do(step("c=biws,r=abcdefghijklxxxx,p=abcdefgh")), memd.StatusAuthError)
}

// TestSASLAuthSelectsUserBucket checks the bucket of a connection after
// authentication against Couchbase Server 7.0.5: kv_engine selects the
// bucket named after the user when the user may access it, a memcached
// bucket too, and no bucket otherwise, also when the connection had selected
// one before.
func TestSASLAuthSelectsUserBucket(t *testing.T) {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })
	addTestUser(t, cluster, "default", "bucket_full_access[default]")
	addTestUser(t, cluster, "bfa", "bucket_full_access[default]")
	addTestUser(t, cluster, "memd", "bucket_full_access[memd]")

	get := func(c *testKvConn) memd.StatusCode {
		return c.Do(&memd.Packet{Command: memd.CmdGet, Vbucket: c.masterVbucket(cluster.Nodes()[0]), Key: []byte("nokey")}).Status
	}
	// default_engine reports collections too, so a memcached bucket is
	// selected for a connection with them.
	for _, username := range []string{"default", "memd"} {
		c := dialTestKv(t, cluster, username, memd.FeatureXerror, memd.FeatureCollections)
		if status := get(c); status != memd.StatusKeyNotFound {
			t.Fatalf("user %s: expected the bucket selected, got status 0x%02x", username, uint16(status))
		}
	}
	for _, username := range []string{"bfa", "default"} {
		c := dialTestKv(t, cluster, "bfa", memd.FeatureXerror)
		c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
		c.mustSucceed(&memd.Packet{Command: memd.CmdSASLAuth, Key: []byte("PLAIN"), Value: []byte("\x00" + username + "\x00password")})
		expected := memd.StatusNoBucket
		if username == "default" {
			expected = memd.StatusKeyNotFound
		}
		if status := get(c); status != expected {
			t.Fatalf("authenticated again as %s: expected status 0x%02x, got 0x%02x", username, uint16(expected), uint16(status))
		}
	}
}
