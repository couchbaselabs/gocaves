package svcimpls

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/contrib/scramserver"
	"github.com/couchbaselabs/gocaves/mock"
)

type kvImplAuth struct {
}

func (x *kvImplAuth) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdSASLListMechs, x.handleSASLListMechsRequest)
	h.RegisterKvHandler(memd.CmdSASLAuth, x.handleSASLAuthRequest)
	h.RegisterKvHandler(memd.CmdSASLStep, x.handleSASLStepRequest)
	h.RegisterKvHandler(memd.CmdSelectBucket, x.handleSelectBucketRequest)
}

func (x *kvImplAuth) handleSASLListMechsRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	// TODO(brett19): Implement actual auth mechanism configuration support.
	supportedMechs := []string{
		"PLAIN",
		"SCRAM-SHA1",
		"SCRAM-SHA256",
		"SCRAM-SHA512",
	}

	supportedBytes := []byte(strings.Join(supportedMechs, " "))

	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdSASLListMechs,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Value:   supportedBytes,
	}, start)
}

// associateBucket selects the bucket of a connection that authenticated as
// SaslAuthCommandContext::tryHandleSaslOk does: the bucket named after the
// user when the user may access it, else no bucket. It moves a connection
// with collections back to no bucket for a bucket without them, which no
// bucket of 7.0.5 is: default_engine reports collections too.
func associateBucket(source mock.KvClient, username string) {
	source.SetSelectedBucketName("")
	cluster := source.Source().Node().Cluster()
	user := cluster.Users().GetUser(username)
	bucket := cluster.GetBucket(username)
	if user == nil || bucket == nil || user.BucketPrivileges(mock.ClusterResources{Cluster: cluster}, bucket.Name()) == nil {
		return
	}
	source.SetSelectedBucketName(bucket.Name())
}

func (x *kvImplAuth) handleSASLAuthRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	// kv_engine keeps the SASL context of a CONTINUE until the step that
	// follows.
	if source.ScramServer().Started() {
		writePacketToSource(source, errorContextResponse(pak, memd.StatusInvalidArgs,
			"Logic error: The server expects SASL STEP after returning CONTINUE from SASL START"), start)
		return
	}
	// SaslAuthCommandContext restarts the authentication of the connection
	// before it runs the mechanism, which cbsasl's ServerContext::start
	// refuses without a message, and picks by its name in upper case.
	source.RestartAuthentication()
	if len(pak.Value) == 0 {
		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusInvalidArgs,
		}, start)
		return
	}
	authMech := strings.ToUpper(string(pak.Key))

	switch authMech {
	case "SCRAM-SHA512", "SCRAM-SHA256", "SCRAM-SHA1":
		// cbsasl refuses a malformed first message as BAD_PARAM, with the
		// reference of the SCRAM context. For an unknown user it goes on
		// with a dummy one, which fails the final step.
		scram := source.ScramServer()
		outBytes, err := scram.Start(pak.Value, authMech)
		if err != nil {
			writePacketToSource(source, errorRefResponse(pak, memd.StatusInvalidArgs), start)
			return
		}
		password := newErrorRef()
		if user := source.Source().Node().Cluster().Users().GetUser(scram.Username()); user != nil {
			password = user.Password
		}
		if err := scram.SetPassword(password); err != nil {
			log.Printf("failed to set scram password: %s", err)
		}

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSASLAuth,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthContinue,
			Value:   outBytes,
		}, start)
		return
	case "PLAIN":
		x.handlePlainAuth(source, pak, start)
		return
	}

	writePacketToSource(source, errorContextResponse(pak, memd.StatusAuthError,
		fmt.Sprintf("Requested mechanism \"%s\" is not supported", authMech)), start)
}

// handlePlainAuth authenticates a PLAIN SASL_AUTH as cbsasl's plain
// ServerBackend::start does. It refuses a message it cannot parse with
// BAD_PARAM, and an unknown user or a wrong password with NO_USER or
// PASSWORD_ERROR, for which kv_engine logs an audit event and answers with
// its reference.
func (x *kvImplAuth) handlePlainAuth(source mock.KvClient, pak *memd.Packet, start time.Time) {
	username, password, ok := plainCredentials(pak.Value)
	if !ok {
		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusInvalidArgs,
		}, start)
		return
	}
	user := source.Source().Node().Cluster().Users().GetUser(username)
	if user == nil || !user.PasswordIs(password) {
		writePacketToSource(source, errorRefResponse(pak, memd.StatusAuthError), start)
		return
	}
	source.SetAuthenticatedUserName(username)
	associateBucket(source, username)
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
	}, start)
}

// plainCredentials splits a PLAIN message as cbsasl does: it skips the
// authorization identity up to the first NUL, takes the username up to the
// next, which it requires, and the password up to the one after or the end.
func plainCredentials(msg []byte) (string, string, bool) {
	authzid := bytes.IndexByte(msg, 0)
	if authzid < 0 || authzid+1 >= len(msg) {
		return "", "", false
	}
	rest := msg[authzid+1:]
	end := bytes.IndexByte(rest, 0)
	if end < 0 {
		return "", "", false
	}
	password := rest[end+1:]
	if i := bytes.IndexByte(password, 0); i >= 0 {
		password = password[:i]
	}
	return string(rest[:end]), string(password), true
}

func (x *kvImplAuth) handleSASLStepRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if !source.ScramServer().Started() {
		writePacketToSource(source, errorContextResponse(pak, memd.StatusInvalidArgs,
			"Logic error: The server expects the client to start with SASL_START before sending SASL STEP"), start)
		return
	}
	// The step goes to the backend of the SASL context, whatever mechanism
	// it names, which refuses an empty message without a reference.
	if len(pak.Value) == 0 {
		source.ScramServer().Finish()
		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusInvalidArgs,
		}, start)
		return
	}
	// cbsasl refuses a message it cannot parse with BAD_PARAM and wrong
	// credentials with PASSWORD_ERROR, and either releases the context;
	// kv_engine answers with the reference of the SCRAM context.
	scram := source.ScramServer()
	outBytes, err := scram.Step(pak.Value)
	if err != nil {
		status := memd.StatusInvalidArgs
		if errors.Is(err, scramserver.ErrAuthFailed) {
			status = memd.StatusAuthError
		}
		writePacketToSource(source, errorRefResponse(pak, status), start)
		return
	}

	scram.Finish()
	source.SetAuthenticatedUserName(scram.Username())
	associateBucket(source, scram.Username())
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdSASLStep,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Value:   outBytes,
	}, start)
}

func (x *kvImplAuth) handleSelectBucketRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	reply := func(status memd.StatusCode) {
		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSelectBucket,
			Opaque:  pak.Opaque,
			Status:  status,
		}, start)
	}

	cluster := source.Source().Node().Cluster()
	name := string(pak.Key)
	switch {
	case !source.IsAuthenticated():
		writePacketToSource(source, errorContextResponse(pak, memd.StatusAccessError, "Not authenticated"), start)
		return
	case name == "@no bucket@":
		source.SetSelectedBucketName("")
		reply(memd.StatusSuccess)
		return
	}
	// kv_engine's select_bucket requires the user to have a privilege on the
	// bucket, or on a scope or collection in it, which nobody has on a
	// bucket that does not exist, and a user dropped since it authenticated
	// has none. It finds no bucket named "", and keeps the bucket selected.
	user := cluster.Users().GetUser(source.AuthenticatedUserName())
	if user != nil && name == "" {
		reply(memd.StatusKeyNotFound)
		return
	}
	bucket := cluster.GetBucket(name)
	if user == nil || bucket == nil || user.BucketPrivileges(mock.ClusterResources{Cluster: cluster}, bucket.Name()) == nil {
		reply(memd.StatusAccessError)
		return
	}

	source.SetSelectedBucketName(bucket.Name())
	reply(memd.StatusSuccess)
}
