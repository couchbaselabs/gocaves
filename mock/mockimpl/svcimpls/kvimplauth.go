package svcimpls

import (
	"log"
	"strings"
	"time"

	"github.com/couchbaselabs/gocaves/mock/mockauth"

	"github.com/couchbase/gocbcore/v9/memd"
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

func (x *kvImplAuth) handleAuthClient(source mock.KvClient, pak *memd.Packet, mech, username, password string, start time.Time) {
	user := source.Source().Node().Cluster().Users().GetUser(username)
	if user == nil {
		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthError,
		}, start)
		return
	}

	source.SetAuthenticatedUserName(username)

	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
	}, start)
}

func (x *kvImplAuth) handleSASLAuthRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	authMech := string(pak.Key)

	switch authMech {
	case "SCRAM-SHA512":
		fallthrough
	case "SCRAM-SHA256":
		fallthrough
	case "SCRAM-SHA1":
		scram := source.ScramServer()
		outBytes, err := scram.Start(pak.Value, authMech)
		if err != nil {
			// SASL failure
			// TODO(brett19): Provide better diagnostics here?
			writePacketToSource(source, &memd.Packet{
				Magic:   memd.CmdMagicRes,
				Command: memd.CmdSASLAuth,
				Opaque:  pak.Opaque,
				Status:  memd.StatusAuthError,
			}, start)
			return
		}

		if outBytes == nil {
			// SASL already completed
			x.handleAuthClient(source, pak, authMech, scram.Username(), scram.Password(), start)
			return
		}

		user := source.Source().Node().Cluster().Users().GetUser(scram.Username())
		if user == nil {
			writePacketToSource(source, &memd.Packet{
				Magic:   memd.CmdMagicRes,
				Command: pak.Command,
				Opaque:  pak.Opaque,
				Status:  memd.StatusAuthError,
			}, start)
			return
		}

		err = scram.SetPassword(user.Password)
		if err != nil {
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
		authPieces := strings.Split(string(pak.Value), string([]byte{0}))
		x.handleAuthClient(source, pak, authMech, authPieces[1], authPieces[2], start)
		return
	}

	// Unsupported mechanism!
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdSASLAuth,
		Opaque:  pak.Opaque,
		Status:  memd.StatusAuthError,
	}, start)
}

func (x *kvImplAuth) handleSASLStepRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	authMech := string(pak.Key)

	log.Printf("AUTH STEP: %+v, %s", authMech, pak.Value)

	switch authMech {
	case "SCRAM-SHA512":
		fallthrough
	case "SCRAM-SHA256":
		fallthrough
	case "SCRAM-SHA1":
		// These are all accepted
	case "PLAIN":
		// Unsupported mechanism!
		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSASLStep,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthError,
		}, start)
		return
	}

	scram := source.ScramServer()
	outBytes, err := scram.Step(pak.Value)
	if err != nil {
		// SASL failure
		// TODO(brett19): Provide better diagnostics here?
		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSASLStep,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthError,
		}, start)
		return
	}

	source.SetAuthenticatedUserName(scram.Username())
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdSASLStep,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Value:   outBytes,
	}, start)
}

func (x *kvImplAuth) handleSelectBucketRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	if !source.CheckAuthenticated(mockauth.PermissionSelect, pak.CollectionID) {
		// TODO(chvck): CheckAuthenticated needs to change, this could be actually be auth or access error depending on the user
		// access levels.
		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdGetClusterConfig,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthError,
		}, start)
		return
	}

	source.SetSelectedBucketName(string(pak.Key))
	if source.SelectedBucket() == nil {
		source.SetSelectedBucketName("")

		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSelectBucket,
			Opaque:  pak.Opaque,
			Status:  memd.StatusKeyNotFound,
		}, start)
		return
	}

	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdSelectBucket,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
	}, start)
}
