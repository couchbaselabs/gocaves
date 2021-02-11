package svcimpls

import (
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"log"
	"strings"

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

func (x *kvImplAuth) handleSASLListMechsRequest(source mock.KvClient, pak *memd.Packet) {
	// TODO(brett19): Implement actual auth mechanism configuration support.
	supportedMechs := []string{
		"PLAIN",
		"SCRAM-SHA1",
		"SCRAM-SHA256",
		"SCRAM-SHA512",
	}

	supportedBytes := []byte(strings.Join(supportedMechs, " "))

	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdSASLListMechs,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Value:   supportedBytes,
	})
}

func (x *kvImplAuth) handleAuthClient(source mock.KvClient, pak *memd.Packet, mech, username, password string) {
	log.Printf("AUTH ATTEMPT: %s, %s, %s", mech, username, password)

	user := source.Source().Node().Cluster().Users().GetUser(username)
	if user == nil {
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthError,
		})
		return
	}

	source.SetAuthenticatedUserName(username)

	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
	})
}

func (x *kvImplAuth) handleSASLAuthRequest(source mock.KvClient, pak *memd.Packet) {
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
			source.WritePacket(&memd.Packet{
				Magic:   memd.CmdMagicRes,
				Command: memd.CmdSASLAuth,
				Opaque:  pak.Opaque,
				Status:  memd.StatusAuthError,
			})
			return
		}

		if outBytes == nil {
			// SASL already completed
			x.handleAuthClient(source, pak, authMech, scram.Username(), scram.Password())
			return
		}

		user := source.Source().Node().Cluster().Users().GetUser(scram.Username())
		if user == nil {
			log.Printf("AUTHNONO %s\n", scram.Username())
			source.WritePacket(&memd.Packet{
				Magic:   memd.CmdMagicRes,
				Command: pak.Command,
				Opaque:  pak.Opaque,
				Status:  memd.StatusAuthError,
			})
			return
		}

		scram.SetPassword(user.Password)
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSASLAuth,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthContinue,
			Value:   outBytes,
		})
		return
	case "PLAIN":
		authPieces := strings.Split(string(pak.Value), string([]byte{0}))
		x.handleAuthClient(source, pak, authMech, authPieces[1], authPieces[2])
		return
	}

	// Unsupported mechanism!
	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdSASLAuth,
		Opaque:  pak.Opaque,
		Status:  memd.StatusAuthError,
	})
}

func (x *kvImplAuth) handleSASLStepRequest(source mock.KvClient, pak *memd.Packet) {
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
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSASLStep,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthError,
		})
		return
	}

	scram := source.ScramServer()
	outBytes, err := scram.Step(pak.Value)
	if err != nil {
		// SASL failure
		// TODO(brett19): Provide better diagnostics here?
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSASLStep,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthError,
		})
		return
	}

	source.SetAuthenticatedUserName(scram.Username())
	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdSASLStep,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
		Value:   outBytes,
	})
}

func (x *kvImplAuth) handleSelectBucketRequest(source mock.KvClient, pak *memd.Packet) {
	if !source.CheckAuthenticated(mockauth.PermissionSelect, pak.CollectionID) {
		// TODO(chvck): CheckAuthenticated needs to change, this could be actually be auth or access error depending on the user
		// access levels.
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdGetClusterConfig,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthError,
		})
		return
	}

	source.SetSelectedBucketName(string(pak.Key))
	if source.SelectedBucket() == nil {
		source.SetSelectedBucketName("")

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSelectBucket,
			Opaque:  pak.Opaque,
			Status:  memd.StatusKeyNotFound,
		})
		return
	}

	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdSelectBucket,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
	})
}
