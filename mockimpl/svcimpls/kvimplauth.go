package svcimpls

import (
	"log"
	"strings"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/hooks"
	"github.com/couchbaselabs/gocaves/mock"
)

type kvImplAuth struct {
}

func (x *kvImplAuth) Register(hooks *hooks.KvHookManager) {
	reqExpects := hooks.Expect().Magic(memd.CmdMagicReq)

	reqExpects.Cmd(memd.CmdSASLListMechs).Handler(x.handleSASLListMechsRequest)
	reqExpects.Cmd(memd.CmdSASLAuth).Handler(x.handleSASLAuthRequest)
	reqExpects.Cmd(memd.CmdSASLStep).Handler(x.handleSASLStepRequest)
	reqExpects.Cmd(memd.CmdSelectBucket).Handler(x.handleSelectBucketRequest)
}

func (x *kvImplAuth) handleSASLListMechsRequest(source mock.KvClient, pak *memd.Packet, next func()) {
	// TODO(brett19): Implement actual auth mechanism configuration support.
	supportedMechs := []string{
		"PLAIN",
		"SCRAM_SHA1",
		"SCRAM_SHA256",
		"SCRAM_SHA512",
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

	// TODO(brett19): Need to implement password validation here...
	if username != "Administrator" {
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

func (x *kvImplAuth) handleSASLAuthRequest(source mock.KvClient, pak *memd.Packet, next func()) {
	authMech := string(pak.Key)

	log.Printf("AUTH START: %+v, %s", authMech, pak.Value)

	switch authMech {
	case "SCRAM-SHA512":
		fallthrough
	case "SCRAM-SHA256":
		fallthrough
	case "SCRAM-SHA1":
		scram := source.ScramServer()
		outBytes, err := scram.Start(pak.Value)
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

		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSASLAuth,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthContinue,
			Value:   outBytes,
		})
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

func (x *kvImplAuth) handleSASLStepRequest(source mock.KvClient, pak *memd.Packet, next func()) {
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

	if outBytes == nil {
		// SASL completed
		x.handleAuthClient(source, pak, authMech, scram.Username(), scram.Password())
		return
	}

	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdSASLStep,
		Opaque:  pak.Opaque,
		Status:  memd.StatusAuthContinue,
		Value:   outBytes,
	})
}

func (x *kvImplAuth) handleSelectBucketRequest(source mock.KvClient, pak *memd.Packet, next func()) {
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
