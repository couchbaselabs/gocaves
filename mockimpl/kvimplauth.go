package mockimpl

import (
	"log"
	"strings"

	"github.com/couchbase/gocbcore/v9/memd"
)

type kvImplAuth struct {
}

func (x *kvImplAuth) Register(hooks *KvHookManager) {
	reqExpects := hooks.Expect().Magic(memd.CmdMagicReq)

	reqExpects.Cmd(memd.CmdSASLListMechs).Handler(x.handleSASLListMechsRequest)
	reqExpects.Cmd(memd.CmdSASLAuth).Handler(x.handleSASLAuthRequest)
	reqExpects.Cmd(memd.CmdSASLStep).Handler(x.handleSASLStepRequest)
	reqExpects.Cmd(memd.CmdSelectBucket).Handler(x.handleSelectBucketRequest)
}

// TODO(brett19): Implement SCRAM authentication here...

func (x *kvImplAuth) handleSASLListMechsRequest(source *KvClient, pak *memd.Packet, next func()) {
	supportedMechs := []string{
		"PLAIN",
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

func (x *kvImplAuth) handleSASLAuthRequest(source *KvClient, pak *memd.Packet, next func()) {
	authMech := string(pak.Key)

	log.Printf("AUTH MECH: %+v", authMech)

	if authMech != "PLAIN" {
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSASLAuth,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthError,
		})
		return
	}

	authPieces := strings.Split(string(pak.Value), string([]byte{0}))
	log.Printf("AUTH PIECES: %+v", authPieces)

	if len(authPieces) != 3 {
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSASLAuth,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthError,
		})
		return
	}

	username := authPieces[1]
	password := authPieces[2]

	log.Printf("Got Auth Request: %s/%s", username, password)

	// TODO(brett19): Implement proper authentication...
	if username != "Administrator" || password != "password" {
		source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: memd.CmdSASLAuth,
			Opaque:  pak.Opaque,
			Status:  memd.StatusAuthError,
		})
		return
	}

	source.SetAuthenticatedUserName(username)

	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdSASLAuth,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
	})
}

func (x *kvImplAuth) handleSASLStepRequest(source *KvClient, pak *memd.Packet, next func()) {
	next()
}

func (x *kvImplAuth) handleSelectBucketRequest(source *KvClient, pak *memd.Packet, next func()) {
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
