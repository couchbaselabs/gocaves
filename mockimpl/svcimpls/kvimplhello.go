package svcimpls

import (
	"encoding/binary"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/hooks"
	"github.com/couchbaselabs/gocaves/mock"
)

type kvImplHello struct {
}

func (x *kvImplHello) Register(hooks *hooks.KvHookManager) {
	reqExpects := hooks.Expect().Magic(memd.CmdMagicReq)

	reqExpects.Cmd(memd.CmdHello).Handler(x.handleHelloRequest)
	reqExpects.Cmd(memd.CmdGetErrorMap).Handler(x.handleGetErrorMap)
}

func (x *kvImplHello) handleHelloRequest(source mock.KvClient, pak *memd.Packet, next func()) {
	enabledFeatures := make([]memd.HelloFeature, 0)

	numFeatures := len(pak.Value) / 2
	for featureIdx := 0; featureIdx < numFeatures; featureIdx++ {
		featureCodeID := binary.BigEndian.Uint16(pak.Value[featureIdx*2:])
		featureCode := memd.HelloFeature(featureCodeID)

		if featureCode == memd.FeatureDatatype {
			enabledFeatures = append(enabledFeatures, featureCode)
		}
	}

	enabledBytes := make([]byte, len(enabledFeatures)*2)
	for featureIdx, featureCode := range enabledFeatures {
		binary.BigEndian.PutUint16(enabledBytes[featureIdx*2:], uint16(featureCode))
	}

	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdHello,
		Opaque:  pak.Opaque,
		Value:   enabledBytes,
		Status:  memd.StatusSuccess,
	})
}

func (x *kvImplHello) handleGetErrorMap(source mock.KvClient, pak *memd.Packet, next func()) {
	// TODO(brett19): Implement some semblance of a realistic error map...
	source.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdGetErrorMap,
		Opaque:  pak.Opaque,
		Status:  memd.StatusUnknownCommand,
	})
}
