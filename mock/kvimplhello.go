package mock

import (
	"encoding/binary"

	"github.com/couchbase/gocbcore/v9/memd"
)

type kvImplHello struct {
}

func (x *kvImplHello) Register(hooks *KvHookManager) {
	reqExpects := hooks.Expect().Magic(memd.CmdMagicReq)

	reqExpects.Cmd(memd.CmdHello).Handler(x.handleHelloRequest)
}

func (x *kvImplHello) handleHelloRequest(source *KvClient, pak *memd.Packet, next func()) {
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
		Value:   enabledBytes,
	})
}
