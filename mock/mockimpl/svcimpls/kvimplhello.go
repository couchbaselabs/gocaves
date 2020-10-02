package svcimpls

import (
	"encoding/binary"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

type kvImplHello struct {
}

func (x *kvImplHello) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdHello, x.handleHelloRequest)
}

func (x *kvImplHello) handleHelloRequest(source mock.KvClient, pak *memd.Packet) {
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
