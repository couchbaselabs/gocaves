package svcimpls

import (
	"encoding/binary"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

type kvImplHello struct {
}

func (x *kvImplHello) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdHello, x.handleHelloRequest)
}

func (x *kvImplHello) handleHelloRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	isInFeatureList := func(features []memd.HelloFeature, feature memd.HelloFeature) bool {
		for _, foundFeature := range features {
			if foundFeature == feature {
				return true
			}
		}
		return false
	}

	// TODO(brett19): This list should actually be moderated by the server
	// version/features configured at the top level for the cluster.
	availableFeatures := []memd.HelloFeature{
		memd.FeatureDatatype,
		memd.FeatureTCPNoDelay,
		memd.FeatureSeqNo,
		memd.FeatureTCPDelay,
		memd.FeatureXattr,
		memd.FeatureXerror,
		memd.FeatureSelectBucket,
		memd.FeatureSnappy,
		memd.FeatureJSON,
		memd.FeatureDuplex,
		//memd.FeatureClusterMapNotif,
		memd.FeatureUnorderedExec,
		memd.FeatureDurations,
		memd.FeatureAltRequests,
		// memd.FeatureSyncReplication,
		memd.FeatureCollections,
		//memd.FeatureOpenTracing,
		memd.FeatureCreateAsDeleted,
	}
	enabledFeatures := make([]memd.HelloFeature, 0)

	numFeatures := len(pak.Value) / 2
	for featureIdx := 0; featureIdx < numFeatures; featureIdx++ {
		featureCodeID := binary.BigEndian.Uint16(pak.Value[featureIdx*2:])
		featureCode := memd.HelloFeature(featureCodeID)

		if isInFeatureList(availableFeatures, featureCode) {
			if !isInFeatureList(enabledFeatures, featureCode) {
				enabledFeatures = append(enabledFeatures, featureCode)
			}
		}
	}

	source.SetFeatures(enabledFeatures)

	enabledBytes := make([]byte, len(enabledFeatures)*2)
	for featureIdx, featureCode := range enabledFeatures {
		binary.BigEndian.PutUint16(enabledBytes[featureIdx*2:], uint16(featureCode))
	}

	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdHello,
		Opaque:  pak.Opaque,
		Value:   enabledBytes,
		Status:  memd.StatusSuccess,
	}, start)
}
