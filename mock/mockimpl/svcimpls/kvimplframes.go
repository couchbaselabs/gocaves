package svcimpls

import (
	"encoding/json"
	"log"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

type kvImplFrames struct {
}

// Register installs validation of request frames. kv_engine validates frames
// before it dispatches any command, so this hook must see every request first.
// Hooks run in reverse order of registration, so it is registered last.
func (x *kvImplFrames) Register(h *hookHelper) {
	h.KvInHooks.Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		// An unknown command is answered at the end of the hook chain.
		status, context := h.validateRequest(source, pak)
		switch status {
		case memd.StatusSuccess, memd.StatusUnknownCommand:
			next()
		case memd.StatusCollectionUnknown:
			// get_scope_id reports no manifest uid for an unknown collection.
			writePacketAsIs(source, unknownCollectionResponse(pak, status, 0), start)
		case statusDisconnect:
			if err := source.Close(); err != nil {
				log.Printf("failed to disconnect %s: %s", source.RemoteAddr(), err)
			}
		default:
			writeErrorContextReply(source, pak, status, context, start)
		}
	})
}

// validateRequest returns the status and error context kv_engine's request
// validator refuses a request with, or success. kv_engine answers an unknown
// command, and checks the request header, before it parses the frames.
func (h *hookHelper) validateRequest(source mock.KvClient, pak *memd.Packet) (memd.StatusCode, string) {
	if pak.Magic != memd.CmdMagicReq {
		return memd.StatusSuccess, ""
	}
	if !h.kvCommands[pak.Command] {
		return memd.StatusUnknownCommand, ""
	}
	if status, context := checkHeader(source, pak); status != memd.StatusSuccess {
		return status, context
	}
	if hasFrames(pak) {
		if status, context := checkFrames(pak); status != memd.StatusSuccess {
			return status, context
		}
	}
	if status := lookUpScope(source, pak); status != memd.StatusSuccess {
		return status, ""
	}
	return checkCommand(source, pak)
}

// statusDisconnect is the status validateRequest refuses a request with
// whose validation throws in kv_engine, which closes the connection.
const statusDisconnect = memd.StatusCode(0xffff)

// lookUpScope looks up the scope of the collection of a request as the end
// of verify_header does for a user with privileges on scopes or collections
// of a bucket with collections, which every bucket of 7.0.5 has, before the
// command's validator checks the key. A collection other than the default one
// that names no collection is unknown_collection; default_engine knows the
// default collection only. Building the CollectionID
// of a reserved id, 2 to 7, or of a key without a stop byte throws, and
// kv_engine closes the connection.
func lookUpScope(source mock.KvClient, pak *memd.Packet) memd.StatusCode {
	bucket := source.SelectedBucket()
	if bucket == nil || !source.HasFeature(memd.FeatureCollections) || !memd.IsCommandCollectionEncoded(pak.Command) ||
		!kvPrivileges(source, bucket).HasScopePrivileges() {
		return memd.StatusSuccess
	}
	switch id := pak.CollectionID; {
	case !keyHasStopByte(pak), id >= 2 && id <= 7:
		return statusDisconnect
	case id == 0:
		return memd.StatusSuccess
	}
	if scope, _ := bucket.CollectionManifest().GetByID(pak.CollectionID); scope == "" {
		return memd.StatusCollectionUnknown
	}
	return memd.StatusSuccess
}

// checkCommand applies the checks a command's validator in
// daemon/mcbp_validators.cc@v7.0.5 makes after verify_header, for the
// commands whose handlers do not.
func checkCommand(source mock.KvClient, pak *memd.Packet) (memd.StatusCode, string) {
	switch pak.Command {
	case memd.CmdGetLocked:
		if context := checkDocumentKey(source, pak); context != "" {
			return memd.StatusInvalidArgs, context
		}
		if len(pak.Extras) != 0 && len(pak.Extras) != 4 {
			return memd.StatusInvalidArgs, "Request extras must be of length 0 or 4"
		}
	case memd.CmdGetMeta:
		if len(pak.Extras) > 1 {
			return memd.StatusInvalidArgs, "Request extras must be of length 0 or 1"
		}
		if context := checkDocumentKey(source, pak); context != "" {
			return memd.StatusInvalidArgs, context
		}
		if len(pak.Extras) == 1 && pak.Extras[0] > 2 {
			return memd.StatusInvalidArgs, "Request extras invalid"
		}
	case memd.CmdHello:
		if len(pak.Value)%2 != 0 {
			return memd.StatusInvalidArgs, "Request value must be of even length"
		}
	case memd.CmdStat:
		if len(pak.Value) > 0 {
			if pak.Datatype&uint8(memd.DatatypeFlagJSON) == 0 {
				return memd.StatusInvalidArgs, "Datatype must be JSON"
			}
			if !json.Valid(pak.Value) {
				return memd.StatusInvalidArgs, "value is not valid JSON"
			}
		}
	case memd.CmdGetErrorMap:
		if pak.Vbucket != 0 {
			return memd.StatusInvalidArgs, "Request vbucket id must be 0"
		}
		if len(pak.Value) != 2 {
			return memd.StatusInvalidArgs, "Request value must be of length 2"
		}
	case memd.CmdObserveSeqNo:
		if len(pak.Value) != 8 {
			return memd.StatusInvalidArgs, "Request value must be of length 8"
		}
	case memd.CmdGet, memd.CmdGetReplica, memd.CmdSet, memd.CmdAdd, memd.CmdReplace, memd.CmdAppend,
		memd.CmdPrepend, memd.CmdDelete, memd.CmdIncrement, memd.CmdDecrement, memd.CmdTouch, memd.CmdGAT,
		memd.CmdUnlockKey:
		// get_validator, set_replace_validator, add_validator and the others
		// check the key after verify_header.
		if context := checkDocumentKey(source, pak); context != "" {
			return memd.StatusInvalidArgs, context
		}
	case memd.CmdSelectBucket:
		// select_bucket_validator: MAX_BUCKET_NAME_LENGTH
		if len(pak.Key) > 100 {
			return memd.StatusInvalidArgs, "Request key length exceeds maximum"
		}
	case memd.CmdCollectionsGetManifest:
		// collections_get_manifest_validator
		if pak.Vbucket != 0 {
			return memd.StatusInvalidArgs, "Request vbucket id must be 0"
		}
	case memd.CmdCollectionsGetID:
		// collections_get_id_validator
		if len(pak.Key) != 0 && len(pak.Value) != 0 {
			return memd.StatusInvalidArgs, "Cannot set both key and value"
		}
		if pak.Vbucket != 0 {
			return memd.StatusInvalidArgs, "Request vbucket id must be 0"
		}
	}
	return memd.StatusSuccess, ""
}

// hasFrames reports whether a request carries frames. The reader lists every
// frame in Packet.UnsupportedFrames.
func hasFrames(pak *memd.Packet) bool {
	return len(pak.UnsupportedFrames) > 0
}

// writeErrorContextReply answers a request with status and the error context
// body kv_engine sends with it, as its request validators do, without
// remapping the status.
func writeErrorContextReply(source mock.KvClient, pak *memd.Packet, status memd.StatusCode, context string, start time.Time) {
	writePacketAsIs(source, errorContextResponse(pak, status, context), start)
}

// errorContextResponse is the response to a request with status and the
// error context body kv_engine sends with it. kv_engine sends no body for a
// refusal without a context, and does not escape "<", ">" and "&" in one.
func errorContextResponse(pak *memd.Packet, status memd.StatusCode, context string) *memd.Packet {
	resp := &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
		Status:  status,
	}
	if context == "" {
		return resp
	}
	var body struct {
		Error struct {
			Context string `json:"context"`
		} `json:"error"`
	}
	body.Error.Context = context
	resp.Datatype = uint8(memd.DatatypeFlagJSON)
	resp.Value = marshalJSON(body)
	return resp
}
