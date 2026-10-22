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
		if status == memd.StatusSuccess || status == memd.StatusUnknownCommand {
			next()
			return
		}
		writeErrorContextReply(source, pak, status, context, start)
	})
}

// validateRequest returns the status and error context kv_engine's request
// validator refuses a request with, or success. kv_engine answers an unknown
// command, and checks the request header, before it parses the frames. The
// header is checked only for a request with frames.
func (h *hookHelper) validateRequest(source mock.KvClient, pak *memd.Packet) (memd.StatusCode, string) {
	if pak.Magic != memd.CmdMagicReq {
		return memd.StatusSuccess, ""
	}
	if !h.kvCommands[pak.Command] {
		return memd.StatusUnknownCommand, ""
	}
	if !hasFrames(pak) {
		return memd.StatusSuccess, ""
	}
	if status, context := checkHeader(source, pak); status != memd.StatusSuccess {
		return status, context
	}
	return checkFrames(pak)
}

// hasFrames reports whether a request carries a frame validateRequest checks
// the request for.
func hasFrames(pak *memd.Packet) bool {
	return pak.DurabilityLevelFrame != nil || pak.BarrierFrame != nil || len(pak.UnsupportedFrames) > 0
}

// writeErrorContextReply answers a request with status and the error context
// body kv_engine sends with it.
func writeErrorContextReply(source mock.KvClient, pak *memd.Packet, status memd.StatusCode, context string, start time.Time) {
	var body struct {
		Error struct {
			Context string `json:"context"`
		} `json:"error"`
	}
	body.Error.Context = context
	value, err := json.Marshal(body)
	if err != nil {
		log.Printf("failed to encode error context: %s", err)
		value = nil
	}
	writePacketToSource(source, &memd.Packet{
		Magic:    memd.CmdMagicRes,
		Command:  pak.Command,
		Opaque:   pak.Opaque,
		Status:   status,
		Datatype: uint8(memd.DatatypeFlagJSON),
		Value:    value,
	}, start)
}
