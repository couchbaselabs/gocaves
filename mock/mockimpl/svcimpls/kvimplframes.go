package svcimpls

import (
	"bytes"
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
	return checkCommand(source, pak)
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
	}
	return memd.StatusSuccess, ""
}

// hasFrames reports whether a request carries frames. The reader lists every
// frame in Packet.UnsupportedFrames.
func hasFrames(pak *memd.Packet) bool {
	return len(pak.UnsupportedFrames) > 0
}

// writeErrorContextReply answers a request with status and the error context
// body kv_engine sends with it. kv_engine sends no body for a refusal without
// a context.
func writeErrorContextReply(source mock.KvClient, pak *memd.Packet, status memd.StatusCode, context string, start time.Time) {
	if context == "" {
		writePacketToSource(source, &memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  status,
		}, start)
		return
	}
	var body struct {
		Error struct {
			Context string `json:"context"`
		} `json:"error"`
	}
	body.Error.Context = context
	// kv_engine does not escape '<', '>' and '&' in the context.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	var value []byte
	if err := enc.Encode(body); err != nil {
		log.Printf("failed to encode error context: %s", err)
	} else {
		value = bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
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
