package testmode

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/checks"
)

type testRun struct {
	RunID      string
	StartTime  time.Time
	ClientName string
	RunGroup   *checks.TestRunner
}

type jsonPacketExtraFrame map[string]interface{}

type jsonRecordedPacket struct {
	ID                     uint64 `json:"id"`
	Direction              string `json:"dir"`
	SrcAddr                string `json:"srcAddr"`
	SrcName                string `json:"srcName"`
	DestAddr               string `json:"destAddr"`
	DestName               string `json:"destName"`
	IsTLS                  bool   `json:"isTls"`
	SelectedBucketName     string `json:"selectedBucketName"`
	ResolvedScopeName      string `json:"resolvedScopeName"`
	ResolvedCollectionName string `json:"resolvedCollectionName"`

	Magic        uint8                  `json:"magic"`
	MagicStr     string                 `json:"magicString"`
	Command      uint8                  `json:"command"`
	CommandStr   string                 `json:"commandString"`
	Datatype     uint8                  `json:"datatype"`
	Status       uint16                 `json:"status"`
	StatusStr    string                 `json:"statusString"`
	Vbucket      uint16                 `json:"vbId"`
	Opaque       uint32                 `json:"opaque"`
	CasHex       string                 `json:"casHex"`
	CollectionID uint32                 `json:"collectionId"`
	KeyBase64    string                 `json:"keyBase64"`
	ExtrasBase64 string                 `json:"extrasBase64"`
	ValueBase64  string                 `json:"valueBase64"`
	ExtFrames    []jsonPacketExtraFrame `json:"extFrames"`
}

type jsonTest struct {
	Name        string               `json:"name"`
	Description string               `json:"desc"`
	Status      string               `json:"status"`
	Logs        []string             `json:"logs"`
	Packets     []jsonRecordedPacket `json:"packets"`
}

type jsonRunReport struct {
	MinVersion int        `json:"minversion"`
	Version    int        `json:"version"`
	ID         string     `json:"id"`
	CreatedAt  string     `json:"createdAt"`
	ClientName string     `json:"client"`
	Tests      []jsonTest `json:"tests"`
}

func testStatusToString(status checks.TestStatus) string {
	switch status {
	case checks.TestStatusSkipped:
		return "skipped"
	case checks.TestStatusFailed:
		return "failed"
	case checks.TestStatusSuccess:
		return "success"
	default:
		return fmt.Sprintf("unknown:%d", status)
	}
}

func cmdMagicToString(magic memd.CmdMagic) string {
	switch magic {
	case memd.CmdMagicReq:
		return "request"
	case memd.CmdMagicRes:
		return "response"
	default:
		return fmt.Sprintf("unk:%d", magic)
	}
}

func durabilityLevelToString(level memd.DurabilityLevel) string {
	switch level {
	case memd.DurabilityLevelMajority:
		return "majority"
	case memd.DurabilityLevelMajorityAndPersistOnMaster:
		return "majorityAndPersistOnMaster"
	case memd.DurabilityLevelPersistToMajority:
		return "persistToMajority"
	default:
		return fmt.Sprintf("unk:%d", level)
	}
}

func translateCapturedPacket(pak *checks.RecordedPacket) jsonRecordedPacket {
	var jpak jsonRecordedPacket
	jpak.ID = pak.ID
	if pak.WasSent {
		jpak.Direction = "out"
	} else {
		jpak.Direction = "in"
	}
	jpak.SrcAddr = pak.SrcAddr
	jpak.SrcName = pak.SrcName
	jpak.DestAddr = pak.DestAddr
	jpak.DestName = pak.DestName
	jpak.IsTLS = pak.IsTLS
	jpak.SelectedBucketName = pak.SelectedBucketName
	jpak.ResolvedScopeName = pak.ResolvedScopeName
	jpak.ResolvedCollectionName = pak.ResolvedCollectionName

	jpak.Magic = uint8(pak.Data.Magic)
	jpak.MagicStr = cmdMagicToString(pak.Data.Magic)
	jpak.Command = uint8(pak.Data.Command)
	jpak.CommandStr = pak.Data.Command.Name()
	jpak.Datatype = pak.Data.Datatype
	jpak.Status = uint16(pak.Data.Status)
	jpak.StatusStr = pak.Data.Status.KVText()
	jpak.Vbucket = pak.Data.Vbucket
	jpak.Opaque = pak.Data.Opaque
	jpak.CasHex = strconv.FormatUint(pak.Data.Cas, 16)
	jpak.CollectionID = pak.Data.CollectionID
	jpak.KeyBase64 = base64.StdEncoding.EncodeToString(pak.Data.Key)
	jpak.ValueBase64 = base64.StdEncoding.EncodeToString(pak.Data.Value)
	jpak.ExtrasBase64 = base64.StdEncoding.EncodeToString(pak.Data.Extras)

	if pak.Data.BarrierFrame != nil {
		jpak.ExtFrames = append(jpak.ExtFrames, map[string]interface{}{
			"type": "barrier",
		})
	}
	if pak.Data.DurabilityLevelFrame != nil {
		frame := pak.Data.DurabilityLevelFrame
		jpak.ExtFrames = append(jpak.ExtFrames, map[string]interface{}{
			"type":     "durabilityLevel",
			"level":    uint8(frame.DurabilityLevel),
			"levelStr": durabilityLevelToString(frame.DurabilityLevel),
		})
	}
	if pak.Data.DurabilityTimeoutFrame != nil {
		frame := pak.Data.DurabilityTimeoutFrame
		jpak.ExtFrames = append(jpak.ExtFrames, map[string]interface{}{
			"type":        "durabilityLevel",
			"durationStr": frame.DurabilityTimeout.String(),
		})
	}
	if pak.Data.StreamIDFrame != nil {
		frame := pak.Data.StreamIDFrame
		jpak.ExtFrames = append(jpak.ExtFrames, map[string]interface{}{
			"type":     "streamId",
			"streamId": frame.StreamID,
		})
	}
	if pak.Data.OpenTracingFrame != nil {
		frame := pak.Data.OpenTracingFrame
		jpak.ExtFrames = append(jpak.ExtFrames, map[string]interface{}{
			"type":       "openTracing",
			"dataBase64": base64.StdEncoding.EncodeToString(frame.TraceContext),
		})
	}
	if pak.Data.ServerDurationFrame != nil {
		frame := pak.Data.ServerDurationFrame
		jpak.ExtFrames = append(jpak.ExtFrames, map[string]interface{}{
			"type":        "serverDuration",
			"durationStr": frame.ServerDuration.String(),
		})
	}
	for _, frame := range pak.Data.UnsupportedFrames {
		jpak.ExtFrames = append(jpak.ExtFrames, map[string]interface{}{
			"type":       fmt.Sprintf("unk:%d", frame.Type),
			"dataBase64": base64.StdEncoding.EncodeToString(frame.Data),
		})
	}

	return jpak
}

func translateTest(result *checks.TestResult) jsonTest {
	var jtest jsonTest
	jtest.Name = result.Name
	jtest.Description = result.Description
	jtest.Status = testStatusToString(result.Status)
	jtest.Logs = result.Logs
	for _, pak := range result.Packets {
		jtest.Packets = append(jtest.Packets, translateCapturedPacket(pak))
	}
	return jtest
}

func (m *testRun) GenerateReport() jsonRunReport {
	var jrun jsonRunReport
	jrun.MinVersion = 1
	jrun.Version = 1
	jrun.ID = m.RunID
	jrun.CreatedAt = m.StartTime.Format(time.RFC3339)
	jrun.ClientName = m.ClientName

	results := m.RunGroup.Results()
	for _, result := range results {
		jrun.Tests = append(jrun.Tests, translateTest(result))
	}

	return jrun
}
