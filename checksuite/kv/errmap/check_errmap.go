package errmap

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/checks"
	"github.com/couchbaselabs/gocaves/checksuite/kv/helpers"
	"github.com/couchbaselabs/gocaves/mock"
	"log"
	"time"
)

// CheckErrMapLinearRetry confirms that the SDK can successfully use an unknown status code to perform a lookup in
// the error map and retry accordingly.
func CheckErrMapLinearRetry(t *checks.T) {
	checkErrorMapRetry(t, 0x7ff0, "7ff0", mock.ErrorMapError{
		Name:  "DUMMY_ERROR_RETRY_EXPONENTIAL",
		Desc:  "Dummy retry error for exponential backoff",
		Attrs: []string{"auto-retry", "temp"},
		Retry: &mock.ErrorMapRetry{
			Strategy:    "constant",
			Interval:    10,
			After:       50,
			MaxDuration: 500,
		},
	})
}

// CheckErrMapConstantRetry confirms that the SDK can successfully use an unknown status code to perform a lookup in
// the error map and retry accordingly.
func CheckErrMapConstantRetry(t *checks.T) {
	checkErrorMapRetry(t, 0x7ff1, "7ff1", mock.ErrorMapError{
		Name:  "DUMMY_ERROR_RETRY_LINEAR",
		Desc:  "Dummy retry error for linear backoff",
		Attrs: []string{"auto-retry", "temp"},
		Retry: &mock.ErrorMapRetry{
			Strategy:    "linear",
			Interval:    2,
			After:       50,
			MaxDuration: 500,
			Ceil:        50,
		},
	})
}

// CheckErrMapExponentialRetry confirms that the SDK can successfully use an unknown status code to perform a lookup in
// the error map and retry accordingly.
func CheckErrMapExponentialRetry(t *checks.T) {
	checkErrorMapRetry(t, 0x7ff2, "7ff2", mock.ErrorMapError{
		Name:  "DUMMY_ERROR_RETRY_CONSTANT",
		Desc:  "Dummy retry error for constant backoff",
		Attrs: []string{"auto-retry", "temp"},
		Retry: &mock.ErrorMapRetry{
			Strategy:    "exponential",
			Interval:    2,
			After:       50,
			MaxDuration: 500,
			Ceil:        200,
		},
	})
}

func checkErrorMapRetry(t *checks.T, status memd.StatusCode, errMapStatus string, errMapErr mock.ErrorMapError) {
	t.RequireMock()

	handler := func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		helpers.WritePacketToSource(source, &memd.Packet{
			Command: pak.Command,
			Magic:   memd.CmdMagicRes,
			Opaque:  pak.Opaque,
			Status:  status,
		}, start)
	}

	errMap, err := mock.NewErrorMap()
	if err != nil {
		log.Printf("Failed to create error map: %s", err)
		t.End(nil)
		return
	}
	errMap.Extend(errMapStatus, errMapErr)

	b, err := errMap.Marshal()
	if err != nil {
		log.Printf("Failed to marshal error map: %s", err)
		t.End(nil)
		return
	}

	col := t.Collection()
	hooks := t.Mock().KvInHooks()
	hooks.Add(col.HookHelper(handler).Cmd(memd.CmdGet).Key("hello").Times(3).Build())
	hooks.Add(helpers.CreateErrorMapHook(memd.StatusSuccess, b))

	// This will effectively force 3 retries before the handler above yields and calls next.
	col.KvExpectReq().
		Cmd(memd.CmdGet).Key("hello").Wait()
}
