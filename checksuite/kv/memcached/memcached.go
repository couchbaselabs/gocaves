package crud

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/checks"
)

// CheckMemcachedBasic confirms that the SDK can successfully connect to a memcached bucket and then send requests to it.
func CheckMemcachedBasic(t *checks.T) {
	t.UseManagementConnString()
	t.SetBucket("memd")
	t.Collection().KvExpectReq().
		Cmd(memd.CmdSet).
		Key("test-doc").
		Wait()

	t.Collection().KvExpectReq().
		Cmd(memd.CmdGet).
		Key("test-doc").
		Wait()
}
