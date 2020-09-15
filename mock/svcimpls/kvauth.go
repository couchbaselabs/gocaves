package svcimpls

import (
	"github.com/couchbaselabs/gocaves/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

// KvAuthSasl provides an implementation of kv authentication SASL.
func KvAuthSasl() {

}

// MemdHandler defines the handler for a memcached operation
type MemdHandler func(cli *mock.KvClient, pak *memd.Packet, next MemdHandler)

func xxtest() {
}
