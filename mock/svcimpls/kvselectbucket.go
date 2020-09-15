package svcimpls

import "github.com/couchbaselabs/gocaves/mock"

type kvCliBucketState struct {
	selectedBucket string
}

func setKvCliSelectedBucket(cli *mock.KvClient, bucket string) {
	var state *kvCliBucketState
	cli.GetContext(&state)
	state.selectedBucket = bucket
}

func getKvCliSelectedBucket(cli *mock.KvClient) string {
	var state *kvCliBucketState
	cli.GetContext(&state)
	return state.selectedBucket
}

func MockKvSelectBucket() {

}
