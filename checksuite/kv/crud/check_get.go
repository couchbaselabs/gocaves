package crud

import (
	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/checks"
	"github.com/stretchr/testify/assert"
)

// CheckSetGet confirms that the SDK can perform Set and Get
// operations appropriately.
/*
Should be implemented as:
	@CavesTest('core/SetGet')
	@CavesRequireFeature('3replicas')
	function testCoreSetGet(t *TestCase) {
		collection := t.Collection()

		testDoc := {
			foo: 'hello',
			bar: true,
		}

		await collection.upsert('test-doc', testDoc)

		doc := await collection.get('test-doc')
		assert.deepEqual(doc.content, testDoc)
	}
*/
func CheckSetGet(t *checks.T) {
	t.RequireFeature(checks.TestFeature3Replicas)

	_, spak := t.Collection().KvExpectReq().
		Cmd(memd.CmdSet).
		Wait()
	assert.Equal(t, spak.Key, []byte("test-doc"))

	_, gpak := t.Collection().KvExpectReq().
		Cmd(memd.CmdGet).
		Wait()
	assert.Equal(t, gpak.Key, []byte("test-doc"))
}
