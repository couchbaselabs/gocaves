package crud

import (
	checkregistry "github.com/couchbaselabs/gocaves/checkregistry"
)

// CheckSetGet confirms that the SDK can perform Set and Get
// operations appropriately.
/*
Should be implemented as:
	@CavesTest('core/SetGet')
	@CavesRequireFeature('replicas')
	function testCoreSetGet(t *TestCase) {
		collection := t.Collection()

		await collection.upsert('testdoc', {
			foo: 'hello',
			bar: true,
		})

		doc := await collection.get('testdoc')
		assert.deepEqual()
	}
*/
func CheckSetGet(t *checkregistry.T) {
	//t.RequireFeature(checkregistry.FeatureReplicas)
	//t.Cluster()

	//t.GlobalCluster()

}
