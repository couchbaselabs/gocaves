package crud

import (
	checkregistry "github.com/couchbaselabs/gocaves/checkregistry"
)

// CheckSetGet confirms that the SDK can perform Set and Get
// operations appropriately.
/*
Should be implemented as:
	type TestDoc struct {
		Foo string
		Bar bool
	}
	t := harness.StartTest("core/SetGet")
	t.RequireFeature("replicas")
	collection := t.Collection() //

	testValOut := TestDoc{
		Foo: "hello",
		Bar: true,
	}

	_, err := collection.Upsert("testdoc", GetOptions{

	}, testValOut)
	assert(err == nil)

	doc, err := collection.Get("testdoc", GetOptions{})
	assert(err == nil)

	testValIn := TestDoc{}
	doc.Value(&testValIn)

	t.End()
*/
func CheckSetGet(t *checkregistry.T) {
	//t.RequireFeature(checkregistry.FeatureReplicas)
	//t.Cluster()

	//t.GlobalCluster()

}
