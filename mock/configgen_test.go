package mock

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"
)

func testReadJSONFile(t *testing.T, path string) interface{} {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read test data file: %s", err)
	}

	var testData interface{}
	json.Unmarshal(data, &testData)
	return testData
}

func testCompareSubLayout(t *testing.T, actual interface{}, expected interface{}, path string) bool {
	if reflect.TypeOf(actual) != reflect.TypeOf(expected) {
		t.Errorf("Field %s was not of expected type (expected %T, actual %T)", path, expected, actual)
		return false
	}

	switch actualTyped := actual.(type) {
	case map[string]interface{}:
		expectedTyped := expected.(map[string]interface{})
		allPathsMatched := true

		for k, v := range actualTyped {
			thisPath := fmt.Sprintf("%s.%s", path, k)
			otherV, foundOther := expectedTyped[k]
			if !foundOther {
				t.Errorf("Field %s was found in actual, but not in expected", thisPath)
				allPathsMatched = false
				continue
			}

			if !testCompareSubLayout(t, v, otherV, thisPath) {
				allPathsMatched = false
			}
		}

		for otherK := range expectedTyped {
			thisPath := fmt.Sprintf("%s.%s", path, otherK)
			_, foundActual := actualTyped[otherK]
			if !foundActual {
				t.Errorf("Field %s was found in expected, but not in actual", thisPath)
				allPathsMatched = false
				continue
			}
		}

		return allPathsMatched
	case []interface{}:
		expectedTyped := expected.([]interface{})

		if len(actualTyped) != len(expectedTyped) {
			t.Errorf("Field %s had a different size (expected: %d, actual: %d)", path, len(actualTyped), len(expectedTyped))
			return false
		}

		allPathsMatched := true

		for idx, v := range actualTyped {
			otherV := expectedTyped[idx]
			thisPath := fmt.Sprintf("%s[%d]", path, idx)

			if !testCompareSubLayout(t, v, otherV, thisPath) {
				allPathsMatched = false
			}

		}

		return allPathsMatched
	case float64:
		return true
	case int:
		return true
	case string:
		return true
	case bool:
		return true
	}

	t.Errorf("Field %s was not of a supported type (expected: %T, actual: %T)", path, expected, actual)
	return false
}

func testCompareLayout(t *testing.T, actual interface{}, expected interface{}) {
	if !testCompareSubLayout(t, actual, expected, "$root") {
		t.FailNow()
	}
}

func TestClusterConfig70(t *testing.T) {
	t.Skipf("support for full cluster configs is not yet available")

	testConfig := testReadJSONFile(t, "testdata/cluster_config_70.json")

	cluster, _ := NewCluster(NewClusterOptions{
		EnabledFeatures: []ClusterFeature{},
		NumVbuckets:     1024,
		InitialNode:     NewNodeOptions{},
	})

	configBytes := cluster.GetConfig(nil)

	var actualConfig interface{}
	json.Unmarshal(configBytes, &actualConfig)

	testCompareLayout(t, actualConfig, testConfig)
}

func TestBucketConfig70(t *testing.T) {
	testConfig := testReadJSONFile(t, "testdata/bucket_config_70.json")

	cluster, _ := NewCluster(NewClusterOptions{
		EnabledFeatures: []ClusterFeature{},
		NumVbuckets:     1024,
		InitialNode:     NewNodeOptions{},
	})

	bucket, _ := cluster.AddBucket(NewBucketOptions{
		Name:        "default",
		Type:        BucketTypeCouchbase,
		NumReplicas: 1,
	})

	configBytes := bucket.GetConfig(nil)

	var actualConfig interface{}
	json.Unmarshal(configBytes, &actualConfig)

	testCompareLayout(t, actualConfig, testConfig)
}

func TestBucketTerseConfig70(t *testing.T) {
	testConfig := testReadJSONFile(t, "testdata/bucket_terseconfig_70.json")

	cluster, _ := NewCluster(NewClusterOptions{
		EnabledFeatures: []ClusterFeature{},
		NumVbuckets:     1024,
		InitialNode:     NewNodeOptions{},
	})

	bucket, _ := cluster.AddBucket(NewBucketOptions{
		Name:        "default",
		Type:        BucketTypeCouchbase,
		NumReplicas: 1,
	})

	configBytes := bucket.GetTerseConfig(nil)

	var actualConfig interface{}
	json.Unmarshal(configBytes, &actualConfig)

	testCompareLayout(t, actualConfig, testConfig)
}
