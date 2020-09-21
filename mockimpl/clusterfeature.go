package mockimpl

// ClusterFeature specifies a specific cluster feature
type ClusterFeature string

// The following is a list of possible cluster features
const (
	ClusterFeatureDurations = "durations"
)

func clusterFeatureListContains(list []ClusterFeature, feature ClusterFeature) bool {
	// An empty list acts like a completely full list
	if len(list) == 0 {
		return true
	}

	// Check if we actually have the service in the list
	for _, listFeat := range list {
		if listFeat == feature {
			return true
		}
	}
	return false
}
