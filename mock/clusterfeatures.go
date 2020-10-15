package mock

// ClusterNodeFeature specifies a specific cluster feature
type ClusterNodeFeature string

// The following is a list of possible cluster features
const (
	ClusterNodeFeatureDurations = "durations"
	ClusterNodeFeatureTLS       = "tls"
)
