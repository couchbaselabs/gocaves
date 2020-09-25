package checks

// TestFeature represents a feature that may be required by a test.
type TestFeature string

// The following is a list of all supported features.
const (
	TestFeatureEnhancedPreparedStatements = "enhancedprepared"
	TestFeature3Replicas                  = "3replicas"
)
