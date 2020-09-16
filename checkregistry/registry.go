package checkregistry

type TestFeature string

const (
	TestFeatureEnhancedPreparedStatements = "enhancedprepared"
	TestFeature3Replicas                  = "3replicas"
)

type T struct {
	Group string
	Name  string
}

func (t *T) RequireFeature(feature TestFeature) {

}

func (t *T) Cluster() {
}

// Register registers a check suite check
func Register(group, name string, fn func(t *T), desc string) {

}
