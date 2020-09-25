package checks

// Check represents a single check that has been registered.
type Check struct {
	Group       string
	Name        string
	Description string
	Fn          func(t *T)
}
