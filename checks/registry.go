package checks

var registeredTests []*Check

// Register registers a check suite check
func Register(group, name string, fn func(*T), desc string) {
	registeredTests = append(registeredTests, &Check{
		Group:       group,
		Name:        name,
		Description: desc,
		Fn:          fn,
	})
}

func getAllRegisteredChecks() []*Check {
	return registeredTests
}
