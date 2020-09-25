package checks

import "encoding/base64"

var registeredTests []*Check

// Register registers a check suite check
func Register(group, name string, fn func(*T), desc string) {
	descBytes, _ := base64.StdEncoding.DecodeString(desc)

	registeredTests = append(registeredTests, &Check{
		Group:       group,
		Name:        name,
		Description: string(descBytes),
		Fn:          fn,
	})
}

func getAllRegisteredChecks() []*Check {
	return registeredTests
}
