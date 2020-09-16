package mock

// ServiceType represents the various service types.
type ServiceType uint

// This represents the various service types that a particular
// node could have enabled.
const (
	ServiceTypeMgmt      = ServiceType(1)
	ServiceTypeKeyValue  = ServiceType(2)
	ServiceTypeViews     = ServiceType(3)
	ServiceTypeQuery     = ServiceType(4)
	ServiceTypeSearch    = ServiceType(5)
	ServiceTypeAnalytics = ServiceType(6)
)

func serviceTypeListContains(list []ServiceType, service ServiceType) bool {
	// An empty list acts like a completely full list
	if len(list) == 0 {
		return true
	}

	// Check if we actually have the service in the list
	for _, listSvc := range list {
		if listSvc == service {
			return true
		}
	}
	return false
}
