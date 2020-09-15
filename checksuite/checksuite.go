package checksuite

import (
	registry "github.com/couchbaselabs/gocaves/checkregistry"

	check_crud "github.com/couchbaselabs/gocaves/checksuite/crud"
)

// RegisterCheckFuncs registers all test suite methods
func RegisterCheckFuncs() {
	registry.Register("crud", "SetGet", check_crud.CheckSetGet,
		"Q2hlY2tTZXRHZXQgY29uZmlybXMgdGhhdCB0aGUgU0RLIGNhbiBwZXJmb3JtIFNldCBhbmQgR2V0Cm9wZXJhdGlvbnMgYXBwcm9wcmlhdGVseS4KClNob3VsZCBiZSBpbXBsZW1lbnRlZCBhczoKCXQgOj0gaGFybmVzcy5TdGFydFRlc3QoImNvcmUvU2V0R2V0IikKCgl0LkVuZCgpCg==")
}
