package mock

import (
	"encoding/json"

	mockdata "github.com/couchbaselabs/gocaves/mock/data"
)

// ErrorMap specifies a collection of ErrorMapErrors.
type ErrorMap struct {
	Version  int                      `json:"version"`
	Revision int                      `json:"revision"`
	Errors   map[string]ErrorMapError `json:"errors"`
}

// ErrorMapError specifies a specific error.
type ErrorMapError struct {
	Name  string   `json:"name"`
	Desc  string   `json:"desc"`
	Attrs []string `json:"attrs"`
}

// NewErrorMap creates a new error map
func NewErrorMap() (*ErrorMap, error) {
	b, err := mockdata.Asset("err_map70.json")
	if err != nil {
		return nil, err
	}

	var emap *ErrorMap
	if err := json.Unmarshal(b, &emap); err != nil {
		return nil, err
	}

	return emap, nil
}

// Marshal marshalls the error map to JSON.
func (errMap *ErrorMap) Marshal() ([]byte, error) {
	return json.Marshal(errMap)
}
