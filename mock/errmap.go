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

// ErrorMapRetry specifies a specific error retry strategy.
type ErrorMapRetry struct {
	Strategy    string `json:"strategy"`
	Interval    int    `json:"interval"`
	After       int    `json:"after"`
	MaxDuration int    `json:"max-duration"`
	Ceil        int    `json:"ceil,omitempty"`
}

// ErrorMapError specifies a specific error.
type ErrorMapError struct {
	Name  string         `json:"name"`
	Desc  string         `json:"desc"`
	Attrs []string       `json:"attrs"`
	Retry *ErrorMapRetry `json:"retry,omitempty"`
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

// Extend adds a new error entry for this error map.
func (errMap *ErrorMap) Extend(key string, err ErrorMapError) *ErrorMap {
	errMap.Errors[key] = err
	return errMap
}

// Marshal marshalls the error map to JSON.
func (errMap *ErrorMap) Marshal() ([]byte, error) {
	return json.Marshal(errMap)
}
