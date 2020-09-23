package mock

import (
	"encoding/json"
	"io/ioutil"
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

func NewErrorMap() (*ErrorMap, error) {
	b, err := ioutil.ReadFile("data/err_map65.json")
	if err != nil {
		return nil, err
	}

	var emap *ErrorMap
	if err := json.Unmarshal(b, &emap); err != nil {
		return nil, err
	}

	return emap, nil
}

func (errMap *ErrorMap) Marshal() ([]byte, error) {
	return json.Marshal(errMap)
}
