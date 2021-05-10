package svcimpls

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/couchbaselabs/gocaves/contrib/pathparse"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"github.com/couchbaselabs/gocaves/mock/mockmr"
	"log"
	"strconv"
	"strings"
)

type viewImplQuery struct {
}

func (x *viewImplQuery) Register(h *hookHelper) {
	h.RegisterViewHandler("GET", "/*/_design/*/_view/*", x.handleQuery)
}

type jsonViewResult struct {
	Rows      []jsonViewRow `json:"rows"`
	TotalRows int           `json:"total_rows"`
	DebugInfo jsonViewDebug `json:"debug_info"`
}

type jsonViewDebugMainGroup struct {
	Cleanups int `json:"cleanups"`
}

type jsonViewDebug struct {
	MainGroup jsonViewDebugMainGroup `json:"main_group"`
}

type jsonViewRow struct {
	ID    string      `json:"id"`
	Key   interface{} `json:"key"`
	Value interface{} `json:"value"`
}

func (x *viewImplQuery) handleQuery(source mock.ViewService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := pathparse.ParseParts(req.URL.Path, "/*/_design/*/_view/*")
	bucketName := pathParts[0]
	ddocName := pathParts[1]
	viewName := pathParts[2]

	options := req.URL.Query()

	// views only operate on default views.
	if !source.CheckAuthenticated(mockauth.PermissionViewsManage, bucketName, "_default", "_default", req) {
		return &mock.HTTPResponse{
			StatusCode: 401,
			Body:       bytes.NewReader([]byte{}),
		}
	}

	bucket := source.Node().Cluster().GetBucket(bucketName)
	if bucket == nil {
		return &mock.HTTPResponse{
			StatusCode: 404,
			Body:       bytes.NewReader([]byte(mockmr.ErrNotFound.Error())),
		}
	}

	docs, err := bucket.Store().GetAll(0, 0)
	if err != nil {
		log.Printf("Failed to get view query docs: %v", err)
		return &mock.HTTPResponse{
			StatusCode: 500,
			Body:       bytes.NewReader([]byte("internal server error")),
		}
	}

	keysOpt := options.Get("keys")
	var keys []string
	if keysOpt != "" {
		keys = strings.Split(keysOpt, ",")
	}

	totalResults, results, err := bucket.ViewIndexManager().Execute(mockmr.ExecuteOptions{
		Data:      docs,
		DesignDoc: ddocName,
		View:      viewName,

		Skip:          x.stringToInt(options.Get("skip")),
		StartKey:      options.Get("startkey"),
		StartKeyDocID: options.Get("startkey_docid"),
		EndKey:        options.Get("endkey"),
		EndKeyDocID:   options.Get("endkey_docid"),
		InclusiveEnd:  x.stringToBool(options.Get("inclusive_end"), true),
		Key:           options.Get("key"),
		Keys:          keys,
		Descending:    x.stringToBool(options.Get("descending"), false),
		Reduce:        x.stringToBool(options.Get("reduce"), true),
		Group:         x.stringToBool(options.Get("group"), false),
		GroupLevel:    x.stringToInt(options.Get("group_level")),
		Limit:         x.stringToInt(options.Get("limit")),
	})
	if err != nil {
		log.Printf("Failed to execute view query: %v", err)
		if errors.Is(err, mockmr.ErrNotFound) {
			return &mock.HTTPResponse{
				StatusCode: 404,
				Body:       bytes.NewReader([]byte(err.Error())),
			}
		} else if errors.Is(err, mockmr.ErrInvalidParameters) {
			return &mock.HTTPResponse{
				StatusCode: 400,
				Body:       bytes.NewReader([]byte(err.Error())),
			}
		}

		return &mock.HTTPResponse{
			StatusCode: 500,
			Body:       bytes.NewReader([]byte("internal server error")),
		}
	}

	rows := []jsonViewRow{} // Make sure this isn't sent as null.
	for _, res := range results.Rows {
		rows = append(rows, jsonViewRow{
			ID:    res.ID,
			Key:   res.Key,
			Value: res.Value,
		})
	}

	output := jsonViewResult{
		Rows:      rows,
		TotalRows: totalResults,
	}

	if x.stringToBool(options.Get("debug"), false) {
		output.DebugInfo = jsonViewDebug{
			MainGroup: jsonViewDebugMainGroup{
				Cleanups: 0,
			},
		}
	}

	b, err := json.Marshal(output)
	if err != nil {
		log.Printf("Failed to marshal view query result: %v", err)
		return &mock.HTTPResponse{
			StatusCode: 500,
			Body:       bytes.NewReader([]byte("internal server error")),
		}
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader(b),
	}

}

func (x *viewImplQuery) stringToInt(num string) int {
	i, err := strconv.Atoi(num)
	if err != nil {
		return 0
	}

	return i
}

func (x *viewImplQuery) stringToBool(val string, def bool) bool {
	i, err := strconv.ParseBool(val)
	if err != nil {
		return def
	}

	return i
}
