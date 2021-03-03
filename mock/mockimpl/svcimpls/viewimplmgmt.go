package svcimpls

import (
	"bytes"
	"encoding/json"
	"github.com/couchbaselabs/gocaves/contrib/pathparse"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"github.com/couchbaselabs/gocaves/mock/mockmr"
)

type viewImplMgmt struct {
}

func (x *viewImplMgmt) Register(h *hookHelper) {
	h.RegisterViewHandler("PUT", "/*/_design/*", x.handleUpsertDesignDocument)
	h.RegisterViewHandler("GET", "/*/_design/*", x.handleGetDesignDocument)
	h.RegisterViewHandler("DELETE", "/*/_design/*", x.handleDropDesignDocument)
}

type jsonView struct {
	Map    string `json:"map,omitempty"`
	Reduce string `json:"reduce,omitempty"`
}

type jsonDesignDocument struct {
	Views map[string]jsonView `json:"views,omitempty"`
}

func ddocToJsonDesignDocument(ddoc *mockmr.DesignDocument) jsonDesignDocument {
	jsonDdoc := jsonDesignDocument{
		Views: make(map[string]jsonView),
	}

	for _, view := range ddoc.Indexes {
		jsonDdoc.Views[view.Name] = jsonView{
			Map:    view.MapFunc,
			Reduce: view.ReduceFunc,
		}
	}

	return jsonDdoc
}

func (x *viewImplMgmt) handleUpsertDesignDocument(source mock.ViewService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := pathparse.ParseParts(req.URL.Path, "/*/_design/*")
	bucketName := pathParts[0]
	ddocName := pathParts[1]

	if !source.CheckAuthenticated(mockauth.PermissionViewsManage, bucketName, "", "", req) {
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

	var ddoc jsonDesignDocument
	err := json.Unmarshal(req.PeekBody(), &ddoc)
	if err != nil {
		return &mock.HTTPResponse{
			StatusCode: 500,
			Body:       bytes.NewReader([]byte("internal server error")),
		}
	}

	views := make([]*mockmr.Index, len(ddoc.Views))
	var i int
	for name, view := range ddoc.Views {
		views[i] = &mockmr.Index{
			Name:       name,
			MapFunc:    view.Map,
			ReduceFunc: view.Reduce,
		}
		i++
	}

	err = bucket.ViewIndexManager().UpsertDesignDocument(ddocName, mockmr.UpsertDesignDocumentOptions{
		Indexes: views,
	})
	if err != nil {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(err.Error())),
		}
	}

	return &mock.HTTPResponse{
		StatusCode: 201,
		Body:       bytes.NewReader([]byte{}),
	}
}

func (x *viewImplMgmt) handleGetDesignDocument(source mock.ViewService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := pathparse.ParseParts(req.URL.Path, "/*/_design/*")
	bucketName := pathParts[0]
	ddocName := pathParts[1]

	if !source.CheckAuthenticated(mockauth.PermissionViewsManage, bucketName, "", "", req) {
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

	ddoc, err := bucket.ViewIndexManager().GetDesignDocument(ddocName)
	if err != nil {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(err.Error())),
		}
	}

	b, err := json.Marshal(ddocToJsonDesignDocument(ddoc))
	if err != nil {
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

func (x *viewImplMgmt) handleDropDesignDocument(source mock.ViewService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := pathparse.ParseParts(req.URL.Path, "/*/_design/*")
	bucketName := pathParts[0]
	ddocName := pathParts[1]

	if !source.CheckAuthenticated(mockauth.PermissionViewsManage, bucketName, "", "", req) {
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

	err := bucket.ViewIndexManager().DropDesignDocument(ddocName)
	if err != nil {
		return &mock.HTTPResponse{
			StatusCode: 400,
			Body:       bytes.NewReader([]byte(err.Error())),
		}
	}

	return &mock.HTTPResponse{
		StatusCode: 200,
		Body:       bytes.NewReader([]byte{}),
	}
}
