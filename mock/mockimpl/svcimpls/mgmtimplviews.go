package svcimpls

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/gocaves/contrib/pathparse"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"github.com/couchbaselabs/gocaves/mock/mockmr"
)

func (x *mgmtImpl) handleGetAllDesignDocuments(source mock.MgmtService, req *mock.HTTPRequest) *mock.HTTPResponse {
	pathParts := pathparse.ParseParts(req.URL.Path, "/pools/default/buckets/*/ddocs")
	bucketName := pathParts[0]

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

	ddocs := bucket.ViewIndexManager().GetAllDesignDocuments()
	var jsonsRows []jsonGetAllDesignDocsRow
	for _, ddoc := range ddocs {
		doc := jsonGetAllDesignDocsDoc{
			Meta: jsonGetAllDesignDocsMeta{
				ID:  "_design/" + ddoc.Name,
				Rev: "", // TODO (chvck): design docs need rev ids
			},
			JSON: ddocToJsonDesignDocument(ddoc),
		}
		jsonsRows = append(jsonsRows, jsonGetAllDesignDocsRow{
			Doc: doc,
			Controllers: jsonGetAllDesignDocsControllers{
				Compact:             fmt.Sprintf("/pools/default/buckets/%s/ddocs/_design/%s/controller/compactView", bucketName, ddoc.Name),
				SetUpdateMinChanges: fmt.Sprintf("/pools/default/buckets/%s/ddocs/_design/%s/controller/setUpdateMinChanges", bucketName, ddoc.Name),
			},
		})
	}

	b, err := json.Marshal(jsonGetAllDesignDocs{
		Rows: jsonsRows,
	})
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

type jsonGetAllDesignDocsMeta struct {
	ID  string `json:"id"`
	Rev string `json:"rev"`
}

type jsonGetAllDesignDocsDoc struct {
	Meta jsonGetAllDesignDocsMeta `json:"meta"`
	JSON jsonDesignDocument       `json:"json"`
}

type jsonGetAllDesignDocsControllers struct {
	Compact             string `json:"compact"`
	SetUpdateMinChanges string `json:"setUpdateMinChanges"`
}

type jsonGetAllDesignDocsRow struct {
	Doc         jsonGetAllDesignDocsDoc         `json:"doc"`
	Controllers jsonGetAllDesignDocsControllers `json:"controllers"`
}

type jsonGetAllDesignDocs struct {
	Rows []jsonGetAllDesignDocsRow `json:"rows"`
}
