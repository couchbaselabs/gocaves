package mockmr

import (
	"fmt"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
	"testing"
)

func TestExecute(t *testing.T) {
	e := NewEngine()

	err := e.UpsertDesignDocument("ddoc", UpsertDesignDocumentOptions{
		Indexes: []*Index{
			{
				Name: "test",
				MapFunc: `
				function (doc, meta) {
					emit(doc.key, meta);
				}`,
				ReduceFunc: "_count",
			},
		},
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	_, res, err := e.Execute(ExecuteOptions{
		Data: []*mockdb.Document{
			{
				Key:   []byte("test"),
				Value: []byte(`{"key":"me","test1":22,"test2":"23"}`),
			},
			{
				Key:   []byte("test2"),
				Value: []byte(`{"key":"me","test1":22,"test2":"23"}`),
			},
			{
				Key:   []byte("test3"),
				Value: []byte(`{"key":22,"test1":22,"test2":"23"}`),
			},
		},
		DesignDoc: "ddoc",
		View:      "test",
		// Reduce:    true,
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	fmt.Printf("%#v", res)
}
