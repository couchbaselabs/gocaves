package store

import (
	"testing"
	"time"

	"github.com/couchbaselabs/gocaves/mocktime"
)

func TestBasic(t *testing.T) {
	chrono := &mocktime.Chrono{}
	bucket, err := NewBucket(BucketConfig{
		Chrono:         chrono,
		NumReplicas:    2,
		NumVbuckets:    4,
		ReplicaLatency: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	insDoc, err := bucket.Insert(&Document{
		VbID:      3,
		Key:       []byte("test"),
		Value:     []byte("hello world"),
		Xattrs:    nil,
		IsDeleted: false,
	})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}
	if insDoc.Cas == 0 {
		t.Fatalf("cas was not assigned correctly")
	}

	getDoc, err := bucket.Get(0, 3, []byte("test"))
	if err != nil {
		t.Fatalf("failed to get document: %v", err)
	}
	if getDoc.Cas != insDoc.Cas {
		t.Fatalf("get cas was not retreived correctly")
	}
}

func TestReplication(t *testing.T) {
	chrono := &mocktime.Chrono{}
	bucket, err := NewBucket(BucketConfig{
		Chrono:         chrono,
		NumReplicas:    2,
		NumVbuckets:    4,
		ReplicaLatency: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	insDoc, err := bucket.Insert(&Document{
		VbID:      3,
		Key:       []byte("test"),
		Value:     []byte("hello world"),
		Xattrs:    nil,
		IsDeleted: false,
	})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}
	if insDoc.Cas == 0 {
		t.Fatalf("cas was not assigned correctly")
	}

	repDoc1, err := bucket.Get(1, 3, []byte("test"))
	if err == nil {
		t.Fatalf("first replica fetch should have failed")
	}
	if repDoc1 != nil {
		t.Fatalf("first replica fetch should have a nil document")
	}

	chrono.TimeTravel(100 * time.Millisecond)

	repDoc2, err := bucket.Get(1, 3, []byte("test"))
	if err != nil {
		t.Fatalf("second replica fetch should have worked")
	}
	if repDoc2.Cas != insDoc.Cas {
		t.Fatalf("second replica cas was not retreived correctly")
	}
}
