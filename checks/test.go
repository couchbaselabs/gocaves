package checks

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

var errFailNow = errors.New("fail now was called")

type RecordedPacket struct {
	ID                     uint64
	WasSent                bool
	SrcAddr                string
	SrcName                string
	DestAddr               string
	DestName               string
	IsTLS                  bool
	SelectedBucketName     string
	ResolvedScopeName      string
	ResolvedCollectionName string
	Data                   memd.Packet
}

// T represents a singular check test that is being run.
type T struct {
	parent *TestRunner
	ptest  *pendingTest
	def    *Check

	hasRun         bool
	cluster        mock.Cluster
	bucketName     string
	scopeName      string
	collectionName string

	kvInHooks  mock.KvHookManager
	kvOutHooks mock.KvHookManager

	hasFinishConfiguring bool
	requiredFeatures     []TestFeature

	wasFailure bool
	wasSuccess bool
	startCh    chan *TestStartedSpec
	cancelCh   chan struct{}
	endedCh    chan struct{}

	lock    sync.Mutex
	logMsgs []string
	packets []*RecordedPacket
}

func (t *T) testKvInHooks() mock.KvHookManager {
	return t.kvInHooks
}

func (t *T) testKvOutHooks() mock.KvHookManager {
	return t.kvOutHooks
}

// RequireFeature marks a feature as required by this check.
func (t *T) RequireFeature(feature TestFeature) {
	if t.hasFinishConfiguring {
		t.Fatalf("Cannot add features after accessing the mock")
	}

	t.requiredFeatures = append(t.requiredFeatures, feature)
}

// Start will start a test and wait till it is configured.
func (t *T) Start() (*TestStartedSpec, error) {
	if t.hasRun {
		return nil, errors.New("cannot run the same test twice")
	}

	t.hasRun = true
	t.startCh = make(chan *TestStartedSpec)
	t.cancelCh = make(chan struct{})
	t.endedCh = make(chan struct{})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("TEST FUNCTION PANICED!: %+v", r)

				if r == errFailNow {
					close(t.endedCh)
					return
				}

				// TODO(brett19): Record the error that occured...
				close(t.endedCh)
			}
		}()

		t.def.Fn(t)

		close(t.endedCh)
		t.wasSuccess = !t.wasFailure
	}()

	var startedSpec *TestStartedSpec
	var startedErr error
	select {
	case spec := <-t.startCh:
		startedSpec = spec
	case <-t.endedCh:
		startedErr = errors.New("test failed during startup")
	}

	close(t.startCh)

	return startedSpec, startedErr
}

func (t *T) markConfigured() {
	if t.hasFinishConfiguring {
		return
	}
	t.hasFinishConfiguring = true

	// TODO(brett19): Implement the rules for what requires a custom cluster
	// Specifically we need to inspect the t.requiredFeatures and decide whether
	// those features require a custom cluster.  I think tests which specifically
	// require caves should be the marker, and I believe that access to t.Mock()
	// should be guarded by that feature.

	t.cluster = t.parent.defaultCluster
	t.bucketName = "default"
	t.scopeName = "_default"
	t.collectionName = "_default"

	t.kvInHooks = t.cluster.KvInHooks().Child()
	t.kvOutHooks = t.cluster.KvOutHooks().Child()

	resolveLocalNodeName := func(source mock.KvClient) string {
		nodeID := source.Source().Node().ID()
		return fmt.Sprintf("node[%s]", nodeID[0:8])
	}

	resolveBucketScopeColl := func(source mock.KvClient, pak *memd.Packet) (string, string, string) {
		bkt := source.SelectedBucket()
		if bkt == nil {
			return "", "", ""
		}

		scp, coll := bkt.CollectionManifest().GetByID(pak.CollectionID)
		return bkt.Name(), scp, coll
	}

	t.kvInHooks.Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		bkt, scp, coll := resolveBucketScopeColl(source, pak)

		t.recordPacket(&RecordedPacket{
			WasSent:                false,
			SrcAddr:                source.RemoteAddr().String(),
			DestAddr:               source.LocalAddr().String(),
			DestName:               resolveLocalNodeName(source),
			IsTLS:                  source.IsTLS(),
			SelectedBucketName:     bkt,
			ResolvedScopeName:      scp,
			ResolvedCollectionName: coll,
			Data:                   *pak,
		})
		next()
	})

	t.kvOutHooks.Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		bkt, scp, coll := resolveBucketScopeColl(source, pak)

		t.recordPacket(&RecordedPacket{
			WasSent:                true,
			SrcAddr:                source.LocalAddr().String(),
			SrcName:                resolveLocalNodeName(source),
			DestAddr:               source.RemoteAddr().String(),
			IsTLS:                  source.IsTLS(),
			SelectedBucketName:     bkt,
			ResolvedScopeName:      scp,
			ResolvedCollectionName: coll,
			Data:                   *pak,
		})
		next()
	})

	t.startCh <- &TestStartedSpec{
		Cluster:        t.cluster,
		BucketName:     t.bucketName,
		ScopeName:      t.scopeName,
		CollectionName: t.collectionName,
	}
}

// End ends this test as quickly as possible.
func (t *T) End(result interface{}) {
	// Close the cancel channel to force everything to cancel
	close(t.cancelCh)

	// Wait for the test to complete, or a maximum.
	select {
	case <-t.endedCh:
	case <-time.After(1 * time.Second):
		log.Printf("Test did not end within a second of cancelling")
	}

	t.kvInHooks.Destroy()
	t.kvOutHooks.Destroy()
}

// Mock returns the mock associated with this check.
func (t *T) Mock() mock.Cluster {
	t.markConfigured()

	return nil
}

// Cluster returns a reference to the cluster associated with this check.
func (t *T) Cluster() ClusterRef {
	t.markConfigured()

	return ClusterRef{
		t: t,
	}
}

// Bucket returns a reference to the bucket associated with this check.
func (t *T) Bucket() BucketRef {
	t.markConfigured()

	return BucketRef{
		Cluster:    t.Cluster(),
		BucketName: t.bucketName,
	}
}

// Scope returns a reference to the scope associated with this check.
func (t *T) Scope() ScopeRef {
	t.markConfigured()

	return ScopeRef{
		Bucket:    t.Bucket(),
		ScopeName: t.scopeName,
	}
}

// Collection returns a reference to the collection associated with this check.
func (t *T) Collection() CollectionRef {
	t.markConfigured()

	return CollectionRef{
		Scope:          t.Scope(),
		CollectionName: t.collectionName,
	}
}

// Fail marks this check as having failed.
func (t *T) Fail() {
	t.wasFailure = true
}

// FailNow mark this check as having failed and immediately bails out of the check.
func (t *T) FailNow() {
	t.wasFailure = true
	panic(errFailNow)
}

// Logf writes a log message as part of this check.
func (t *T) Logf(format string, args ...interface{}) {
	t.recordLog(fmt.Sprintf(format, args...))
	log.Printf("TEST LOGGED: "+format, args...)
}

// Errorf writes a log message as part of this check and then calls Fail.
func (t *T) Errorf(format string, args ...interface{}) {
	t.Logf(format, args...)
	t.Fail()
}

// Fatalf writes a log message as part of this check and then calls FailNow.
func (t *T) Fatalf(format string, args ...interface{}) {
	t.Logf(format, args...)
	t.FailNow()
}

func (t *T) recordLog(msg string) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.logMsgs = append(t.logMsgs, msg)
}

func (t *T) recordPacket(pak *RecordedPacket) {
	t.lock.Lock()
	defer t.lock.Unlock()

	pak.ID = uint64(len(t.packets)) + 1
	t.packets = append(t.packets, pak)
}
