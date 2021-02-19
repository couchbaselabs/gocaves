package testmode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/couchbaselabs/gocaves/cmd/api"
)

// Main wraps the linkmode cmd
type Main struct {
	SdkPort    int
	ListenPort int
	ReportAddr string

	testRuns   testRunManager
	clusterMgr clusterManager
}

// Go starts the app
func (m *Main) Go() {
	if m.SdkPort == 0 {
		srv, err := api.NewServer(api.NewServerOptions{
			ListenPort: m.ListenPort,
			Handler:    m.handleAPIRequest,
		})
		if err != nil {
			log.Printf("failed to start listen server: %s", err)
			return
		}

		log.Printf("CAVES listen server started on: :%d", srv.ListenPort())

		<-make(chan struct{})
		return
	}

	cli, err := api.ConnectAsServer(api.ConnectAsServerOptions{
		Address: fmt.Sprintf("127.0.0.1:%d", m.SdkPort),
		Handler: m.handleAPIRequest,
	})
	if err != nil {
		log.Printf("failed to connect to the sdk: %s", err)
		return
	}

	log.Printf("connected to the sdk")

	cli.WaitForClose()

	log.Printf("sdk disconnected")
}

func (m *Main) maybeSendReport(report *jsonRunReport) {
	if m.ReportAddr == "" {
		return
	}

	reportingURI := fmt.Sprintf("http://%s/submit", m.ReportAddr)
	reportBytes, _ := json.Marshal(report)

	_, err := http.Post(reportingURI, "text/javascript", bytes.NewReader(reportBytes))
	if err != nil {
		log.Printf("failed to send report to `%s`: %s", reportingURI, err)
		return
	}

	log.Printf("successfully sent report to reporting server")
}

func (m *Main) handleAPIRequest(pkt interface{}) interface{} {
	switch pktTyped := pkt.(type) {
	case *api.CmdCreateCluster:
		cluster, err := m.clusterMgr.NewCluster(pktTyped.ClusterID)
		if err != nil {
			log.Printf("failed to create cluster: %s", err)
			return &api.CmdCreatedCluster{}
		}

		return &api.CmdCreatedCluster{
			MgmtAddrs: cluster.Mock.MgmtAddrs(),
			ConnStr:   cluster.Mock.ConnectionString(),
		}
	case *api.CmdStartTesting:
		run, err := m.testRuns.NewRun(pktTyped.RunID, pktTyped.ClientName)
		if run == nil {
			log.Printf("failed to start testing: %s", err)
			return &api.CmdStartedTesting{}
		}

		return &api.CmdStartedTesting{
			ConnStr: run.RunGroup.DefaultCluster().ConnectionString(),
		}

	case *api.CmdEndTesting:
		report, err := m.testRuns.EndRun(pktTyped.RunID)
		if err != nil {
			log.Printf("failed to end testing: %s", err)
			return &api.CmdEndedTesting{}
		}

		m.maybeSendReport(report)
		log.Printf("ended test run; full report:\n%+v", report)

		return &api.CmdEndedTesting{
			Report: report,
		}

	case *api.CmdStartTest:
		spec, err := m.testRuns.StartTest(pktTyped.RunID, pktTyped.TestName)
		if err != nil {
			log.Printf("failed to start test: %s", err)
			return &api.CmdStartedTest{}
		}

		return &api.CmdStartedTest{
			ConnStr:        spec.Cluster.ConnectionString(),
			BucketName:     spec.BucketName,
			ScopeName:      spec.ScopeName,
			CollectionName: spec.CollectionName,
		}
	case *api.CmdEndTest:
		err := m.testRuns.EndCurrentTest(pktTyped.RunID, pktTyped.Result)
		if err != nil {
			log.Printf("failed to end test: %s", err)
			return &api.CmdEndedTest{}
		}

		return &api.CmdEndedTest{}
	case *api.CmdTimeTravel:
		ttAmnt := time.Duration(pktTyped.Amount) * time.Millisecond

		err := m.clusterMgr.TimeTravel(pktTyped.ClusterID, ttAmnt)
		if err != nil {
			log.Printf("failed to time travel cluster: %s", err)
		}

		err = m.testRuns.TimeTravel(pktTyped.RunID, ttAmnt)
		if err != nil {
			log.Printf("failed to time travel tests: %s", err)
		}

		return &api.CmdTimeTravelled{}
	case *api.CmdAddBucket:
		err := m.clusterMgr.AddBucket(pktTyped.ClusterID, pktTyped.BucketName, pktTyped.BucketType, pktTyped.NumReplicas)
		if err != nil {
			log.Printf("failed to add bucket: %s", err)
			return &api.CmdAddedBucket{}
		}

		return &api.CmdAddedBucket{}
	}

	return nil
}
