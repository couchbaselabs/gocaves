package cmd

import (
	"flag"
	"log"
	"os"

	"github.com/couchbaselabs/gocaves/checksuite"
	"github.com/couchbaselabs/gocaves/cmd/linkmode"
	"github.com/couchbaselabs/gocaves/cmd/mockmode"
	"github.com/couchbaselabs/gocaves/cmd/testmode"
)

var controlPortFlag = flag.Int("control-port", 0, "specifies we are running in a test-framework")
var linkAddrFlag = flag.String("link-addr", "", "specifies a caves dev server to connect to")
var reportingAddrFlag = flag.String("reporting-addr", "", "specifies a caves reporting server to use")
var mockOnlyFlag = flag.Bool("mock-only", false, "specifies only to use the mock")
var listenPortFlag = flag.Int("listen-port", 0, "specifies a port for the listen server")

func parseReportingAddr() string {
	if reportingAddrFlag == nil {
		return ""
	}
	return *reportingAddrFlag
}

// Start will start the CAVES system.
func Start() {
	flag.Parse()

	log.SetPrefix("GOCAVES ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetOutput(os.Stderr)

	checksuite.RegisterCheckFuncs()

	if mockOnlyFlag != nil && *mockOnlyFlag {
		// Mock-only mode
		(&mockmode.Main{}).Go()
	} else if linkAddrFlag != nil && *linkAddrFlag != "" {
		// Test-suite inside an SDK linked to a dev mod instance
		if controlPortFlag == nil || *controlPortFlag <= 0 {
			log.Printf("control port must be specified with link-addr")
			return
		}

		(&linkmode.Main{
			SdkPort:  *controlPortFlag,
			LinkAddr: *linkAddrFlag,
		}).Go()
	} else if controlPortFlag != nil && *controlPortFlag > 0 {
		// Standard test-suite mode
		(&testmode.Main{
			SdkPort:    *controlPortFlag,
			ReportAddr: parseReportingAddr(),
		}).Go()
	} else if listenPortFlag != nil && *listenPortFlag > 0 {
		// Development mode
		(&testmode.Main{
			ListenPort: *listenPortFlag,
			ReportAddr: parseReportingAddr(),
		}).Go()
	} else {
		log.Printf(`You must specify an option to start CAVES.  If you intended to start the reporting server, please see the README for more details.`)
		return
	}
}
