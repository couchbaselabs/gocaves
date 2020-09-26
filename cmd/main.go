package cmd

import (
	"flag"
	"log"
	"os"

	"github.com/couchbaselabs/gocaves/checksuite"
	"github.com/couchbaselabs/gocaves/cmd/devmode"
	"github.com/couchbaselabs/gocaves/cmd/linkmode"
	"github.com/couchbaselabs/gocaves/cmd/mockmode"
	"github.com/couchbaselabs/gocaves/cmd/testmode"
)

var controlPortFlag = flag.Int("control-port", 0, "specifies we are running in a test-framework")
var linkAddrFlag = flag.String("link-addr", "", "specifies a caves dev server to connect to")
var reportingAddrFlag = flag.String("reporting-addr", "", "specifies a caves reporting server to use")
var mockOnlyFlag = flag.Bool("mock-only", false, "specifies only to use the mock")

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
		reportAddr := ""
		if reportingAddrFlag != nil {
			reportAddr = *reportingAddrFlag
		}

		(&testmode.Main{
			SdkPort:    *controlPortFlag,
			ReportAddr: reportAddr,
		}).Go()
	} else {
		// Development mode
		(&devmode.Main{}).Go()
	}
}
