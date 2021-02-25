package svcimpls

import (
	"encoding/json"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/google/uuid"
	"strings"
)

// GenPoolsConfig returns the current config for the default pool.
func GenPoolsConfig(c mock.Cluster) []byte {
	config := make(map[string]interface{})

	uuid := strings.Replace(uuid.New().String(), "-", "", -1)
	config["uuid"] = uuid
	config["isEnterprise"] = true
	config["isAdminCreds"] = true
	config["isROAdminCreds"] = false
	config["allowedServices"] = []string{
		"kv",
		"n1ql",
		"index",
		"fts",
		"cbas",
		"eventing",
		"backup",
	}
	config["isIPv6"] = false
	config["isDeveloperPreview"] = false
	config["packageVariant"] = "centos7"
	config["pools"] = []map[string]string{
		{
			"name":         "default",
			"uri":          "/pools/default?uuid=" + uuid,
			"streamingUri": "/poolsStreaming/default?uuid=" + uuid,
		},
	}
	config["settings"] = map[string]string{
		"maxParallelIndexers": "/settings/maxParallelIndexers?uuid=" + uuid,
		"viewUpdateDaemon":    "/settings/viewUpdateDaemon?uuid=" + uuid,
	}
	config["implementationVersion"] = "7.0.0-3016-enterprise"
	config["componentsVersion"] = map[string]string{
		"ns_server":  "7.0.0-3016-enterprise",
		"inets":      "7.1.3.3",
		"os_mon":     "2.5.1.1",
		"ale":        "0.0.0",
		"crypto":     "4.6.5.1",
		"stdlib":     "3.12.1",
		"public_key": "1.7.2",
		"ssl":        "9.6.2.3",
		"lhttpc":     "1.3.0",
		"asn1":       "5.0.12",
		"sasl":       "3.4.2",
		"kernel":     "6.5.2.1",
	}

	configBytes, _ := json.Marshal(config)
	return configBytes
}
