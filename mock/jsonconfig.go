package mock

// A Node is a computer in a cluster running the couchbase software.
type jsonCfgNode struct {
	ClusterCompatibility int                `json:"clusterCompatibility"`
	ClusterMembership    string             `json:"clusterMembership"`
	CouchAPIBase         string             `json:"couchApiBase"`
	Hostname             string             `json:"hostname"`
	InterestingStats     map[string]float64 `json:"interestingStats,omitempty"`
	MCDMemoryAllocated   float64            `json:"mcdMemoryAllocated"`
	MCDMemoryReserved    float64            `json:"mcdMemoryReserved"`
	MemoryFree           float64            `json:"memoryFree"`
	MemoryTotal          float64            `json:"memoryTotal"`
	OS                   string             `json:"os"`
	Ports                map[string]int     `json:"ports"`
	Status               string             `json:"status"`
	Uptime               int                `json:"uptime,string"`
	Version              string             `json:"version"`
	ThisNode             bool               `json:"thisNode,omitempty"`
}

type jsonCfgNodeServices struct {
	Kv      uint16 `json:"kv"`
	Capi    uint16 `json:"capi"`
	Mgmt    uint16 `json:"mgmt"`
	N1ql    uint16 `json:"n1ql"`
	Fts     uint16 `json:"fts"`
	Cbas    uint16 `json:"cbas"`
	KvSsl   uint16 `json:"kvSSL"`
	CapiSsl uint16 `json:"capiSSL"`
	MgmtSsl uint16 `json:"mgmtSSL"`
	N1qlSsl uint16 `json:"n1qlSSL"`
	FtsSsl  uint16 `json:"ftsSSL"`
	CbasSsl uint16 `json:"cbasSSL"`
}

type jsonCfgNodeAltAddress struct {
	Ports    *jsonCfgNodeServices `json:"ports,omitempty"`
	Hostname string               `json:"hostname"`
}

type jsonCfgNodeExt struct {
	Services     jsonCfgNodeServices              `json:"services"`
	Hostname     string                           `json:"hostname"`
	AltAddresses map[string]jsonCfgNodeAltAddress `json:"alternateAddresses"`
}

// VBucketServerMap is the a mapping of vbuckets to nodes.
type jsonCfgVBucketServerMap struct {
	HashAlgorithm string   `json:"hashAlgorithm"`
	NumReplicas   int      `json:"numReplicas"`
	ServerList    []string `json:"serverList"`
	VBucketMap    [][]int  `json:"vBucketMap"`
}

// Bucket is the primary entry point for most data operations.
type jsonCfgBucket struct {
	Rev                 int64 `json:"rev"`
	SourceHostname      string
	Capabilities        []string `json:"bucketCapabilities"`
	CapabilitiesVersion string   `json:"bucketCapabilitiesVer"`
	Name                string   `json:"name"`
	NodeLocator         string   `json:"nodeLocator"`
	URI                 string   `json:"uri"`
	StreamingURI        string   `json:"streamingUri"`
	UUID                string   `json:"uuid"`
	DDocs               struct {
		URI string `json:"uri"`
	} `json:"ddocs,omitempty"`

	// These are used for JSON IO, but isn't used for processing
	// since it needs to be swapped out safely.
	VBucketServerMap       jsonCfgVBucketServerMap `json:"vBucketServerMap"`
	Nodes                  []jsonCfgNode           `json:"nodes"`
	NodesExt               []jsonCfgNodeExt        `json:"nodesExt,omitempty"`
	ClusterCapabilitiesVer []int                   `json:"clusterCapabilitiesVer,omitempty"`
	ClusterCapabilities    map[string][]string     `json:"clusterCapabilities,omitempty"`
}
