package svcimpls

type mgmtImpl struct {
}

func (x *mgmtImpl) Register(h *hookHelper) {
	h.RegisterMgmtHandler("GET", "/", x.handlePing)
	h.RegisterMgmtHandler("GET", "/pools/default", x.handleGetPoolConfig)
	h.RegisterMgmtHandler("GET", "/pools/default/buckets/*", x.handleGetBucketConfig)
	h.RegisterMgmtHandler("POST", "/pools/default/buckets/*/scopes", x.handleCreateScope)
	h.RegisterMgmtHandler("POST", "/pools/default/buckets/*/scopes/*/collections", x.handleCreateCollection)
	h.RegisterMgmtHandler("DELETE", "/pools/default/buckets/*/scopes/*", x.handleDropScope)
	h.RegisterMgmtHandler("DELETE", "/pools/default/buckets/*/scopes/*/collections/*", x.handleDropCollection)
	h.RegisterMgmtHandler("GET", "/pools/default/buckets/*/scopes", x.handleGetAllScopes)
	h.RegisterMgmtHandler("PUT", "/settings/rbac/users/*/*", x.handleUpsertUser)
	h.RegisterMgmtHandler("GET", "/settings/rbac/users/*", x.handleGetAllUsers)
	h.RegisterMgmtHandler("GET", "/settings/rbac/users/*/*", x.handleGetUser)
	h.RegisterMgmtHandler("DELETE", "/settings/rbac/users/*/*", x.handleDropUser)
}