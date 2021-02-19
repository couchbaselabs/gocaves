package mockauth

// Permission represents a permission a user may need for an operation.
type Permission uint8

// Various permissions which can be required.
const (
	PermissionDataRead Permission = iota + 1
	PermissionDataWrite
	PermissionUserRead
	PermissionUserManage
	PermissionViewsRead
	PermissionViewsManage
	PermissionDCPRead
	PermissionSearchRead
	PermissionSearchManage
	PermissionQueryRead
	PermissionQueryWrite
	PermissionQueryDelete
	PermissionQueryManage
	PermissionAnalyticsRead
	PermissionsAnalyticsManage
	PermissionSyncGateway
	PermissionStatsRead
	PermissionReplicationTarget
	PermissionReplicationManage
	PermissionClusterRead
	PermissionClusterManage
	PermissionBucketManage
	PermissionSettings
	PermissionSelect
)
