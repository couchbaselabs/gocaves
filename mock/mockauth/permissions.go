package mockauth

type Permission uint8

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
