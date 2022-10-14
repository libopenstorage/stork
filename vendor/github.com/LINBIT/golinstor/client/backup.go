package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/go-querystring/query"

	"github.com/LINBIT/golinstor/devicelayerkind"
)

type Backup struct {
	Id                string          `json:"id"`
	StartTime         string          `json:"start_time,omitempty"`
	StartTimestamp    *TimeStampMs    `json:"start_timestamp,omitempty"`
	FinishedTime      string          `json:"finished_time,omitempty"`
	FinishedTimestamp *TimeStampMs    `json:"finished_timestamp,omitempty"`
	OriginRsc         string          `json:"origin_rsc"`
	OriginNode        string          `json:"origin_node,omitempty"`
	FailMessages      string          `json:"fail_messages,omitempty"`
	Vlms              []BackupVolumes `json:"vlms"`
	Success           bool            `json:"success,omitempty"`
	Shipping          bool            `json:"shipping,omitempty"`
	Restorable        bool            `json:"restorable,omitempty"`
	S3                BackupS3        `json:"s3,omitempty"`
	BasedOnId         string          `json:"based_on_id,omitempty"`
}

type BackupInfo struct {
	Rsc          string               `json:"rsc"`
	Full         string               `json:"full"`
	Latest       string               `json:"latest"`
	Count        int32                `json:"count,omitempty"`
	DlSizeKib    int64                `json:"dl_size_kib"`
	AllocSizeKib int64                `json:"alloc_size_kib"`
	Storpools    []BackupInfoStorPool `json:"storpools"`
}

type BackupInfoRequest struct {
	SrcRscName  string            `json:"src_rsc_name,omitempty"`
	LastBackup  string            `json:"last_backup,omitempty"`
	StorPoolMap map[string]string `json:"stor_pool_map,omitempty"`
	NodeName    string            `json:"node_name,omitempty"`
}

type BackupInfoStorPool struct {
	Name              string             `json:"name"`
	ProviderKind      ProviderKind       `json:"provider_kind,omitempty"`
	TargetName        string             `json:"target_name,omitempty"`
	RemainingSpaceKib int64              `json:"remaining_space_kib,omitempty"`
	Vlms              []BackupInfoVolume `json:"vlms"`
}

type BackupInfoVolume struct {
	Name          string                          `json:"name,omitempty"`
	LayerType     devicelayerkind.DeviceLayerKind `json:"layer_type"`
	DlSizeKib     int64                           `json:"dl_size_kib,omitempty"`
	AllocSizeKib  int64                           `json:"alloc_size_kib"`
	UsableSizeKib int64                           `json:"usable_size_kib,omitempty"`
}

type BackupList struct {
	// Linstor is a map of all entries found that could be parsed as LINSTOR backups.
	Linstor map[string]Backup `json:"linstor,omitempty"`
	// Other are files that could not be parsed as LINSTOR backups.
	Other BackupOther `json:"other,omitempty"`
}

type BackupOther struct {
	Files *[]string `json:"files,omitempty"`
}

type BackupRestoreRequest struct {
	SrcRscName    string            `json:"src_rsc_name,omitempty"`
	LastBackup    string            `json:"last_backup,omitempty"`
	StorPoolMap   map[string]string `json:"stor_pool_map,omitempty"`
	TargetRscName string            `json:"target_rsc_name"`
	Passphrase    string            `json:"passphrase,omitempty"`
	NodeName      string            `json:"node_name"`
	DownloadOnly  bool              `json:"download_only,omitempty"`
}

type BackupS3 struct {
	MetaName string `json:"meta_name,omitempty"`
}

type BackupAbortRequest struct {
	RscName string `json:"rsc_name"`
	Restore *bool  `json:"restore,omitempty"`
	Create  *bool  `json:"create,omitempty"`
}

type BackupCreate struct {
	RscName     string `json:"rsc_name"`
	NodeName    string `json:"node_name,omitempty"`
	Incremental bool   `json:"incremental,omitempty"`
}

type BackupShipRequest struct {
	SrcNodeName    string            `json:"src_node_name,omitempty"`
	SrcRscName     string            `json:"src_rsc_name"`
	DstRscName     string            `json:"dst_rsc_name"`
	DstNodeName    string            `json:"dst_node_name,omitempty"`
	DstNetIfName   string            `json:"dst_net_if_name,omitempty"`
	DstStorPool    string            `json:"dst_stor_pool,omitempty"`
	StorPoolRename map[string]string `json:"stor_pool_rename,omitempty"`
	DownloadOnly   *bool             `json:"download_only,omitempty"`
}

type BackupVolumes struct {
	VlmNr             int64            `json:"vlm_nr"`
	FinishedTime      *string          `json:"finished_time,omitempty"`
	FinishedTimestamp *TimeStampMs     `json:"finished_timestamp,omitempty"`
	S3                *BackupVolumesS3 `json:"s3,omitempty"`
}

type BackupVolumesS3 struct {
	Key *string `json:"key,omitempty"`
}

type BackupDeleteOpts struct {
	ID              string       `url:"id,omitempty"`
	IDPrefix        string       `url:"id_prefix,omitempty"`
	Cascading       bool         `url:"cascading,omitempty"`
	Timestamp       *TimeStampMs `url:"timestamp,omitempty"`
	ResourceName    string       `url:"resource_name,omitempty"`
	NodeName        string       `url:"node_name,omitempty"`
	AllLocalCluster bool         `url:"all_local_cluster,omitempty"`
	All             bool         `url:"all,omitempty"`
	S3Key           string       `url:"s3key,omitempty"`
	S3KeyForce      string       `url:"s3key_force,omitempty"`
	DryRun          bool         `url:"dryrun,omitempty"`
}

type BackupProvider interface {
	// GetAll fetches information on all backups stored at the given remote. Optionally limited to the given
	// resource names.
	GetAll(ctx context.Context, remoteName string, rscNames ...string) (BackupList, error)
	// DeleteAll backups that fit the given criteria.
	DeleteAll(ctx context.Context, remoteName string, filter BackupDeleteOpts) error
	// Create a new backup operation.
	Create(ctx context.Context, remoteName string, request BackupCreate) (string, error)
	// Info retrieves information about a specific backup instance.
	Info(ctx context.Context, remoteName string, request BackupInfoRequest) (BackupInfo, error)
	// Abort all running backup operations of a resource.
	Abort(ctx context.Context, remoteName string, request BackupAbortRequest) error
	// Ship ships a backup from one LINSTOR cluster to another.
	Ship(ctx context.Context, remoteName string, request BackupShipRequest) error
	// Restore starts to restore a resource from a backup.
	Restore(ctx context.Context, remoteName string, request BackupRestoreRequest) error
}

var _ BackupProvider = &BackupService{}

type BackupService struct {
	client *Client
}

func (b *BackupService) GetAll(ctx context.Context, remoteName string, rscNames ...string) (BackupList, error) {
	vals, err := query.Values(struct {
		ResourceName []string `url:"rsc_name"`
	}{ResourceName: rscNames})
	if err != nil {
		return BackupList{}, fmt.Errorf("failed to encode resource names: %w", err)
	}

	var list BackupList
	_, err = b.client.doGET(ctx, "/v1/remotes/"+remoteName+"/backups?"+vals.Encode(), &list)
	return list, err
}

func (b *BackupService) DeleteAll(ctx context.Context, remoteName string, filter BackupDeleteOpts) error {
	vals, err := query.Values(filter)
	if err != nil {
		return fmt.Errorf("failed to encode filter options: %w", err)
	}

	_, err = b.client.doDELETE(ctx, "/v1/remotes/"+remoteName+"/backups?"+vals.Encode(), nil)
	return err
}

func (b *BackupService) Create(ctx context.Context, remoteName string, request BackupCreate) (string, error) {
	req, err := b.client.newRequest(http.MethodPost, "/v1/remotes/"+remoteName+"/backups", request)
	if err != nil {
		return "", err
	}

	var resp []ApiCallRc
	_, err = b.client.do(ctx, req, &resp)
	if err != nil {
		return "", err
	}

	for _, rc := range resp {
		if s, ok := rc.ObjRefs["Snapshot"]; ok {
			return s, nil
		}
	}

	return "", errors.New("missing snapshot reference")
}

func (b *BackupService) Info(ctx context.Context, remoteName string, request BackupInfoRequest) (BackupInfo, error) {
	req, err := b.client.newRequest(http.MethodPost, "/v1/remotes/"+remoteName+"/backups/info", request)
	if err != nil {
		return BackupInfo{}, err
	}

	var resp BackupInfo
	_, err = b.client.do(ctx, req, &resp)
	if err != nil {
		return BackupInfo{}, err
	}

	return resp, nil
}

func (b *BackupService) Abort(ctx context.Context, remoteName string, request BackupAbortRequest) error {
	_, err := b.client.doPOST(ctx, "/v1/remotes/"+remoteName+"/backups/abort", request)
	return err
}

func (b *BackupService) Ship(ctx context.Context, remoteName string, request BackupShipRequest) error {
	_, err := b.client.doPOST(ctx, "/v1/remotes/"+remoteName+"/backups/ship", request)
	return err
}

func (b *BackupService) Restore(ctx context.Context, remoteName string, request BackupRestoreRequest) error {
	_, err := b.client.doPOST(ctx, "/v1/remotes/"+remoteName+"/backups/restore", request)
	return err
}
