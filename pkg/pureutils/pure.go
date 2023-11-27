package pureutils

import (
	"fmt"
	"github.com/devans10/pugo/flasharray"
	"strings"

	"github.com/portworx/torpedo/pkg/units"
)

// GetAppDataDir checks the pod namespace prefix, and returns a path that we can
// write data to on a volume. Because the mountpath varies so heavily between applications
// (some have multiple PVCs, some have configmap vols, etc. etc.) this is the easiest way
// at the moment. As more apps get added to our test suite, we should add them here.
func GetAppDataDir(namespace string) (string, int) {
	if strings.HasPrefix(namespace, "nginx-without-enc") {
		return "/usr/share/nginx/html", units.GiB / 2
	}
	if strings.HasPrefix(namespace, "wordpress") {
		return "/var/www/html", units.GiB / 2
	}
	if strings.HasPrefix(namespace, "elasticsearch") {
		return "/usr/share/elasticsearch/data", units.GiB * 2
	}
	if strings.HasPrefix(namespace, "mysql-without-enc") {
		return "/var/lib/mysql", units.GiB
	}
	if strings.HasPrefix(namespace, "nginx-fa-davol") {
		return "/data", units.GiB
	}
	if strings.HasPrefix(namespace, "nginx-fa-darawvol") {
		return "/dev/xvda", units.GiB
	}
	return "", 0
}

// GetFAClientMapFromPXPureSecret takes a PXPureSecret and returns a map of mgmt endpoints to FA clients
func GetFAClientMapFromPXPureSecret(secret PXPureSecret) (map[string]*flasharray.Client, error) {
	clientMap := make(map[string]*flasharray.Client)
	for _, fa := range secret.Arrays {
		faClient, err := flasharray.NewClient(fa.MgmtEndPoint, "", "", fa.APIToken, "", false, false, "", nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create FA client for [%s]. Err: [%v]", fa.MgmtEndPoint, err)
		}
		clientMap[fa.MgmtEndPoint] = faClient
	}
	return clientMap, nil
}

// GetFBClientMapFromPXPureSecret takes a PXPureSecret and returns a map of mgmt endpoints to FB clients
func GetFBClientMapFromPXPureSecret(secret PXPureSecret) (map[string]*flasharray.Client, error) {
	clientMap := make(map[string]*flasharray.Client)
	for _, fb := range secret.Blades {
		fbClient, err := flasharray.NewClient(fb.MgmtEndPoint, "", "", fb.APIToken, "", false, false, "", nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create FB client for [%s]. Err: [%v]", fb.MgmtEndPoint, err)
		}
		clientMap[fb.MgmtEndPoint] = fbClient
	}
	return clientMap, nil
}
