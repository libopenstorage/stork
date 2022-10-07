package pureutils

import (
	"github.com/portworx/torpedo/pkg/units"
	"strings"
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
	return "", 0
}
