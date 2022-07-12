package netutil

import (
	"net"
	"strconv"
)

// MakeURL makes URL based on params taking into consideration IPv6
func MakeURL(urlPrefix, ip string, port int) string {
	return urlPrefix + net.JoinHostPort(ip, strconv.Itoa(port))
}
