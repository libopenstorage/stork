package testrail

import (
	"strconv"
	"strings"
)

func applySpecificFilter(uri, request string, filters []int) string {
	uri = uri + "&" + request + "="
	for i := 0; i < len(filters); i++ {
		uri = uri + strconv.Itoa(filters[i]) + ","
	}
	uri = trimSuffix(uri, ",")

	return uri
}

func trimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}

func btoitos(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
