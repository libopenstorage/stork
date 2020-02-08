// +build unittest

package storkctl

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/resource"
)

var unstructuredSerializer = resource.UnstructuredPlusDefaultContentConfig().NegotiatedSerializer

func defaultHeader() http.Header {
	header := http.Header{}
	header.Set("Content-Type", runtime.ContentTypeJSON)
	return header
}

func objBody(codec runtime.Codec, obj runtime.Object) io.ReadCloser {
	return ioutil.NopCloser(bytes.NewReader([]byte(runtime.EncodeOrDie(codec, obj))))
}
