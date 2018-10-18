package storkctl

import (
	"fmt"
	"net/http"
	"testing"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/portworx/sched-ops/k8s"

	"k8s.io/client-go/rest/fake"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

func TestStork(t *testing.T) {
	fmt.Println("Entered test")
}

func TestStorkGetVolumeSnapshots(t *testing.T) {
	var snapshots snapv1.VolumeSnapshotList

	f := NewTestFactory()
	tf := f.TestFactory.WithNamespace("test")
	defer tf.Cleanup()
	codec := legacyscheme.Codecs.LegacyCodec(schema.GroupVersion{Version: "v1"})

	fakeRestClient := &fake.RESTClient{
		NegotiatedSerializer: unstructuredSerializer,
		Resp:                 &http.Response{StatusCode: 200, Header: defaultHeader(), Body: objBody(codec, &snapshots)},
	}
	tf.Client = fakeRestClient
	fakeKubeClient, _ := tf.KubernetesClientSet()

	k8s.Instance().SetClient(fakeKubeClient, fakeRestClient, nil, nil, nil)

	streams, _, buf, _ := genericclioptions.NewTestIOStreams()
	cmd := newGetCommand(f, streams)
	cmd.SetOutput(buf)
	cmd.SetArgs([]string{"volumesnapshots"})
	cmd.Execute()
	fmt.Println(buf)

	expected := `NAME      READY     STATUS    RESTARTS   AGE         LABELS
foo       0/0                 0          <unknown>   <none>
`
	if e, a := expected, buf.String(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
}
