package mock

import (
	"fmt"
	"net"
	"sync"

	"github.com/libopenstorage/openstorage/api"
	pxapi "github.com/libopenstorage/operator/api/px"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// SdkServers consists of different mock servers that the mock
// sdk server can implement
type SdkServers struct {
	Cluster         *MockOpenStorageClusterServer
	Node            *MockOpenStorageNodeServer
	Role            *MockOpenStorageRoleServer
	Volume          *MockOpenStorageVolumeServer
	PortworxService *MockPortworxServiceServer
}

// SdkServer can be used to create a sdk server which implements mock server
// objects given by the user
type SdkServer struct {
	server   *grpc.Server
	servers  SdkServers
	wg       sync.WaitGroup
	listener net.Listener
	running  bool
}

// NewSdkServer creates a new instance of mock sdk server
func NewSdkServer(servers SdkServers) *SdkServer {
	return &SdkServer{
		servers: servers,
	}
}

// StartOnAddress starts a new gRPC server listening on given address.
func (m *SdkServer) StartOnAddress(ip, port string) error {
	var err error
	m.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%s", ip, port))
	if err != nil {
		return err
	}

	m.server = grpc.NewServer()

	if m.servers.Cluster != nil {
		api.RegisterOpenStorageClusterServer(m.server, m.servers.Cluster)
	}
	if m.servers.Node != nil {
		api.RegisterOpenStorageNodeServer(m.server, m.servers.Node)
	}
	if m.servers.Role != nil {
		api.RegisterOpenStorageRoleServer(m.server, m.servers.Role)
	}
	if m.servers.Volume != nil {
		api.RegisterOpenStorageVolumeServer(m.server, m.servers.Volume)
	}
	if m.servers.PortworxService != nil {
		pxapi.RegisterPortworxServiceServer(m.server, m.servers.PortworxService)
	}

	reflection.Register(m.server)
	waitForServer := make(chan bool)
	m.goServe(waitForServer)
	<-waitForServer
	m.running = true
	return nil
}

func (m *SdkServer) goServe(started chan<- bool) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		started <- true
		err := m.server.Serve(m.listener)
		if err != nil {
			logrus.Fatalf("ERROR: Unable to start test gRPC server: %s\n", err.Error())
		}
	}()
}

// Stop stops the grpc server
func (m *SdkServer) Stop() {
	if !m.running {
		return
	}
	m.server.Stop()
	m.wg.Wait()
	m.running = false
}
