package connectionstatus

type ConnectionStatus int

const (
	Offline             ConnectionStatus = 0
	Connected           ConnectionStatus = 1
	Online              ConnectionStatus = 2
	VersionMismatch     ConnectionStatus = 3
	FullSyncFailed      ConnectionStatus = 4
	AuthenticationError ConnectionStatus = 5
	Unknown             ConnectionStatus = 6
	HostnameMismatch    ConnectionStatus = 7
	OtherController     ConnectionStatus = 8
	Authenticated       ConnectionStatus = 9
	NoStltConn          ConnectionStatus = 10
)
