package bloodlabnet

import (
	"time"
)

type ConnectionInstance interface {
	// Send  data directly from instance. This will not work if the instance
	Send(data [][]byte) (int, error)
	//Receive data directly from instance. This will not work if the instance handles many connections like TCP-Servers
	Receive() ([]byte, error)
	// Run - Main-Loop
	Run(handler Handler)
	// Stop the main-loop of the Run-handler
	Stop()
	// Retrieve a session by IP. Do not use this for a normal protocol conversion of a server... Can return nil
	FindSessionsByIp(ip string) []Session
	// Wait until the server is ready for connections. Useful at startup. Returns true when the server is ready
	WaitReady() bool
}

type Session interface {
	IsAlive() bool
	Send(msg [][]byte) (int, error)
	Receive() ([]byte, error)
	Close() error
	WaitTermination() error
	RemoteAddress() (string, error)
}

type ConnectionAndSessionInstance interface {
	Connect() error
	ConnectionInstance
	Session
}

type Handler interface {
	//DataReceived event is triggered whenever the underlying protocol delivered a complete block(file/transmission) of data
	DataReceived(session Session, data []byte, receiveTimestamp time.Time) error
	// Connected event is triggered when connection is established. For client as well as for servers. 	For clients in addition every time the connection had
	// to be reestablished. If this method returns anything other but null, the connections
	// is declined and the thread ends
	Connected(session Session) error
	// Disconnected event is triggered when connection
	// is terminated.
	//For Servers: when the client ends the session
	// For clients: when the only client connection ends (inc.eof)
	Disconnected(session Session)

	// Error is called from async process (Run) when
	// status messages regarding the connection is available
	Error(session Session, typeOfError ErrorType, err error)
}
