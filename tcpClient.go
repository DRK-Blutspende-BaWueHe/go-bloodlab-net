package bloodlabnet

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/DRK-Blutspende-BaWueHe/go-bloodlab-net/protocol"
)

/*
One instance of client can only connect one

	session to a server, thus Instance = Session
	implements both interfaces
*/
type tcpClientConnectionAndSession struct {
	hostname         string
	port             int
	lowLevelProtocol protocol.Implementation
	proxy            ConnectionType
	timingConfig     TCPClientConfiguration
	conn             net.Conn
	connected        bool
	isStopped        bool
	handler          Handler
}

func CreateNewTCPClient(hostname string, port int,
	lowLevelProtocol protocol.Implementation,
	proxy ConnectionType, timing ...TCPClientConfiguration) ConnectionAndSessionInstance {

	var clientConfiguration TCPClientConfiguration
	if len(timing) == 0 {
		clientConfiguration = DefaultTCPClientSettings
	} else {
		clientConfiguration = timing[0]
	}

	return &tcpClientConnectionAndSession{
		hostname:         hostname,
		port:             port,
		lowLevelProtocol: lowLevelProtocol,
		proxy:            proxy,
		timingConfig:     clientConfiguration,
		connected:        false,
		isStopped:        false,
		handler:          nil, // is set by run
	}
}

func (s *tcpClientConnectionAndSession) WaitReady() bool {
	// a client is not async. this function is only useful for the server
	return false
}

// Run - Ensure the client stays connected and receives Data.
// On disconnect from server the client will forever retry to connect.
// Call Stop() will exit the loop
func (s *tcpClientConnectionAndSession) Run(handler Handler) {
	s.handler = handler
	s.isStopped = false

	s.Connect()
	for !s.isStopped && s.IsAlive() {
		s.ensureConnected()
		data, err := s.Receive()
		if err != nil {
			if err == io.EOF {
				s.Close()
				break
			} else {
				if s.handler != nil {
					s.handler.Error(s, ErrorReceive, err)
				}
			}
		} else {
			handler.DataReceived(s, data, time.Now())
		}
	}

	s.handler = nil
}

func (s *tcpClientConnectionAndSession) Stop() {
	s.isStopped = true
	s.Close()
}

func (instance *tcpClientConnectionAndSession) FindSessionsByIp(ip string) []Session {
	sessions := make([]Session, 0)

	// todo: restrict to only your remote
	sessions = append(sessions, instance)

	return sessions
}

func (s *tcpClientConnectionAndSession) RemoteAddress() (string, error) {
	if s.conn != nil {
		if err := s.ensureConnected(); err != nil {
			return "", err
		}
	}

	if s.conn != nil {
		host, _, err := net.SplitHostPort(s.conn.RemoteAddr().String())
		return host, err
	} else {
		return "", nil
	}
}

func (s *tcpClientConnectionAndSession) Close() error {
	if s.conn != nil {
		if s.handler != nil {
			s.handler.Disconnected(s)
		}
		err := s.conn.Close()
		s.conn = nil
		if err != nil {
			if s.handler != nil {
				s.handler.Error(s, ErrorDisconnect, err)
			}
			return err
		}
	}
	return nil
}

func (s *tcpClientConnectionAndSession) IsAlive() bool {
	if s.conn != nil && s.connected {
		return true
	}
	return false
}

func (s *tcpClientConnectionAndSession) WaitTermination() error {
	return errors.New("client connections can not be passively observed for disconnect")
}

func (s *tcpClientConnectionAndSession) Connect() error {
	if err := s.ensureConnected(); err != nil {
		if s.handler != nil {
			go s.handler.Error(s, ErrorConnect, fmt.Errorf("failed to connect - %w", err))
		}
		return err
	}
	return nil
}

func (s *tcpClientConnectionAndSession) Receive() ([]byte, error) {

	if err := s.ensureConnected(); err != nil {
		if s.handler != nil {
			s.handler.Error(s, ErrorReceive, fmt.Errorf("failed to reconnect %w", err))
		}
		return nil, err
	}

	return s.lowLevelProtocol.Receive(s.conn)
}

func (s *tcpClientConnectionAndSession) Send(data [][]byte) (int, error) {

	if err := s.ensureConnected(); err != nil {
		return 0, err
	}

	return s.lowLevelProtocol.Send(s.conn, data)
}

func (s *tcpClientConnectionAndSession) ensureConnected() error {
	if s.connected || s.isStopped {
		// fmt.Printf("Return due to connected:%t and stopped:%t conn is nil?:%t\n", s.connected, s.isStopped, s.conn == nil)
		return nil
	}

	dialer := &net.Dialer{}
	if s.timingConfig.SourceIP != "" {
		sourceIP, err := net.ResolveTCPAddr("tcp", s.timingConfig.SourceIP+":0")
		if err != nil {
			if s.handler != nil {
				s.handler.Error(s, ErrorConnect, err)
			}
			return err
		} else {
			dialer.LocalAddr = sourceIP
		}
	}

	conn, err := dialer.Dial("tcp", s.hostname+fmt.Sprintf(":%d", s.port))
	if err != nil {
		if s.handler != nil {
			s.handler.Error(s, ErrorConnect, err)
		}
		return err
	}
	s.conn = conn
	s.connected = true

	if s.handler != nil {
		s.handler.Connected(s)
	}

	return nil
}
