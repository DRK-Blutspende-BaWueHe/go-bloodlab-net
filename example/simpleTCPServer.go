package main

import (
	"fmt"
	"time"

	bnet "github.com/DRK-Blutspende-BaWueHe/go-bloodlab-net"
	"github.com/DRK-Blutspende-BaWueHe/go-bloodlab-net/protocol"
)

type MySessionHandler struct {
}

func (s *MySessionHandler) Connected(session bnet.Session) error {
	fmt.Println("Connect Event")
	return nil
}

func (s *MySessionHandler) Disconnected(session bnet.Session) {
	fmt.Println("Disconnected Event")
}

func (s *MySessionHandler) Error(session bnet.Session, errorType bnet.ErrorType, err error) {
	fmt.Println(err)
}

func (s *MySessionHandler) DataReceived(session bnet.Session, data []byte, receiveTimestamp time.Time) {
	rad, _ := session.RemoteAddress()
	fmt.Printf("From %s received '%s'", rad, string(data))

	dataToSend := make([][]byte, 0)
	dataToSend = append(dataToSend, []byte(fmt.Sprintf("You are sending from %s", rad)))
	session.Send(dataToSend)
}

func main() {

	server := bnet.CreateNewTCPServerInstance(4001,
		protocol.Lis1A1Protocol(),
		bnet.HAProxySendProxyV2,
		10) // Max Connections

	server.Run(&MySessionHandler{})
}
