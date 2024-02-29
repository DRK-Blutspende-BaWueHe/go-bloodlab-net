package protocol

/*
Implementation of the Beckman&Coulter AU6xx low-level protocol.
*/

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/blutspende/go-bloodlab-net/protocol/utilities"
)

type AU6XXProtocolSettings struct {
	strictChecksumValidation bool
	sendTimeoutDuration      time.Duration
	startByte                byte
	endByte                  byte
	lineBreak                byte
	acknowledgementTimeout   time.Duration
	realTimeDataTransmission bool
}

func (s AU6XXProtocolSettings) SetLineBreakByte(lineBreak byte) *AU6XXProtocolSettings {
	s.lineBreak = lineBreak
	return &s
}

func (s AU6XXProtocolSettings) SetStartByte(start byte) *AU6XXProtocolSettings {
	s.startByte = start
	return &s
}

func (s AU6XXProtocolSettings) SetEndByte(end byte) *AU6XXProtocolSettings {
	s.endByte = end
	return &s
}

func (s AU6XXProtocolSettings) SetSendTimeoutDuration(duration time.Duration) *AU6XXProtocolSettings {
	s.sendTimeoutDuration = duration
	return &s
}

func (s AU6XXProtocolSettings) EnableChecksumValidation() *AU6XXProtocolSettings {
	s.strictChecksumValidation = true
	return &s
}

func (s AU6XXProtocolSettings) DisableChecksumValidation() *AU6XXProtocolSettings {
	s.strictChecksumValidation = false
	return &s
}

// SetAcknowledgementTimeout should be between 0.5ms to max 2.5s
// Default is 500ms
func (s AU6XXProtocolSettings) SetAcknowledgementTimeout(duration time.Duration) *AU6XXProtocolSettings {
	if duration > time.Millisecond*2500 {
		fmt.Printf("warning | acknowledgementTimeout is too big. The instrument wont work with a duration of %d ms", duration/time.Millisecond)
	}

	s.acknowledgementTimeout = duration
	return &s
}

func (s AU6XXProtocolSettings) EnableRealTimeDataTransmission() *AU6XXProtocolSettings {
	s.realTimeDataTransmission = true
	return &s
}

func (s AU6XXProtocolSettings) DisableRealTimeDataTransmission() *AU6XXProtocolSettings {
	s.realTimeDataTransmission = false
	return &s
}

func DefaultAU6XXProtocolSettings() *AU6XXProtocolSettings {
	return &AU6XXProtocolSettings{
		strictChecksumValidation: false,
		sendTimeoutDuration:      time.Second * 60,
		startByte:                utilities.STX,
		endByte:                  utilities.ETX,
		lineBreak:                utilities.CR,
		acknowledgementTimeout:   time.Millisecond * 500,
		realTimeDataTransmission: false,
	}
}

type processState struct {
	State           int
	LastMessage     string
	LastChecksum    string
	MessageLog      []string
	ProtocolMessage protocolMessage
	isRequest       bool
}

type au6xxProtocol struct {
	settings               *AU6XXProtocolSettings
	receiveThreadIsRunning bool
	receiveQ               chan protocolMessage
	state                  processState
}

func AU6XXProtocol(settings ...*AU6XXProtocolSettings) Implementation {
	var theSettings *AU6XXProtocolSettings
	if len(settings) >= 1 {
		theSettings = settings[0]
	} else {
		theSettings = DefaultAU6XXProtocolSettings()
	}

	return &au6xxProtocol{
		settings: theSettings,
		receiveQ: make(chan protocolMessage),
	}
}

const (
	RequestStart          utilities.ActionCode = "RequestStart"
	RetransmitLastMessage utilities.ActionCode = "RetransmitLastMessage"
)

func (p *au6xxProtocol) Interrupt() {
	panic("not implemented")
}

func (p *au6xxProtocol) generateRules() []utilities.Rule {
	var printableChars8BitWithoutE = []byte{10, 13, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255}

	// CHECK For If CheckSumCheck is enabled
	return []utilities.Rule{
		{FromState: 0, Symbols: []byte{p.settings.startByte}, ToState: 1, Scan: false},
		{FromState: 1, Symbols: []byte{'D', 'S', 'd'}, ToState: 2, Scan: true},
		{FromState: 1, Symbols: []byte{'R'}, ToState: 10, Scan: true},

		{FromState: 10, Symbols: []byte{'B'}, ToState: 14, Scan: true},
		{FromState: 10, Symbols: []byte{'E'}, ToState: 15, Scan: true},
		{FromState: 10, Symbols: printableChars8BitWithoutE, ToState: 11, ActionCode: RequestStart, Scan: true},
		{FromState: 11, Symbols: []byte{p.settings.endByte}, ToState: 12, ActionCode: LineReceived, Scan: false},
		{FromState: 11, Symbols: utilities.PrintableChars8Bit, ToState: 11, Scan: true},

		{FromState: 12, Symbols: []byte{utilities.ACK}, ToState: 13, Scan: false},
		{FromState: 12, Symbols: []byte{utilities.NAK}, ToState: 13, ActionCode: RetransmitLastMessage, Scan: false},

		{FromState: 13, Symbols: []byte{p.settings.startByte}, ToState: 1, Scan: false},
		{FromState: 13, Symbols: []byte{utilities.NAK}, ToState: 13, ActionCode: RetransmitLastMessage, Scan: false},
		{FromState: 13, Symbols: []byte{utilities.ACK}, ToState: 13, Scan: false},

		{FromState: 14, Symbols: []byte{p.settings.endByte}, ToState: 13, ActionCode: LineReceived, Scan: false},
		{FromState: 14, Symbols: utilities.PrintableChars8Bit, ToState: 14, Scan: true},

		{FromState: 2, Symbols: printableChars8BitWithoutE, ToState: 3, Scan: true},
		{FromState: 3, Symbols: []byte{p.settings.endByte}, ToState: 5, ActionCode: LineReceived, Scan: false},
		{FromState: 3, Symbols: utilities.PrintableChars8Bit, ToState: 3, Scan: true},

		//utilities.Rule{FromState:4 , Symbols: utilities.PrintableChars8Bit, ToState: 5, ActionCode: utilities.CheckSum, Scan: true},
		{FromState: 5, Symbols: []byte{p.settings.startByte}, ToState: 1, Scan: false},

		{FromState: 2, Symbols: []byte{'E'}, ToState: 7, Scan: true},
		{FromState: 7, Symbols: []byte{p.settings.endByte}, ToState: 15, ActionCode: utilities.Finished, Scan: false},
		{FromState: 7, Symbols: utilities.PrintableChars8Bit, ToState: 7, Scan: true},
		//utilities.Rule{FromState: 8, Symbols: utilities.PrintableChars8Bit, ToState: 9, ActionCode: utilities.CheckSum, Scan: true},
		{FromState: 9, Symbols: utilities.PrintableChars8Bit, ToState: 0, ActionCode: utilities.Finished, Scan: false},
		{FromState: 15, Symbols: []byte{p.settings.endByte}, ToState: 16, ActionCode: utilities.RequestFinished, Scan: false},
		{FromState: 15, Symbols: utilities.PrintableChars8Bit, ToState: 15, Scan: true},

		{FromState: 16, Symbols: []byte{utilities.ACK, utilities.NAK}, ToState: 0},
		{FromState: 16, Symbols: []byte{p.settings.startByte}, ToState: 1, Scan: false},
	}
}

func (p *au6xxProtocol) ensureReceiveThreadRunning(conn net.Conn) {

	if p.receiveThreadIsRunning {
		return
	}

	go func() {
		p.receiveThreadIsRunning = true
		p.state.State = 0

		lastMessage := make([]byte, 0)
		tcpReceiveBuffer := make([]byte, 4096)
		fileBuffer := make([][]byte, 0)

		fsm := utilities.CreateFSM(p.generateRules())
		for p.receiveThreadIsRunning {

			err := conn.SetReadDeadline(time.Now().Add(time.Minute * 25))
			if err != nil {
				fmt.Printf(`should not happen: %s`, err.Error())
				p.receiveThreadIsRunning = false
				p.receiveQ <- protocolMessage{
					Status: DISCONNECT,
					Data:   []byte(err.Error()),
				}
				return
			}
			n, err := conn.Read(tcpReceiveBuffer)
			// enabled FSM
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					fmt.Printf("read timeout reached. reset fsm")
					tcpReceiveBuffer = make([]byte, 4096)
					fsm.ResetBuffer()
					fsm.Init()
					continue // on timeout....
				} else if opErr, ok := err.(*net.OpError); ok && opErr.Op == "read" {
					p.receiveThreadIsRunning = false
					p.receiveQ <- protocolMessage{
						Status: DISCONNECT,
						Data:   []byte(err.Error()),
					}
					return
				} else if err == io.EOF { // EOF = silent exit
					p.receiveQ <- protocolMessage{
						Status: DISCONNECT,
						Data:   []byte(err.Error()),
					}
					p.receiveThreadIsRunning = false
					return
				}

				p.receiveQ <- protocolMessage{
					Status: DISCONNECT,
					Data:   []byte(err.Error()),
				}
				p.receiveThreadIsRunning = false
				return
			}

			for _, ascii := range tcpReceiveBuffer[:n] {
				messageBuffer, action, err := fsm.Push(ascii)
				if err != nil {
					p.receiveQ <- protocolMessage{
						Status: ERROR,
						Data:   []byte(err.Error()),
					}
					p.receiveThreadIsRunning = false
					return
				}

				switch action {
				case utilities.Ok:
				case utilities.Error:
					// error
					protocolMsg := protocolMessage{
						Status: ERROR,
						Data:   []byte("Internal error"),
					}

					if err != nil {
						protocolMsg.Data = []byte(err.Error())
					}

					p.receiveQ <- protocolMsg
					p.receiveThreadIsRunning = false
					return
				case RequestStart:
					p.state.isRequest = true
				case LineReceived:
					// append Data
					if p.settings.acknowledgementTimeout > 0 {
						time.Sleep(p.settings.acknowledgementTimeout)
					}
					_, err = conn.Write([]byte{utilities.ACK})
					if err != nil {
						fmt.Printf("can not send ACK in LineReceived. Should never happen\n")
						fsm.Init()
					}

					lastMessage = messageBuffer
					if p.state.isRequest && len(messageBuffer) > 5 { // 5 Because if a bcc is set than its bigger than 4
						// Request logic
						p.receiveQ <- protocolMessage{
							Status: DATA,
							Data:   messageBuffer,
						}
					} else if !p.state.isRequest {
						if p.settings.realTimeDataTransmission {
							p.receiveQ <- protocolMessage{
								Status: DATA,
								Data:   messageBuffer,
							}
						} else {
							fileBuffer = append(fileBuffer, lastMessage)
						}
					}

					fsm.ResetBuffer()
				case RetransmitLastMessage:
					p.receiveQ <- protocolMessage{
						Status: DATA,
						Data:   lastMessage,
					}

				case utilities.RequestFinished:
					time.Sleep(p.settings.acknowledgementTimeout)
					_, err = conn.Write([]byte{utilities.ACK})
					if err != nil {
						fmt.Printf("can not send ACK in Finish. Should never happen\n")
						fsm.Init()
					}

					/*time.Sleep(p.settings.acknowledgementTimeout)
					_, err = conn.Write([]byte{utilities.STX, 'S', 'E', p.settings.endByte})
					if err != nil {
						fmt.Printf("can not send ACK in Finish. Should never happen\n")
						fsm.Init()
					}
					fileBuffer = make([][]byte, 0) */
					fsm.ResetBuffer()
				case utilities.Finished:
					// send fileData if not request
					if p.settings.acknowledgementTimeout > 0 {
						time.Sleep(p.settings.acknowledgementTimeout)
					}
					_, err = conn.Write([]byte{utilities.ACK})
					if err != nil {
						fmt.Printf("can not send ACK in Finish. Should never happen\n")
						fsm.Init()
					}

					fullMsg := make([]byte, 0)
					for _, messageLine := range fileBuffer {
						// skip first element because this is only RecordType + unitNo not needed in instrumentAPI
						if len(messageLine) <= 5 {
							continue
						}
						fullMsg = append(fullMsg, messageLine...)
						fullMsg = append(fullMsg, p.settings.lineBreak)
					}

					// Send only if data are set
					if len(fullMsg) > 0 {
						p.receiveQ <- protocolMessage{
							Status: DATA,
							Data:   fullMsg,
						}
					}
					fileBuffer = make([][]byte, 0)
					fsm.ResetBuffer()
					fsm.Init()
				//case JustAck:
				//	if p.settings.acknowledgementTimeout > 0 {
				//		time.Sleep(p.settings.acknowledgementTimeout)
				//	}
				//	_, err = conn.Write([]byte{utilities.ACK})
				//	if err != nil {
				//		fsm.Init()
				//	}
				default:
					protocolMsg := protocolMessage{
						Status: ERROR,
						Data:   []byte("Invalid action code "),
					}

					p.receiveQ <- protocolMsg
					p.receiveThreadIsRunning = false
					fmt.Println("Disconnect due to unexpected, unknown and unlikely error")
					return
				}
			}
		}
	}()
}

func (p *au6xxProtocol) Receive(conn net.Conn) ([]byte, error) {
	p.ensureReceiveThreadRunning(conn)

	message := <-p.receiveQ

	switch message.Status {
	case DATA:
		return message.Data, nil
	case EOF, DISCONNECT:
		return []byte{}, io.EOF
	case ERROR:
		return []byte{}, fmt.Errorf("error while reading - abort receiving data: %s", string(message.Data))
	default:
		return []byte{}, fmt.Errorf("internal error: Invalid status of communication (%d) - abort", message.Status)
	}
}

func (p *au6xxProtocol) Send(conn net.Conn, data [][]byte) (int, error) {
	// Maybe need to wait until the answer of the instrument
	for _, buff := range data {
		msgBuff := make([]byte, 0)
		if p.settings.acknowledgementTimeout > 0 {
			time.Sleep(p.settings.acknowledgementTimeout)
		}
		msgBuff = append(msgBuff, p.settings.startByte)
		msgBuff = append(msgBuff, buff...)
		msgBuff = append(msgBuff, p.settings.endByte)
		_, err := conn.Write(msgBuff)
		if err != nil {
			return -1, err
		}
	}

	return 0, nil
}

func (p *au6xxProtocol) receiveSendAnswer(conn net.Conn) (byte, error) {
	err := conn.SetReadDeadline(time.Now().Add(time.Second * p.settings.sendTimeoutDuration))
	if err != nil {
		return 0, ReceiverDoesNotRespond
	}

	receivingMsg := make([]byte, 1)
	_, err = conn.Read(receivingMsg)
	if err != nil {
		return 0, err
	}

	switch receivingMsg[0] {
	case utilities.ACK:
		// was successfully do next
		return utilities.ACK, nil
	case utilities.NAK:
		// Last was not successfully do next
		return utilities.NAK, nil
	default:
		return 0, ReceivedMessageIsInvalid
	}

}

func (p *au6xxProtocol) NewInstance() Implementation {
	return &au6xxProtocol{
		settings: p.settings,
		receiveQ: make(chan protocolMessage),
	}
}
