package lucidity

import (
	"errors"
	"log"
	"os"

	"nanomsg.org/go-mangos"
	"nanomsg.org/go-mangos/protocol/pub"
	"nanomsg.org/go-mangos/protocol/req"
	"nanomsg.org/go-mangos/protocol/sub"
	"nanomsg.org/go-mangos/transport/ipc"
	"nanomsg.org/go-mangos/transport/tcp"

	"github.com/golang/protobuf/proto"
	pb "github.com/lucidity-dev/bulletin/protobuf"
)

type SocketType int

const (
	BULLETIN_URL            = "tcp://127.0.0.1:40899"
	PUB_SOCKET   SocketType = 0
	SUB_SOCKET   SocketType = 1
)

var bulletin_server mangos.Socket

func Connect() {
	var err error

	bulletin_server, err = req.NewSocket()
	if err != nil {
		log.Fatalf("Error: can't connect to bulletin. %s \n", err)
		os.Exit(-1)
	}

	bulletin_server.AddTransport(tcp.NewTransport())

	err = bulletin_server.Dial(BULLETIN_URL)
	if err != nil {
		log.Fatalf("Error: can't dial on socket: %s \n", err)
		os.Exit(-1)
	}
}

func GetTopicSocket(topic String, mode SocketType) (mangos.Socket, error) {
	url := getTopicURL(topic)

	switch mode {
	case PUB_SOCKET:
		socket, err := pub.NewSocket()
		if err != nil {
			log.Fatalf("Error: can't listen on pub socket: %s \n", err)
			os.Exit(-1)
		}
		socket.AddTransport(ipc.NewTransport())
		socket.AddTransport(tcp.NewTransport())
		if err = socket.Listen(url); err != nil {
			log.Fatalf("Error: can't get new pub socket: %s \n")
			os.Exit(-1)
		}
		return socket, nil
	case SUB_SOCKET:
		socket, err := sub.NewSocket()
		if err != nil {
			log.Fatalf("Error: can't listen on pub socket: %s \n", err)
			os.Exit(-1)
		}
		socket.AddTransport(ipc.NewTransport())
		socket.AddTransport(tcp.NewTransport())
		if err = socket.Dial(url); err != nil {
			log.Fatalf("Error: can't get new pub socket: %s \n")
			os.Exit(-1)
		}

		err = socket.SetOption(mangos.OptionSubscribe, []byte(""))
		if err != nil {
			log.Fatalf("Error: can't subscribe: %s", err)
		}

		return socket, nil
	default:
		var socket mangos.Socket
		return socket, errors.New("Not a valid socket mode.")
	}

}

func getTopicURL(topic string) string {
	request := &pb.Message{
		Cmd:  pb.Message_GET,
		Args: topic,
	}

	data, err := proto.Marshal(request)
	if err != nil {
		log.Fatalf("Error: couldn't create protobuf: %s \n")
		os.Exit(-1)
	}

	if err = bulletin_server.Send(data); err != nil {
		log.Fatalf("Error: failed to send message: %s \n")
		os.Exit(-1)
	}

	var msg []byte
	if msg, err = bulletin_server.Recv(); err != nil {
		log.Fatalf("Error: failed to receive data: %s \n")
		os.Exit(-1)
	}

	var body pb.Topic
	proto.Unmarshal(msg, &body)
	return body.Url
}
