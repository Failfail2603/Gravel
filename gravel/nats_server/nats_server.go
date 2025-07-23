package nats_server

import (
	"fmt"
	"gravel/env"
	"log"
	"strconv"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type NatsConnection struct {
	natsServer *server.Server
	natsConn   *nats.Conn
}

func connectToNats(natsInterface *NatsConnection) error {
	natsURL := fmt.Sprintf("nats://%s:%s",
		env.GetEnvOrDefault("NATS_HOST", "localhost"),
		env.GetEnvOrDefault("NATS_PORT", "4222"))

	var err error
	natsInterface.natsConn, err = nats.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %v", err)
	}

	return nil
}

func Shutdown(natsInterface *NatsConnection) {
	// Close NATS connection
	if natsInterface.natsConn != nil {
		natsInterface.natsConn.Close()
	}

	// Shutdown NATS server
	if natsInterface.natsServer != nil {
		natsInterface.natsServer.Shutdown()
	}
}

func StartNatsServer() (*NatsConnection, error) {
	log.Println("Starting NATS server...")
	natsInterface := &NatsConnection{}

	host := env.GetEnvOrDefault("NATS_HOST", "localhost")
	port, err := strconv.Atoi(env.GetEnvOrDefault("NATS_PORT", "4222"))
	if err != nil {
		return nil, fmt.Errorf("invalid NATS_PORT: %v", err)
	}

	opts := &server.Options{
		Host: host,
		Port: port,
	}

	natsInterface.natsServer, err = server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS server: %v", err)
	}

	go natsInterface.natsServer.Start()

	// Wait for server to be ready
	if !natsInterface.natsServer.ReadyForConnections(5 * time.Second) {
		return nil, fmt.Errorf("NATS server not ready")
	}

	if err := connectToNats(natsInterface); err != nil {
		return nil, err
	}

	return natsInterface, nil
}

func (natsInterface *NatsConnection) SubscribeTo(subject string, callback nats.MsgHandler) {
	natsInterface.natsConn.Subscribe(subject, callback)
}
