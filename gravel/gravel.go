package main

import (
	"encoding/json"
	"gravel/db"
	"gravel/nats_server"
	"log"

	"github.com/nats-io/nats.go"
)

type GravelServer struct {
	natsConnection *nats_server.NatsConnection

	/**
	 * dbServices is a map of database services.
	 * If a client connects to gravel and requests a database connection and there is currently no database open,
	 * gravel will create a new database service for that client.
	 */
	dbServices map[string]*db.DBService
}

func NewGravelServer(natsConnection *nats_server.NatsConnection) *GravelServer {
	return &GravelServer{
		natsConnection: natsConnection,
		dbServices:     make(map[string]*db.DBService),
	}
}

func (gravel *GravelServer) listenToConnects() {
	gravel.natsConnection.SubscribeTo("gravel.connect", func(m *nats.Msg) {
		log.Println("Received gravel.connect request")
		var req db.DatabaseConnectRequest

		if err := json.Unmarshal(m.Data, &req); err != nil {
			response := db.DatabaseConnectResponse{
				Status:   "error",
				Database: req.Database,
				Error:    err.Error(),
			}
			responseData, _ := json.Marshal(response)
			log.Println(response.Error)
			m.Respond(responseData)
			return
		}

		// check if a connection to the requested database already exists
		// if not create a new connection
		if _, exists := gravel.dbServices[req.Database]; !exists {

			service, err := db.StartDBConnection(db.DBTypeMongoDB)

			// there can be errors in the connection buildup. We want to give them back to the client which is connected, because it should be config error
			if err != nil {
				response := db.DatabaseConnectResponse{
					Status:   "error",
					Database: req.Database,
					Error:    err.Error(),
				}
				responseData, _ := json.Marshal(response)
				log.Println(response.Error)
				m.Respond(responseData)
				return
			}
			gravel.dbServices[req.Database] = service
		}

		// Send success response
		response := db.DatabaseConnectResponse{
			Status:   "connected",
			Database: req.Database,
		}
		responseData, _ := json.Marshal(response)
		m.Respond(responseData)
	})
}

func (gravel *GravelServer) StartListening() {

	gravel.listenToConnects()

}
