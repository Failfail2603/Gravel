package main

import (
	"encoding/json"
	"gravel/db"
	"gravel/nats_server"
	"log"

	"github.com/nats-io/nats.go"
)

type WatchQueryRequest struct {
	Database   string `json:"database"`
	Collection string `json:"collection"`
}

type GravelServer struct {
	natsConnection *nats_server.NatsConnection

	/**
	 * dbServices is a map of database services.
	 * If a client connects to gravel and requests a database connection and there is currently no database open,
	 * gravel will create a new database service for that client.
	 */
	dbServices map[string]*db.DBService
}

func generateGravelServer(natsConnection *nats_server.NatsConnection) *GravelServer {
	return &GravelServer{
		natsConnection: natsConnection,
		dbServices:     make(map[string]*db.DBService),
	}
}

func (gravel *GravelServer) listenToConnects() {
	gravel.natsConnection.SubscribeTo("gravel.connect", func(m *nats.Msg) {
		log.Println("Received gravel.connect request")
		var req db.DatabaseConnectRequest

		// check if request is valid and parse it to internal type
		//
		if err := json.Unmarshal(m.Data, &req); err != nil {
			response := db.DatabaseConnectResponse{
				Status:   "error",
				Database: req.MongoURL,
				Error:    err.Error(),
			}
			responseData, _ := json.Marshal(response)
			log.Println(response.Error)
			m.Respond(responseData)
			return
		}

		// check if a connection to the requested database already exists
		// if not create a new connection
		if _, exists := gravel.dbServices[req.ClientID]; !exists {

			service, err := db.StartDBConnection(req)

			// there can be errors in the connection buildup. We want to give them back to the client which is connected, because it should be config error
			if err != nil {
				response := db.DatabaseConnectResponse{
					Status:   "error",
					Database: req.MongoURL,
					Error:    err.Error(),
				}
				responseData, _ := json.Marshal(response)
				log.Println(response.Error)
				m.Respond(responseData)
				return
			}
			gravel.dbServices[req.ClientID] = service
		}

		// Send success response
		response := db.DatabaseConnectResponse{
			Status:   "connected",
			Database: req.MongoURL,
		}
		responseData, _ := json.Marshal(response)
		m.Respond(responseData)
	})

	gravel.natsConnection.SubscribeTo("gravel.watchquery", func(m *nats.Msg) {
		log.Println("Received gravel.watchquery request")
		var req db.WatchQueryRequest

		// check if request is valid and parse it to internal type
		//
		if err := json.Unmarshal(m.Data, &req); err != nil {
			response := db.WatchQueryResponse{
				Status: "error",
				Error:  err.Error(),
			}
			responseData, _ := json.Marshal(response)
			log.Println(response.Error)
			m.Respond(responseData)
			return
		}

		var dbService *db.DBService = gravel.dbServices[req.ClientID]

		if dbService == nil {
			response := db.WatchQueryResponse{
				Status: "error",
				Error:  "No database connection found for client " + req.ClientID,
			}
			responseData, _ := json.Marshal(response)
			log.Println(response.Error)
			m.Respond(responseData)
			return
		}

		// check if the watchquery already exists with the hash. Different clients can have the same watchquery.
		// we need to ensure that all unique clients in the
		watchQuery := dbService.WatchQueries[req.Hash]

		// if yes we just count up the connections count. We do not need to do anything else as gravel already sends updates down the channel
		if watchQuery != nil {
			watchQuery.NumberOfConnections++
			response := db.WatchQueryResponse{
				Status: "success",
			}
			responseData, _ := json.Marshal(response)
			log.Println(response.Error)
			m.Respond(responseData)
			return
		}

		// if the watchqueries are empty we need to start the change stream
		var shouldStartChangeStream bool = len(dbService.WatchQueries) == 0

		// if no we create a new watchquery and start the change stream

		dbService.WatchQueries[req.Hash] = &db.WatchQuery{
			ClientID:            req.ClientID,
			Hash:                req.Hash,
			Collection:          req.CollectionName,
			Query:               req.Query,
			Options:             req.Options,
			NumberOfConnections: 1,
		}

		updateChannel := make(chan db.DBChangeStreamEvent)

		if shouldStartChangeStream {
			dbService.Connection.StartChangeStream(updateChannel)
		}

		go func() {
			for update := range updateChannel {
				log.Println("Sending update to client", req.ClientID)
				updateJson, _ := json.Marshal(update)
				gravel.natsConnection.Publish("gravel.mongo.watchquery."+req.ClientID, string(updateJson))
			}
		}()

		response := db.WatchQueryResponse{
			Status: "success",
		}
		responseData, _ := json.Marshal(response)
		log.Println(response.Error)
		m.Respond(responseData)

	})
}

func (gravel *GravelServer) StartListening() {

	gravel.listenToConnects()

}
