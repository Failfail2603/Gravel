package main

import (
	"encoding/json"
	"fmt"
	"gravel/db"
	"gravel/json_patch"
	"gravel/nats_server"
	"gravel/relevant_changes"
	"gravel/types"
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

func (gravel *GravelServer) runQuery(dbService *db.DBService, req types.WatchQueryRequest) (*types.WatchQueryResponse, error) {
	// Execute the query using the dbService
	results := dbService.Connection.Query(req.CollectionName, req.Query, req.Options)

	// Marshal the results to JSON
	resultData, err := json.Marshal(results)
	if err != nil {
		errorMsg := "Failed to marshal query results for client " + req.ClientID + ": " + err.Error()
		log.Println(errorMsg)
		gravel.natsConnection.Publish("gravel.debug", errorMsg)
		return nil, err
	}

	// if resultdata is "null" (standard for no documents found and json makes a string)
	if string(resultData) == "null" {
		resultData = []byte("[]")
	}

	response := types.WatchQueryResponse{
		QueryHash: req.Hash,
		Type:      "full",
		Result:    string(resultData),
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		errorMsg := "Failed to marshal query results for client " + req.ClientID + ": " + err.Error()
		log.Println(errorMsg)
		gravel.natsConnection.Publish("gravel.debug", errorMsg)
		return nil, err
	}

	// Publish the results to the initial data channel
	channelName := "gravel.mongo.initial." + req.ClientID
	gravel.natsConnection.Publish(channelName, string(responseJSON))

	log.Printf("Query results sent to client %s on channel %s", req.ClientID, channelName)

	return &response, nil
}

func (gravel *GravelServer) StartListening() {

	gravel.natsConnection.SubscribeTo("gravel.connect", func(m *nats.Msg) {
		log.Println("Received gravel.connect request")
		var req types.DatabaseConnectRequest

		// check if request is valid and parse it to internal type
		//
		if err := json.Unmarshal(m.Data, &req); err != nil {
			response := types.DatabaseConnectResponse{
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
				response := types.DatabaseConnectResponse{
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
		response := types.DatabaseConnectResponse{
			Status:   "connected",
			Database: req.MongoURL,
		}
		responseData, _ := json.Marshal(response)
		m.Respond(responseData)
	})

	gravel.natsConnection.SubscribeTo("gravel.watchquery", func(m *nats.Msg) {
		log.Println("Received gravel.watchquery request")
		var req types.WatchQueryRequest

		// check if request is valid and parse it to internal type
		//
		if err := json.Unmarshal(m.Data, &req); err != nil {
			response := types.DebugMessage{
				ClientID: req.ClientID,
				Status:   "error",
				Error:    err.Error(),
			}
			responseData, _ := json.Marshal(response)
			if response.Error != "" {
				log.Println(response.Error)
			}
			m.Respond(responseData)
			return
		}

		var dbService *db.DBService = gravel.dbServices[req.ClientID]

		if dbService == nil {
			response := types.DebugMessage{
				ClientID: req.ClientID,
				Status:   "error",
				Error:    "No database connection found for client " + req.ClientID,
			}
			responseData, _ := json.Marshal(response)
			if response.Error != "" {
				log.Println(response.Error)
			}
			m.Respond(responseData)
			return
		}

		// run the initial query which directly pipes to the client
		queryResult, err := gravel.runQuery(dbService, req)
		if err != nil {
			response := types.DebugMessage{
				ClientID: req.ClientID,
				Status:   "error",
				Error:    "Failed to execute initial query: " + err.Error(),
			}
			responseData, _ := json.Marshal(response)
			log.Println(response.Error)
			m.Respond(responseData)
			return
		}

		// Now you have access to the query result here
		log.Printf("Query executed successfully for client %s, result type: %s", req.ClientID, queryResult.Type)

		// check if the watchquery already exists with the hash. Different clients can have the same watchquery.
		// we need to ensure that all unique clients in the
		watchQuery := dbService.WatchQueries[req.Hash]

		// if yes we just count up the connections count. We do not need to do anything else as gravel already sends updates down the channel
		if watchQuery != nil {
			watchQuery.NumberOfConnections++
			response := types.DebugMessage{
				ClientID: req.ClientID,
				Message:  "Matching watchquery found for Query with Hash " + req.Hash + ". Increased connection count to " + fmt.Sprint(watchQuery.NumberOfConnections),
				Status:   "success",
			}
			responseData, _ := json.Marshal(response)
			if response.Error != "" {
				log.Println(response.Error)
			}
			m.Respond(responseData)
			return
		}

		// if the watchqueries are empty we need to start the change stream
		var shouldStartChangeStream bool = len(dbService.WatchQueries) == 0

		queryInformation, err := dbService.Connection.GetQueryAnalysis(req, queryResult)

		if err != nil {
			response := types.DebugMessage{
				ClientID: req.ClientID,
				Status:   "error",
				Error:    err.Error(),
			}
			responseData, _ := json.Marshal(response)
			if response.Error != "" {
				log.Println(response.Error)
			}
			m.Respond(responseData)
			return
		}

		// if no we create a new watchquery and start the change stream
		var newWatchQuery db.WatchQuery = db.WatchQuery{
			ClientID:            req.ClientID,
			Hash:                req.Hash,
			Collection:          req.CollectionName,
			Query:               req.Query,
			Options:             req.Options,
			NumberOfConnections: 1,
			QueryInformation:    queryInformation,
		}

		// build watched document if the window is not infinite
		watchedDocuments := []types.WatchedDocument{}
		if !newWatchQuery.IsInfiniteWindow() {

			// parse the result to documents
			var documents []types.Document
			if queryResult != nil && queryResult.Result != "" {
				if err := json.Unmarshal([]byte(queryResult.Result), &documents); err != nil {
					log.Printf("Failed to unmarshal query results to extract document IDs: %v", err)
				}
			}

			for _, document := range documents {

				watchedDocument, err := dbService.Connection.GetWatchedDocumentInfo(document, queryInformation)
				if err != nil {
					log.Printf("Failed to get watched document info: %v", err)
					continue
				}

				watchedDocuments = append(watchedDocuments, watchedDocument)
			}
		}

		newWatchQuery.WatchedDocuments = watchedDocuments
		dbService.WatchQueries[req.Hash] = &newWatchQuery

		log.Printf("WatchedDocuments: %+v", watchedDocuments)

		if shouldStartChangeStream {
			dbService.UpdateChannel = make(chan types.DBChangeStreamEvent)
			go dbService.Connection.StartChangeStream(dbService.UpdateChannel)
		}

		go func() {
			for update := range dbService.UpdateChannel {

				patches := relevant_changes.GetPatchesForChange(dbService, &newWatchQuery, &update)

				// check if the update is relevant for the watchquery
				if len(patches) == 0 {
					continue
				}

				// send the update to the client
				update := types.WatchQueryResponse{
					QueryHash: req.Hash,
					Type:      "patch",
					Result:    json_patch.PatchArrayToString(patches),
				}

				responseData, _ := json.Marshal(update)
				gravel.natsConnection.Publish("gravel.mongo.watchquery."+req.ClientID, string(responseData))
			}
		}()

		response := types.DebugMessage{
			ClientID: req.ClientID,
			Message:  "Successfully initialized watchquery for Query with Hash " + req.Hash,
			Status:   "success",
		}
		responseData, _ := json.Marshal(response)
		if response.Error != "" {
			log.Println(response.Error)
		}
		m.Respond(responseData)

	})

	gravel.natsConnection.SubscribeTo("gravel.watchquery.stop", func(m *nats.Msg) {
		log.Println("Received gravel.watchquery.stop request")
		var req types.WatchQueryStopRequest

		// check if request is valid and parse it to internal type
		if err := json.Unmarshal(m.Data, &req); err != nil {
			response := types.DebugMessage{
				ClientID: req.ClientID,
				Status:   "error",
				Error:    err.Error(),
			}
			responseData, _ := json.Marshal(response)
			log.Println(response.Error)
			m.Respond(responseData)
			return
		}

		// check if the watchquery exists
		watchQuery := gravel.dbServices[req.ClientID].WatchQueries[req.Hash]

		if watchQuery == nil {
			log.Println("Watchquery not found for client ", req.ClientID, " and hash ", req.Hash)
			response := types.DebugMessage{
				ClientID: req.ClientID,
				Status:   "error",
				Error:    "Watchquery not found for client " + req.ClientID + " and hash " + req.Hash + " Query is already disconnected",
			}
			responseData, _ := json.Marshal(response)
			log.Println(response.Error)
			m.Respond(responseData)
			return
		}

		watchQuery.NumberOfConnections--

		// if the connection count is 0 we pull the watchqeuery from the map
		if watchQuery.NumberOfConnections == 0 {
			delete(gravel.dbServices[req.ClientID].WatchQueries, req.Hash)
		}

		if len(gravel.dbServices[req.ClientID].WatchQueries) == 0 {
			gravel.dbServices[req.ClientID].Connection.StopChangeStream()
		}

		log.Println("Stopped watchquery for client", req.ClientID, "and hash", req.Hash)

		response := types.DebugMessage{
			ClientID: req.ClientID,
			Message:  "Successfully stopped watchquery for Query with Hash " + req.Hash,
			Status:   "success",
		}
		responseData, _ := json.Marshal(response)
		if response.Error != "" {
			log.Println(response.Error)
		}
		m.Respond(responseData)
	})

}
