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
	"time"

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

// detectAndResetClusterTimeBatch checks if a new ClusterTime batch has started
// and resets tracking state if needed
func detectAndResetClusterTimeBatch(watchQuery *db.WatchQuery, update *types.DBChangeStreamEvent) {
	if watchQuery.LastClusterTime == nil ||
		watchQuery.LastClusterTime.T != update.ClusterTime.T ||
		watchQuery.LastClusterTime.I != update.ClusterTime.I {
		// New ClusterTime batch - reset the processed documents list and shift counter
		watchQuery.LastClusterTime = update.ClusterTime
		watchQuery.ProcessedDocumentIDsInBatch = []string{}
		watchQuery.ShiftsInBatch = 0
		log.Printf("Clustertime changed: Restarting Batch")
	}
}

// prepareBatchContext copies current batch state to the event for query processing
func prepareBatchContext(watchQuery *db.WatchQuery, update *types.DBChangeStreamEvent) {
	// Copy the current batch's processed IDs to the event for query exclusion
	update.ProcessedDocumentIDs = make([]string, len(watchQuery.ProcessedDocumentIDsInBatch))
	copy(update.ProcessedDocumentIDs, watchQuery.ProcessedDocumentIDsInBatch)

	// Copy the cumulative shift offset for index adjustment in queries
	update.BatchShiftOffset = watchQuery.ShiftsInBatch
}

// countWindowShifts analyzes patches to determine how many shifts occurred
func countWindowShifts(patches []json_patch.JSONPatch, lastIndex int) int {
	shiftsInThisEvent := 0
	for _, patch := range patches {
		if patch.Op == "add" && patch.Type == "shift" {
			shiftsInThisEvent++
		}
	}
	return shiftsInThisEvent
}

// trackWindowShifts updates the shift counter for this batch
func trackWindowShifts(watchQuery *db.WatchQuery, patches []json_patch.JSONPatch) {
	shiftsInThisEvent := countWindowShifts(patches, watchQuery.ShiftsInBatch)
	if shiftsInThisEvent != 0 {
		watchQuery.ShiftsInBatch += shiftsInThisEvent
		log.Printf("Window shifted %d times in this event, total batch shifts: %d",
			shiftsInThisEvent, watchQuery.ShiftsInBatch)
	}
}

func generateGravelServer(natsConnection *nats_server.NatsConnection) *GravelServer {
	return &GravelServer{
		natsConnection: natsConnection,
		dbServices:     make(map[string]*db.DBService),
	}
}

func (gravel *GravelServer) runQuery(dbService *db.DBService, req types.WatchQueryRequest) (*types.WatchQueryResponse, []types.Document, error) {
	// Execute the query using the dbService
	results := dbService.Connection.Query(req.CollectionName, req.Query, req.Options)

	// Marshal the results to JSON
	resultData, err := json.Marshal(results)
	if err != nil {
		errorMsg := "Failed to marshal query results for client " + req.ClientID + ": " + err.Error()
		log.Println(errorMsg)
		gravel.natsConnection.Publish("gravel.debug", errorMsg)
		return nil, nil, err
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
		return nil, nil, err
	}

	// Publish the results to the initial data channel
	channelName := "gravel.mongo.initial." + req.ClientID
	gravel.natsConnection.Publish(channelName, string(responseJSON))

	return &response, results, nil
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
		queryResult, documents, err := gravel.runQuery(dbService, req)
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

		// check if the watchquery already exists with the hash. Different clients can have the same watchquery.
		// we need to ensure that all unique clients in the
		dbService.WatchQueriesMutex.RLock()
		watchQuery := dbService.WatchQueries[req.Hash]
		dbService.WatchQueriesMutex.RUnlock()

		// if yes we just count up the connections count. We do not need to do anything else as gravel already sends updates down the channel
		if watchQuery != nil {
			dbService.WatchQueriesMutex.Lock()
			watchQuery.NumberOfConnections++
			dbService.WatchQueriesMutex.Unlock()
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
		dbService.WatchQueriesMutex.RLock()
		var shouldStartChangeStream bool = len(dbService.WatchQueries) == 0
		dbService.WatchQueriesMutex.RUnlock()

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

		// use the raw documents from the query to preserve MongoDB BSON time types
		for _, document := range documents {

			watchedDocument, err := dbService.Connection.GetWatchedDocumentInfo(document, queryInformation)
			if err != nil {
				log.Printf("Failed to get watched document info: %v", err)
				continue
			}

			watchedDocuments = append(watchedDocuments, watchedDocument)
		}

		newWatchQuery.WatchedDocuments = watchedDocuments
		// Create a buffered channel for this watchquery to receive updates
		newWatchQuery.UpdateChannel = make(chan types.DBChangeStreamEvent, 100)
		dbService.WatchQueriesMutex.Lock()
		dbService.WatchQueries[req.Hash] = &newWatchQuery
		dbService.WatchQueriesMutex.Unlock()

		if shouldStartChangeStream {
			dbService.UpdateChannel = make(chan types.DBChangeStreamEvent)
			go dbService.Connection.StartChangeStream(dbService.UpdateChannel)

			// Start dispatcher goroutine that broadcasts updates to all watchqueries
			go func() {
				for update := range dbService.UpdateChannel {
					// Broadcast to all watchquery channels
					dbService.WatchQueriesMutex.RLock()
					for _, wq := range dbService.WatchQueries {
						select {
						case wq.UpdateChannel <- update:
							// Successfully sent
						default:
							// Channel full, skip this watchquery (non-blocking)
							log.Printf("Warning: UpdateChannel full for watchquery %s, dropping update", wq.Hash)
						}
					}
					dbService.WatchQueriesMutex.RUnlock()
				}
				// Close all watchquery channels when dispatcher stops
				dbService.WatchQueriesMutex.RLock()
				for _, wq := range dbService.WatchQueries {
					close(wq.UpdateChannel)
				}
				dbService.WatchQueriesMutex.RUnlock()
			}()
		}

		go func() {
			for update := range newWatchQuery.UpdateChannel {
				// Lock the watchquery to prevent concurrent modification during processing
				newWatchQuery.Mutex.Lock()

				// Check if the watchquery has been stopped
				if newWatchQuery.Stopped {
					log.Printf("Watchquery %s stopped, exiting update processing goroutine", req.Hash)
					newWatchQuery.Mutex.Unlock()
					return
				}

				start := time.Now()
				log.Println("Calculate Update for", update.ID)
				log.Printf("ClusterTime (time12Bit:inc12Bit): %d:%d", update.ClusterTime.T, update.ClusterTime.I)

				// Handle ClusterTime batching for insertMany operations
				detectAndResetClusterTimeBatch(&newWatchQuery, &update)
				prepareBatchContext(&newWatchQuery, &update)

				// Calculate patches with snapshot isolation
				patches := relevant_changes.GetPatchesForChange(dbService, &newWatchQuery, &update)

				// Update batch tracking state for subsequent events
				trackWindowShifts(&newWatchQuery, patches)

				log.Println("Patches len", len(patches))
				end := time.Now()
				log.Println("Calculated Update took ", end.Sub(start).String())
				log.Println("")

				// check if the update is relevant for the watchquery
				if len(patches) == 0 {
					newWatchQuery.Mutex.Unlock()
					continue
				}

				// send the update to the client
				updateResponse := types.WatchQueryResponse{
					QueryHash: req.Hash,
					Type:      "patch",
					Result:    json_patch.PatchArrayToString(patches),
				}

				responseData, _ := json.Marshal(updateResponse)
				gravel.natsConnection.Publish("gravel.mongo.watchquery."+req.ClientID, string(responseData))

				// Unlock after processing is complete
				newWatchQuery.Mutex.Unlock()
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
		dbService := gravel.dbServices[req.ClientID]
		if dbService == nil {
			log.Println("DBService not found for client ", req.ClientID, " - service already stopped")
			response := types.DebugMessage{
				ClientID: req.ClientID,
				Status:   "error",
				Error:    "DBService not found for client " + req.ClientID + " - service already stopped",
			}
			responseData, _ := json.Marshal(response)
			m.Respond(responseData)
			return
		}
		dbService.WatchQueriesMutex.RLock()
		watchQuery := dbService.WatchQueries[req.Hash]
		dbService.WatchQueriesMutex.RUnlock()

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

		dbService.WatchQueriesMutex.Lock()
		watchQuery.NumberOfConnections--

		// if the connection count is 0 we pull the watchqeuery from the map
		if watchQuery.NumberOfConnections == 0 {
			// Lock the watchquery to signal graceful shutdown
			watchQuery.Mutex.Lock()
			watchQuery.Stopped = true
			watchQuery.Mutex.Unlock()

			dbService.Connection.StopChangeStream()

			// Close the watchquery's update channel (the update goroutine will exit gracefully)
			close(watchQuery.UpdateChannel)
			delete(dbService.WatchQueries, req.Hash)
		}

		dbService.WatchQueriesMutex.Unlock()

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
