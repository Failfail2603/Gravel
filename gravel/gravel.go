package main

import (
	"encoding/json"
	"fmt"
	"gravel/db"
	"gravel/env"
	"gravel/json_patch"
	"gravel/nats_server"
	"gravel/relevant_changes"
	"gravel/types"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

var clientKeepAliveInterval = time.Duration(env.GetEnvIntOrDefault("CLIENT_KEEPALIVE_INTERVAL_SECONDS", 30)) * time.Second

var staleClientTimeout = time.Duration(env.GetEnvIntOrDefault("CLIENT_STALE_TIMEOUT_SECONDS", 60)) * time.Second

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
	dbServices               map[string]*db.DBService
	dbServicesMutex          sync.RWMutex
	clientLastKeepAlive      map[string]time.Time
	clientLastKeepAliveMutex sync.RWMutex
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

	}
}

func generateGravelServer(natsConnection *nats_server.NatsConnection) *GravelServer {
	return &GravelServer{
		natsConnection:      natsConnection,
		dbServices:          make(map[string]*db.DBService),
		clientLastKeepAlive: make(map[string]time.Time),
	}
}

func (gravel *GravelServer) removeClient(clientID string) {
	gravel.dbServicesMutex.Lock()
	dbService := gravel.dbServices[clientID]
	if dbService == nil {
		gravel.dbServicesMutex.Unlock()
		return
	}
	delete(gravel.dbServices, clientID)
	gravel.dbServicesMutex.Unlock()

	gravel.clientLastKeepAliveMutex.Lock()
	delete(gravel.clientLastKeepAlive, clientID)
	gravel.clientLastKeepAliveMutex.Unlock()

	dbService.WatchQueriesMutex.Lock()
	for hash, watchQuery := range dbService.WatchQueries {
		watchQuery.Mutex.Lock()
		watchQuery.Stopped = true
		watchQuery.Mutex.Unlock()
		watchQuery.StopDrainer()
		delete(dbService.WatchQueries, hash)
	}
	dbService.WatchQueriesMutex.Unlock()

	dbService.Connection.StopChangeStream()
	if err := dbService.Connection.Disconnect(); err != nil {
		log.Printf("Failed to disconnect DB service for stale client %s: %v", clientID, err)
	}

	log.Printf("Removed stale client %s and cleaned up its watchqueries", clientID)
}

func (gravel *GravelServer) startClientKeepAliveLoop() {
	ticker := time.NewTicker(clientKeepAliveInterval)
	go func() {
		for range ticker.C {
			now := time.Now()

			gravel.clientLastKeepAliveMutex.RLock()
			staleClientIDs := make([]string, 0)
			for clientID, lastKeepAlive := range gravel.clientLastKeepAlive {
				if now.Sub(lastKeepAlive) > staleClientTimeout {
					staleClientIDs = append(staleClientIDs, clientID)
				}
			}
			gravel.clientLastKeepAliveMutex.RUnlock()

			for _, clientID := range staleClientIDs {
				log.Printf("Client %s expired after %s without a keepalive", clientID, staleClientTimeout)
				gravel.removeClient(clientID)
			}
		}
	}()
}

func (gravel *GravelServer) runQuery(dbService *db.DBService, req types.WatchQueryRequest, publishChannel string) (*types.WatchQueryResponse, []types.Document, error) {
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

	// Publish the results to the specified channel
	gravel.natsConnection.Publish(publishChannel, string(responseJSON))

	return &response, results, nil
}

func (gravel *GravelServer) StartListening() {
	gravel.startClientKeepAliveLoop()

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
		gravel.dbServicesMutex.RLock()
		_, exists := gravel.dbServices[req.ClientID]
		gravel.dbServicesMutex.RUnlock()
		if !exists {

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
			gravel.dbServicesMutex.Lock()
			gravel.dbServices[req.ClientID] = service
			gravel.dbServicesMutex.Unlock()

			gravel.clientLastKeepAliveMutex.Lock()
			gravel.clientLastKeepAlive[req.ClientID] = time.Now()
			gravel.clientLastKeepAliveMutex.Unlock()
		}

		// Send success response
		response := types.DatabaseConnectResponse{
			Status:   "connected",
			Database: req.MongoURL,
		}
		responseData, _ := json.Marshal(response)
		m.Respond(responseData)
	})

	gravel.natsConnection.SubscribeTo("gravel.keepalive", func(m *nats.Msg) {
		var req types.KeepAliveRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			m.Respond([]byte(`{"status":"error"}`))
			return
		}

		gravel.dbServicesMutex.RLock()
		_, exists := gravel.dbServices[req.ClientID]
		gravel.dbServicesMutex.RUnlock()
		if !exists {
			log.Printf("Received client keepalive for unknown/stale client %s", req.ClientID)
			response := types.KeepAliveResponse{
				Status: "stale",
			}
			responseData, _ := json.Marshal(response)
			m.Respond(responseData)
			return
		}

		gravel.clientLastKeepAliveMutex.Lock()
		gravel.clientLastKeepAlive[req.ClientID] = time.Now()
		gravel.clientLastKeepAliveMutex.Unlock()

		log.Printf("Received client keepalive from %s", req.ClientID)

		response := types.KeepAliveResponse{
			Status: "ok",
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

		gravel.dbServicesMutex.RLock()
		var dbService *db.DBService = gravel.dbServices[req.ClientID]
		gravel.dbServicesMutex.RUnlock()

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

		publishChannel := "gravel.mongo.initial." + req.ClientID

		// Execute initial query
		queryResult, documents, err := gravel.runQuery(dbService, req, publishChannel)
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

		// Decrement the query count for the inital query
		dbService.Connection.DecrementQueryCount()

		// check if the watchquery already exists with the hash. Different clients can have the same watchquery.
		// we need to ensure that all unique clients in the
		dbService.WatchQueriesMutex.RLock()
		watchQuery := dbService.WatchQueries[req.Hash]
		dbService.WatchQueriesMutex.RUnlock()

		// server-side pooling: if the hash already exists, another client is already watching this query.
		// increment the connection count so multiple clients share the same watchquery.
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
		newWatchQuery.UpdateChannel = make(chan types.DBChangeStreamEvent, 1000)
		// Create ready channel to signal when initial query result has been sent
		newWatchQuery.ReadyChan = make(chan struct{})
		newWatchQuery.StartDrainer()
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
						wq.EnqueueUpdate(update)
					}
					dbService.WatchQueriesMutex.RUnlock()
				}
				// Stop all watchquery drainers (flushes remaining items and closes UpdateChannel)
				dbService.WatchQueriesMutex.RLock()
				for _, wq := range dbService.WatchQueries {
					wq.StopDrainer()
				}
				dbService.WatchQueriesMutex.RUnlock()
			}()
		}

		go func() {
			// Wait until initial query result has been sent to client before processing updates
			<-newWatchQuery.ReadyChan

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

				if len(patches) > 0 {
					// send the update to the client
					updateResponse := types.WatchQueryResponse{
						QueryHash: req.Hash,
						Type:      "patch",
						Result:    json_patch.PatchArrayToString(patches),
					}

					responseData, _ := json.Marshal(updateResponse)
					gravel.natsConnection.Publish("gravel.mongo.watchquery."+req.ClientID, string(responseData))
				}

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

		// Signal that watchquery is ready - this unblocks the update processing goroutine
		close(newWatchQuery.ReadyChan)

	})

	gravel.natsConnection.SubscribeTo("gravel.metrics.querycount", func(m *nats.Msg) {
		log.Println("Received gravel.metrics.querycount request")
		var req struct {
			ClientID string `json:"clientID"`
		}
		if err := json.Unmarshal(m.Data, &req); err != nil {
			m.Respond([]byte(`{"error":"invalid request"}`))
			return
		}

		gravel.dbServicesMutex.RLock()
		dbService := gravel.dbServices[req.ClientID]
		gravel.dbServicesMutex.RUnlock()
		if dbService == nil {
			m.Respond([]byte(`{"count":0}`))
			return
		}

		// Get the count and reset it; subtract 1 for the initial query
		count := dbService.Connection.GetAndResetQueryCount()

		response := fmt.Sprintf(`{"count":%d}`, count)
		m.Respond([]byte(response))
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

		// only fully stop the watchquery when no more clients are using it
		if watchQuery.NumberOfConnections == 0 {
			watchQuery.Mutex.Lock()
			watchQuery.Stopped = true
			watchQuery.Mutex.Unlock()

			dbService.Connection.StopChangeStream()

			// Stop the drainer (flushes remaining items and closes UpdateChannel)
			watchQuery.StopDrainer()
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
