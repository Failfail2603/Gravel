package main

import (
	"gravel/env"
	"gravel/nats_server"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	log.Println("Starting Gravel")

	// if the .env file is not found we warn the user as we are using internal standard values.
	// Standard values are the standard values of the different services like nats or mongo
	env.WarnIfEnvFileNotFound()

	// Setup channel for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Initialize Server
	natsConnection, err := nats_server.StartNatsServer()
	if err != nil {
		log.Fatalf("Failed to start NATS server: %v", err)
	}

	// Initialize the Gravel server
	gravelServer := NewGravelServer(natsConnection)

	log.Println("Gravel server started successfully")

	gravelServer.StartListening()

	<-c
	log.Println("Shutting down server...")

}

/**

func (s *GravelServer) setupMessageHandlers() {
	// Handle database connection requests
	s.natsConn.Subscribe("database.connect", func(m *nats.Msg) {
		var req DatabaseRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			log.Printf("Failed to unmarshal database request: %v", err)
			return
		}

		log.Printf("Client %s requesting database: %s", req.ClientID, req.Database)

		// Only MongoDB is supported for now
		if req.Database != "mongodb" {
			response := map[string]string{
				"error": "Only MongoDB is supported",
			}
			responseData, _ := json.Marshal(response)
			m.Respond(responseData)
			return
		}

		// Start change stream for this client
		go s.startChangeStream(req.ClientID)

		// Send success response
		response := map[string]string{
			"status":   "connected",
			"database": req.Database,
		}
		responseData, _ := json.Marshal(response)
		m.Respond(responseData)
	})

	// Handle client disconnect
	s.natsConn.Subscribe("client.disconnect", func(m *nats.Msg) {
		var req map[string]string
		if err := json.Unmarshal(m.Data, &req); err != nil {
			log.Printf("Failed to unmarshal disconnect request: %v", err)
			return
		}

		clientID := req["client_id"]
		log.Printf("Client %s disconnecting", clientID)

		// Unsubscribe from change stream
		if sub, exists := s.subscribers[clientID]; exists {
			sub.Unsubscribe()
			delete(s.subscribers, clientID)
		}
	})
}

func (s *GravelServer) startChangeStream(clientID string) {
	db := s.mongoClient.Database(s.database)

	// Create change stream options
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	// Start change stream on all collections
	changeStream, err := db.Watch(context.Background(), mongo.Pipeline{}, opts)
	if err != nil {
		log.Printf("Failed to start change stream for client %s: %v", clientID, err)
		return
	}
	defer changeStream.Close(context.Background())

	log.Printf("Started change stream for client %s", clientID)

	// Subscribe to change stream events
	subject := fmt.Sprintf("changestream.%s", clientID)

	for changeStream.Next(context.Background()) {
		var changeEvent bson.M
		if err := changeStream.Decode(&changeEvent); err != nil {
			log.Printf("Failed to decode change event for client %s: %v", clientID, err)
			continue
		}

		// Create structured event
		event := ChangeStreamEvent{
			Database:  s.database,
			Operation: fmt.Sprintf("%v", changeEvent["operationType"]),
			Document:  changeEvent["fullDocument"],
			Timestamp: time.Now(),
		}

		// Marshal and publish to NATS
		eventData, err := json.Marshal(event)
		if err != nil {
			log.Printf("Failed to marshal change event for client %s: %v", clientID, err)
			continue
		}

		if err := s.natsConn.Publish(subject, eventData); err != nil {
			log.Printf("Failed to publish change event for client %s: %v", clientID, err)
			continue
		}

		log.Printf("Published change event to %s: %s", subject, event.Operation)
	}

	if err := changeStream.Err(); err != nil {
		log.Printf("Change stream error for client %s: %v", clientID, err)
	}
}

func (s *GravelServer) shutdown() {

	// Close MongoDB connection
	if s.mongoClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.mongoClient.Disconnect(ctx)
	}

	log.Println("Server shutdown complete")
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

*/
