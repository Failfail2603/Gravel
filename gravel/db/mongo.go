package db

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoProvider struct {
	client        *mongo.Client
	changeStreams map[string]*mongo.ChangeStream
	stopChannels  map[string]chan struct{}
	mu            sync.RWMutex
}

// NewMongoProvider creates a new MongoDB provider instance
func NewMongoProvider() *MongoProvider {
	return &MongoProvider{
		changeStreams: make(map[string]*mongo.ChangeStream),
		stopChannels:  make(map[string]chan struct{}),
	}
}

// Connect establishes a connection to MongoDB
func (m *MongoProvider) connect() error {
	mongoURI := os.Getenv("MONGODB_URI")
	if mongoURI == "" {
		return fmt.Errorf("MONGODB_URI environment variable is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	options := options.Client().ApplyURI(mongoURI).SetReplicaSet("rs0").SetDirect(true)

	log.Println("Connecting to MongoDB...", mongoURI)
	client, err := mongo.Connect(ctx, options)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping the database to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	m.client = client
	log.Println("Connected to MongoDB successfully")
	return nil
}

// Disconnect closes the MongoDB connection
func (m *MongoProvider) Disconnect() error {
	if m.client == nil {
		return nil
	}

	// Stop all active change streams
	m.mu.Lock()
	for key := range m.changeStreams {
		m.stopChangeStreamInternal(key)
	}
	m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to disconnect from MongoDB: %w", err)
	}

	log.Println("Disconnected from MongoDB")
	return nil
}

// StartChangeStream starts monitoring changes for a specific database and collection
func (m *MongoProvider) StartChangeStream(db string, collection string, dbUpdates chan DBChangeStreamEvent) {
	if m.client == nil {
		log.Printf("MongoDB client not connected, cannot start change stream for %s.%s", db, collection)
		return
	}

	key := fmt.Sprintf("%s.%s", db, collection)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop existing change stream if it exists
	if _, exists := m.changeStreams[key]; exists {
		m.stopChangeStreamInternal(key)
	}

	// Create change stream options
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	// Start change stream
	coll := m.client.Database(db).Collection(collection)
	changeStream, err := coll.Watch(context.Background(), mongo.Pipeline{}, opts)
	if err != nil {
		log.Printf("Failed to start change stream for %s.%s: %v", db, collection, err)
		return
	}

	stopChan := make(chan struct{})
	m.changeStreams[key] = changeStream
	m.stopChannels[key] = stopChan

	// Start goroutine to handle change stream events
	go m.handleChangeStream(key, db, collection, changeStream, dbUpdates, stopChan)

	log.Printf("Started change stream for %s.%s", db, collection)
}

// StopChangeStream stops monitoring changes for a specific database and collection
func (m *MongoProvider) StopChangeStream(db string, collection string) {
	key := fmt.Sprintf("%s.%s", db, collection)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopChangeStreamInternal(key)
}

// stopChangeStreamInternal stops a change stream without locking (internal use)
func (m *MongoProvider) stopChangeStreamInternal(key string) {
	if stopChan, exists := m.stopChannels[key]; exists {
		close(stopChan)
		delete(m.stopChannels, key)
	}

	if changeStream, exists := m.changeStreams[key]; exists {
		changeStream.Close(context.Background())
		delete(m.changeStreams, key)
		log.Printf("Stopped change stream for %s", key)
	}
}

// handleChangeStream processes change stream events
func (m *MongoProvider) handleChangeStream(key, db, collection string, changeStream *mongo.ChangeStream, dbUpdates chan DBChangeStreamEvent, stopChan chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Change stream handler for %s panicked: %v", key, r)
		}
	}()

	for {
		select {
		case <-stopChan:
			log.Printf("Change stream for %s stopped", key)
			return
		default:
			if !changeStream.Next(context.Background()) {
				if err := changeStream.Err(); err != nil {
					log.Printf("Change stream error for %s: %v", key, err)
				}
				return
			}

			var changeEvent bson.M
			if err := changeStream.Decode(&changeEvent); err != nil {
				log.Printf("Failed to decode change event for %s: %v", key, err)
				continue
			}

			// Extract operation type
			operation, ok := changeEvent["operationType"].(string)
			if !ok {
				log.Printf("Invalid operation type in change event for %s", key)
				continue
			}

			// Extract document (fullDocument for insert/update, documentKey for delete)
			var document interface{}
			if fullDoc, exists := changeEvent["fullDocument"]; exists {
				document = fullDoc
			} else if docKey, exists := changeEvent["documentKey"]; exists {
				document = docKey
			} else {
				document = changeEvent
			}

			// Create and send change event
			event := DBChangeStreamEvent{
				Database:  db,
				Operation: operation,
				Document:  document,
				Timestamp: time.Now(),
			}

			select {
			case dbUpdates <- event:
				log.Printf("Sent change event for %s: %s", key, operation)
			case <-stopChan:
				log.Printf("Change stream for %s stopped while sending event", key)
				return
			default:
				log.Printf("Channel blocked, dropping change event for %s", key)
			}
		}
	}
}
