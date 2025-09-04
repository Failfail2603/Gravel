package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
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
	MongoUrl      string
}

// generateMongoProvider creates a new MongoDB provider instance
func generateMongoProvider(connectionRequest DatabaseConnectRequest) *MongoProvider {
	return &MongoProvider{
		changeStreams: make(map[string]*mongo.ChangeStream),
		stopChannels:  make(map[string]chan struct{}),
		MongoUrl:      connectionRequest.MongoURL,
	}
}

// Connect establishes a connection to MongoDB
func (m *MongoProvider) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	options := options.Client().ApplyURI(m.MongoUrl).SetReplicaSet("rs0").SetDirect(true)

	log.Println("Connecting to MongoDB...", m.MongoUrl)
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
func (m *MongoProvider) StartChangeStream(dbUpdates chan string) {
	if m.client == nil {
		log.Printf("MongoDB client not connected, cannot start change stream for %s", m.MongoUrl)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop existing change stream if it exists
	if _, exists := m.changeStreams[m.MongoUrl]; exists {
		m.stopChangeStreamInternal(m.MongoUrl)
	}

	// Create change stream options
	opts := options.ChangeStream()

	// Start change stream
	changeStream, err := m.client.Watch(context.Background(), mongo.Pipeline{}, opts)
	if err != nil {
		log.Printf("Failed to start change stream for %s: %v", m.MongoUrl, err)
		return
	}

	stopChan := make(chan struct{})
	m.changeStreams[m.MongoUrl] = changeStream
	m.stopChannels[m.MongoUrl] = stopChan

	// Start goroutine to handle change stream events
	go m.handleChangeStream(changeStream, dbUpdates, stopChan)

	log.Printf("Started change stream for %s", m.MongoUrl)
}

// StopChangeStream stops monitoring changes for a specific database and collection
func (m *MongoProvider) StopChangeStream() {
	key := m.MongoUrl

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

func parseChangeToJSONPatchString(event DBChangeStreamEvent) string {
	log.Println("Event", event)
	var patches []map[string]interface{}

	// Convert the document to a map for easier processing
	docBytes, err := json.Marshal(event.Document)
	if err != nil {
		log.Printf("Failed to marshal document: %v", err)
		return "[]"
	}

	var docMap map[string]interface{}
	if err := json.Unmarshal(docBytes, &docMap); err != nil {
		log.Printf("Failed to unmarshal document: %v", err)
		return "[]"
	}

	switch strings.ToLower(event.Operation) {
	case "insert":
		// For insert operations, add the entire document
		if fullDoc, ok := docMap["fullDocument"].(map[string]interface{}); ok {
			patches = append(patches, map[string]interface{}{
				"op":    "add",
				"path":  "",
				"value": fullDoc,
			})
		} else {
			// If no fullDocument, use the entire document
			patches = append(patches, map[string]interface{}{
				"op":    "add",
				"path":  "",
				"value": docMap,
			})
		}

	case "update":
		// For update operations, process the updateDescription
		if updateDesc, ok := docMap["updateDescription"].(map[string]interface{}); ok {
			// Handle updated fields
			if updatedFields, ok := updateDesc["updatedFields"].(map[string]interface{}); ok {
				for field, value := range updatedFields {
					patches = append(patches, map[string]interface{}{
						"op":    "replace",
						"path":  "/" + strings.ReplaceAll(field, ".", "/"),
						"value": value,
					})
				}
			}

			// Handle removed fields
			if removedFields, ok := updateDesc["removedFields"].([]interface{}); ok {
				for _, field := range removedFields {
					if fieldStr, ok := field.(string); ok {
						patches = append(patches, map[string]interface{}{
							"op":   "remove",
							"path": "/" + strings.ReplaceAll(fieldStr, ".", "/"),
						})
					}
				}
			}

			// Handle truncated arrays
			if truncatedArrays, ok := updateDesc["truncatedArrays"].([]interface{}); ok {
				for _, arrayInfo := range truncatedArrays {
					if arrayMap, ok := arrayInfo.(map[string]interface{}); ok {
						if field, ok := arrayMap["field"].(string); ok {
							if newSize, ok := arrayMap["newSize"].(float64); ok {
								patches = append(patches, map[string]interface{}{
									"op":    "replace",
									"path":  "/" + strings.ReplaceAll(field, ".", "/") + "/length",
									"value": int(newSize),
								})
							}
						}
					}
				}
			}
		}

	case "replace":
		// For replace operations, replace the entire document
		if fullDoc, ok := docMap["fullDocument"].(map[string]interface{}); ok {
			patches = append(patches, map[string]interface{}{
				"op":    "replace",
				"path":  "",
				"value": fullDoc,
			})
		}

	case "delete":
		// For delete operations, remove the entire document
		patches = append(patches, map[string]interface{}{
			"op":   "remove",
			"path": "",
		})

	default:
		log.Printf("Unknown operation type: %s", event.Operation)
		return "[]"
	}

	// Convert patches to JSON string
	patchBytes, err := json.Marshal(patches)
	if err != nil {
		log.Printf("Failed to marshal patches: %v", err)
		return "[]"
	}

	return string(patchBytes)
}

// handleChangeStream processes change stream events
func (m *MongoProvider) handleChangeStream(changeStream *mongo.ChangeStream, dbUpdates chan string, stopChan chan struct{}) {
	// Capture the MongoUrl at the start to avoid accessing it after potential cleanup
	mongoUrl := m.MongoUrl

	defer func() {
		// as we have any async context we need to recover if the channel panics when mongo is closed while executing
		// should be normal behavior
		recover()
	}()

	for {
		select {
		case <-stopChan:
			log.Printf("Change stream for %s stopped", mongoUrl)
			return
		default:
			// Check if change stream is still valid before using it
			if changeStream == nil {
				log.Printf("Change stream for %s is nil, stopping", mongoUrl)
				return
			}

			if !changeStream.Next(context.Background()) {
				if err := changeStream.Err(); err != nil {
					log.Printf("Change stream error for %s: %v", mongoUrl, err)
				}
				return
			}

			var changeEvent bson.M
			if err := changeStream.Decode(&changeEvent); err != nil {
				log.Printf("Failed to decode change event for %s: %v", mongoUrl, err)
				continue
			}

			// Extract operation type
			operation, ok := changeEvent["operationType"].(string)
			if !ok {
				log.Printf("Invalid operation type in change event for %s", mongoUrl)
				continue
			}

			// Create and send change event
			event := DBChangeStreamEvent{
				Database:  mongoUrl,
				Operation: operation,
				Document:  changeEvent,
				Timestamp: time.Now(),
			}

			select {
			case dbUpdates <- parseChangeToJSONPatchString(event):
				// log.Printf("Sent change event for %s: %s", mongoUrl, operation)
			case <-stopChan:
				log.Printf("Change stream for %s stopped while sending event", mongoUrl)
				return
			default:
				log.Printf("Channel blocked, dropping change event for %s", mongoUrl)
			}
		}
	}
}
