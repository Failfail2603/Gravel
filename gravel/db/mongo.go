package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoProvider struct {
	client        *mongo.Client
	changeStreams map[string]*mongo.ChangeStream
	stopChannels  map[string]chan struct{}
	mu            sync.RWMutex
	MongoUrl      string
	DatabaseName  string
}

// extractDatabaseFromURL extracts the database name from a MongoDB URL
func extractDatabaseFromURL(mongoURL string) string {
	parsedURL, err := url.Parse(mongoURL)
	if err != nil {
		log.Printf("Failed to parse MongoDB URL: %v", err)
		return "test" // fallback to default
	}

	// Remove leading slash and get the database name
	path := strings.TrimPrefix(parsedURL.Path, "/")
	if path == "" {
		return "test" // fallback to default if no database in URL
	}

	// Handle case where there might be additional path segments
	parts := strings.Split(path, "/")
	return parts[0]
}

// generateMongoProvider creates a new MongoDB provider instance
func generateMongoProvider(connectionRequest DatabaseConnectRequest) *MongoProvider {
	dbName := extractDatabaseFromURL(connectionRequest.MongoURL)
	return &MongoProvider{
		changeStreams: make(map[string]*mongo.ChangeStream),
		stopChannels:  make(map[string]chan struct{}),
		MongoUrl:      connectionRequest.MongoURL,
		DatabaseName:  dbName,
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

// Query runs a normal query against the database. Mainly used for initial data loading
func (m *MongoProvider) Query(collection string, query string, findOptionsStr string) []interface{} {
	if m.client == nil {
		log.Printf("MongoDB client not connected, cannot execute query")
		return nil
	}

	// Get the collection
	coll := m.client.Database(m.DatabaseName).Collection(collection)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Parse query string to bson.M
	var filter bson.M
	if query == "" {
		filter = bson.M{}
	} else {
		if err := json.Unmarshal([]byte(query), &filter); err != nil {
			log.Printf("Failed to parse query string: %v", err)
			return nil
		}
	}

	findOptions, err := parseFindOptions(findOptionsStr)
	if err != nil {
		log.Printf("Failed to parse query string: %v", err)
		return nil
	}

	// Execute the query
	cursor, err := coll.Find(ctx, filter, &findOptions)
	if err != nil {
		log.Printf("Failed to execute query: %v", err)
		return nil
	}
	defer cursor.Close(ctx)

	// Collect results
	var results []interface{}
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			log.Printf("Failed to decode document: %v", err)
			continue
		}
		results = append(results, doc)
	}

	if err := cursor.Err(); err != nil {
		log.Printf("Cursor error: %v", err)
		return nil
	}

	log.Printf("Query executed successfully, returned %d documents", len(results))
	return results
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
func (m *MongoProvider) StartChangeStream(dbUpdates chan DBChangeStreamEvent) {
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

func (m *MongoProvider) ParseChangeToJSONPatchString(event DBChangeStreamEvent) string {
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
func (m *MongoProvider) handleChangeStream(changeStream *mongo.ChangeStream, dbUpdates chan DBChangeStreamEvent, stopChan chan struct{}) {
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

			// Extract document ID safely
			var id string
			if documentKey, ok := changeEvent["documentKey"].(bson.M); ok {
				if docID, exists := documentKey["_id"]; exists {
					// Handle different ID types (ObjectID, string, etc.)
					switch v := docID.(type) {
					case string:
						id = v
					case primitive.ObjectID:
						id = v.Hex()
					default:
						id = fmt.Sprintf("%v", v)
					}
				}
			}

			// Extract collection safely
			var collection string
			if ns, ok := changeEvent["ns"].(bson.M); ok {
				if coll, exists := ns["coll"]; exists {
					collection = coll.(string)
				}
			}

			if id == "" {
				log.Printf("Could not extract document ID from change event for %s", mongoUrl)
				continue
			}

			test, _ := json.MarshalIndent(changeEvent, "", "  ")
			log.Println("ChangeEvent", string(test))

			// Create and send change event
			event := DBChangeStreamEvent{
				Database:   mongoUrl,
				Operation:  operation,
				ID:         id,
				Collection: collection,
				Document:   changeEvent,
				Timestamp:  time.Now(),
			}

			select {
			case dbUpdates <- event:
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

func parseFindOptions(findOptionsStr string) (options.FindOptions, error) {
	findOptions := options.Find()
	if findOptionsStr != "" {
		var optionsMap map[string]interface{}
		if err := json.Unmarshal([]byte(findOptionsStr), &optionsMap); err != nil {
			log.Printf("Failed to parse options string: %v", err)
		} else {
			if limit, ok := optionsMap["limit"].(float64); ok {
				findOptions.SetLimit(int64(limit))
			}
			if skip, ok := optionsMap["skip"].(float64); ok {
				findOptions.SetSkip(int64(skip))
			}
			if projection, ok := optionsMap["projection"].(map[string]interface{}); ok {

				findOptions.SetProjection(projection)
			}
			if sort, ok := optionsMap["sort"].(map[string]interface{}); ok {
				findOptions.SetSort(sort)
			}
		}
	}
	return *findOptions, nil
}

// flattenObject converts a projection object to a slice of dot-notated field paths
func flattenObject(projection interface{}) []string {
	if projection == nil {
		return []string{}
	}

	var fields []string

	switch proj := projection.(type) {
	case map[string]interface{}:
		for key, value := range proj {
			fields = append(fields, flattenObjectRecursive(key, value, "")...)
		}
	case bson.M:
		for key, value := range proj {
			fields = append(fields, flattenObjectRecursive(key, value, "")...)
		}
	}

	return fields
}

// flattenObjectRecursive recursively flattens nested projection objects
func flattenObjectRecursive(key string, value interface{}, prefix string) []string {
	var fields []string

	fullKey := key
	if prefix != "" {
		fullKey = prefix + "." + key
	}

	switch v := value.(type) {
	case map[string]interface{}:
		// If it's a nested object, recurse into it
		for nestedKey, nestedValue := range v {
			fields = append(fields, flattenObjectRecursive(nestedKey, nestedValue, fullKey)...)
		}
	case bson.M:
		// If it's a nested bson.M, recurse into it
		for nestedKey, nestedValue := range v {
			fields = append(fields, flattenObjectRecursive(nestedKey, nestedValue, fullKey)...)
		}
	default:
		// For primitive values (1, 0, true, false), add the field path
		fields = append(fields, fullKey)
	}

	return fields
}

func (m *MongoProvider) GetDestructuredQueryInformation(query WatchQueryRequest) (DestructuredQueryInformation, error) {

	var queryInformation = DestructuredQueryInformation{}

	findOptions, err := parseFindOptions(query.Options)
	if err != nil {
		return DestructuredQueryInformation{}, err
	}

	var filter bson.M
	if query.Query == "" {
		filter = bson.M{}
	} else {
		if err := json.Unmarshal([]byte(query.Query), &filter); err != nil {
			log.Printf("Failed to parse query string: %v", err)
			return DestructuredQueryInformation{}, err
		}
	}

	// Extract relevant fields from projection
	queryInformation.ProjectionFields = flattenObject(findOptions.Projection)

	// add _id if not present so we track it
	if !slices.Contains(queryInformation.ProjectionFields, "_id") {
		queryInformation.ProjectionFields = append(queryInformation.ProjectionFields, "_id")
	}

	// Extract relevant fields from filter
	queryInformation.FilterFields = flattenObject(filter)

	// Extract relevant fields from sort
	queryInformation.SortFields = flattenObject(findOptions.Sort)

	return queryInformation, nil
}
