package mongo

import (
	"context"
	"fmt"
	"gravel/types"
	"log"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	queryCount    int64
}

// IncrementQueryCount atomically increments the query counter
func (m *MongoProvider) IncrementQueryCount() {
	atomic.AddInt64(&m.queryCount, 1)
}

// IncrementQueryCount atomically increments the query counter
func (m *MongoProvider) DecrementQueryCount() {
	atomic.AddInt64(&m.queryCount, -1)
}

// GetAndResetQueryCount atomically reads and resets the query counter
func (m *MongoProvider) GetAndResetQueryCount() int64 {
	return atomic.SwapInt64(&m.queryCount, 0)
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

// NewMongoProvider creates a new MongoDB provider instance
func NewMongoProvider(mongoURL string) *MongoProvider {
	dbName := extractDatabaseFromURL(mongoURL)
	return &MongoProvider{
		changeStreams: make(map[string]*mongo.ChangeStream),
		stopChannels:  make(map[string]chan struct{}),
		MongoUrl:      mongoURL,
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

	// Enable pre and post images for change streams at cluster level
	command := bson.D{
		{Key: "setClusterParameter", Value: bson.D{
			{Key: "changeStreamOptions", Value: bson.D{
				{Key: "preAndPostImages", Value: bson.D{
					{Key: "expireAfterSeconds", Value: 10},
				}},
			}},
		}},
	}

	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, command).Decode(&result); err != nil {
		log.Printf("Warning: Failed to set changeStreamOptions cluster parameter: %v", err)
		log.Println("Pre and post images may not be available. This is expected if not running as a replica set.")
	} else {
		log.Println("Successfully configured pre and post images for change streams at cluster level")

		// Enable pre and post images on all collections in the database
		if err := m.enablePrePostImagesOnCollections(client, ctx); err != nil {
			log.Printf("Warning: Failed to enable pre and post images on collections: %v", err)
		}
	}

	log.Println("Connected to MongoDB successfully")
	return nil
}

// enablePrePostImagesOnCollections enables pre and post images on all collections in the database
func (m *MongoProvider) enablePrePostImagesOnCollections(client *mongo.Client, ctx context.Context) error {
	// Get list of collections in the database
	db := client.Database(m.DatabaseName)
	collections, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	log.Printf("Enabling pre and post images on %d collections in database '%s'", len(collections), m.DatabaseName)

	// Enable pre and post images on each collection
	for _, collName := range collections {
		// Skip system collections
		if strings.HasPrefix(collName, "system.") {
			continue
		}

		collModCmd := bson.D{
			{Key: "collMod", Value: collName},
			{Key: "changeStreamPreAndPostImages", Value: bson.D{
				{Key: "enabled", Value: true},
			}},
		}

		var collModResult bson.M
		if err := db.RunCommand(ctx, collModCmd).Decode(&collModResult); err != nil {
			log.Printf("Warning: Failed to enable pre/post images on collection '%s': %v", collName, err)
			continue
		}
		log.Printf("Enabled pre and post images on collection '%s'", collName)
	}

	return nil
}

// Query runs a normal query against the database. Mainly used for initial data loading
func (m *MongoProvider) Query(collection string, query string, findOptionsStr string) []types.Document {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Create a minimal event for non-snapshot queries (no clusterTime)
	event := &types.DBChangeStreamEvent{
		UpdateCache: make(map[int]types.Document),
	}
	return m.QueryWithEvent(ctx, event, collection, query, findOptionsStr)
}

// QueryWithEvent runs a query with optional snapshot read concern at a specific cluster time
// If event.ClusterTime is provided, the query will use snapshot read concern at that specific point in time
func (m *MongoProvider) QueryWithEvent(ctx context.Context, event *types.DBChangeStreamEvent, collection string, query string, findOptionsStr string) []types.Document {
	m.IncrementQueryCount()

	if m.client == nil {
		log.Printf("MongoDB client not connected, cannot execute query")
		return nil
	}

	if event == nil {
		log.Printf("No event provided, cannot execute query")
		return nil
	}

	// Get the collection
	coll := m.client.Database(m.DatabaseName).Collection(collection)

	// Parse query string to bson.M

	var filter bson.M
	if query == "" {
		filter = bson.M{}
	} else {
		parsed, err := parseQueryString(query)
		if err != nil {
			log.Printf("Failed to parse query string: %v", err)
			return nil
		}
		filter = parsed
	}

	findOptions, err := parseFindOptionsString(findOptionsStr)
	if err != nil {
		log.Printf("Failed to parse query string: %v", err)
		return nil
	}

	// Execute the query

	var cursor *mongo.Cursor

	// If ClusterTime is provided, use RunCommand with custom read concern including atClusterTime
	// This ensures all reads happen at the exact same point in time as the change event
	if event.ClusterTime != nil {
		// Construct find command with snapshot read concern at specific cluster time
		cmd := bson.D{
			{Key: "find", Value: collection},
			{Key: "filter", Value: filter},
			{Key: "readConcern", Value: bson.D{
				{Key: "level", Value: "snapshot"},
				{Key: "atClusterTime", Value: event.ClusterTime},
			}},
		}

		// Add findOptions to command
		if findOptions.Limit != nil {
			cmd = append(cmd, bson.E{Key: "limit", Value: *findOptions.Limit})
		}
		if findOptions.Skip != nil {
			cmd = append(cmd, bson.E{Key: "skip", Value: *findOptions.Skip})
		}
		if findOptions.Sort != nil {
			cmd = append(cmd, bson.E{Key: "sort", Value: findOptions.Sort})
		}
		if findOptions.Projection != nil {
			cmd = append(cmd, bson.E{Key: "projection", Value: findOptions.Projection})
		}

		// Execute the command
		var result bson.M
		err = m.client.Database(m.DatabaseName).RunCommand(ctx, cmd).Decode(&result)
		if err != nil {
			log.Printf("Failed to execute snapshot query with atClusterTime: %v", err)
			return nil
		}

		// Extract cursor from result
		if cursorDoc, ok := result["cursor"].(bson.M); ok {
			if firstBatch, ok := cursorDoc["firstBatch"].(bson.A); ok {
				// Convert firstBatch to documents
				var results []types.Document
				for _, doc := range firstBatch {
					if docMap, ok := doc.(bson.M); ok {
						results = append(results, types.Document(docMap))
					}
				}

				return results
			}
		}
		log.Printf("Failed to extract results from snapshot query")
		return nil
	}

	// Regular query without snapshot read concern
	cursor, err = coll.Find(ctx, filter, &findOptions)
	if err != nil {
		log.Printf("Failed to execute query: %v", err)
		return nil
	}
	defer cursor.Close(ctx)

	// Collect results
	var results []types.Document
	docCount := 0
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			log.Printf("Failed to decode document: %v", err)
			continue
		}
		results = append(results, types.Document(doc))
		docCount++
	}

	if err := cursor.Err(); err != nil {
		log.Printf("Cursor error: %v", err)
		return nil
	}

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
func (m *MongoProvider) StartChangeStream(dbUpdates chan types.DBChangeStreamEvent) {
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
	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetFullDocumentBeforeChange(options.WhenAvailable)

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

// handleChangeStream processes change stream events
func (m *MongoProvider) handleChangeStream(changeStream *mongo.ChangeStream, dbUpdates chan types.DBChangeStreamEvent, stopChan chan struct{}) {
	// Capture the MongoUrl at the start to avoid accessing it after potential cleanup
	mongoUrl := m.MongoUrl

	defer func() {
		// as we have any async context we need to recover if the channel panics when mongo is closed while executing
		// should be normal behavior
		recover()
	}()

	// currentBatch := []types.DBChangeStreamEvent{}
	// currentBatchTime := primitive.Timestamp{}

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

			// if there is no change incoming return and wait for next one
			// as next blocks while not receiving a change we can return here and wait for it with the background context
			if !changeStream.Next(context.Background()) {
				if err := changeStream.Err(); err != nil {
					log.Printf("Change stream error for %s: %v", mongoUrl, err)
				}
				return
			}

			// Decode the change event
			var changeEvent bson.M
			if err := changeStream.Decode(&changeEvent); err != nil {
				log.Printf("Failed to decode change event for %s: %v", mongoUrl, err)
				continue
			}

			// Extract clusterTime from the change event for snapshot reads
			var clusterTime *primitive.Timestamp
			if ct, ok := changeEvent["clusterTime"].(primitive.Timestamp); ok {
				clusterTime = &ct
			} else {
				log.Printf("Warning: Failed to extract clusterTime from change event for %s", mongoUrl)
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

			// Extract full document (available for insert, update, replace operations)
			var fullDocument interface{}
			if fullDoc, exists := changeEvent["fullDocument"]; exists {

				fullDocument = fullDoc

			}

			// Extract full document before change (pre-image, available when enabled)
			var fullDocumentBeforeChange interface{}
			if preImage, exists := changeEvent["fullDocumentBeforeChange"]; exists {

				fullDocumentBeforeChange = preImage

			}

			// Extract clusterTime timestamp from the change event
			var timestamp time.Time
			if clusterTime, exists := changeEvent["clusterTime"]; exists {
				if ts, ok := clusterTime.(primitive.Timestamp); ok {
					// Convert BSON timestamp to Unix time (T is the seconds component)
					timestamp = time.Unix(int64(ts.T), 0)
				} else {
					// Fallback to current time if conversion fails
					log.Printf("Failed to convert clusterTime to time for %s", mongoUrl)
					timestamp = time.Now()
				}
			} else {
				// Fallback to current time if clusterTime is not present
				log.Printf("No clusterTime found in change event for %s", mongoUrl)
				timestamp = time.Now()
			}

			// Create and send change event
			event := types.DBChangeStreamEvent{
				Database:                 mongoUrl,
				Operation:                operation,
				ID:                       id,
				Collection:               collection,
				FullUpdate:               changeEvent,
				FullDocument:             fullDocument,
				FullDocumentBeforeChange: fullDocumentBeforeChange,
				Timestamp:                timestamp,
				ClusterTime:              clusterTime,
				UpdateCache:              make(map[int]types.Document),
			}

			select {
			case dbUpdates <- event:
				// log.Printf("Sent change event for %s: %s", mongoUrl, operation)
			case <-stopChan:
				log.Printf("Change stream for %s stopped while sending event", mongoUrl)
				return
			}
		}
	}
}

// TestFilterWithDocument tests if a MongoDB filter matches a given document
// using an aggregation pipeline with $documents and $match stages.
// This approach creates an ephemeral document in the pipeline without persisting it.
// Returns true if the document matches the filter, false otherwise.
//
// Parameters:
//   - filterJSON: MongoDB filter as a JSON string (e.g., `{"name": "test"}` or `{"name": {"$regex": "susan", "$options": "i"}}`)
//   - document: The document to test (must be a map or struct)
func (m *MongoProvider) TestFilterWithDocument(filterJSON string, document types.Document) (bool, error) {
	if m.client == nil {
		return false, fmt.Errorf("MongoDB client not connected")
	}

	ctx := context.Background()

	// Parse the filter JSON string to BSON
	var filter bson.M
	if err := bson.UnmarshalExtJSON([]byte(filterJSON), true, &filter); err != nil {
		return false, fmt.Errorf("failed to parse filter JSON: %w", err)
	}

	// Create aggregation pipeline with $documents and $match stages
	// $documents creates ephemeral documents that exist only in the pipeline
	// $match applies the filter to those documents
	pipeline := mongo.Pipeline{
		{{Key: "$documents", Value: []types.Document{document}}},
		{{Key: "$match", Value: filter}},
	}

	// Execute the aggregation on any database (we use admin since we don't need a real collection)
	// The $documents stage creates ephemeral data, so no collection access is needed
	cursor, err := m.client.Database("gravel").Aggregate(ctx, pipeline)
	if err != nil {
		return false, fmt.Errorf("failed to execute aggregation: %w", err)
	}
	defer cursor.Close(ctx)

	// Check if any documents were returned
	// If the filter matched, we'll get the document back; otherwise, no results
	hasResults := cursor.Next(ctx)
	if err := cursor.Err(); err != nil {
		return false, fmt.Errorf("cursor error: %w", err)
	}

	if hasResults {
		return true, nil
	}
	return false, nil
}

// Destructures a query into its component parts and analyzes it to get all relevant information to watch it probably
func (m *MongoProvider) GetQueryAnalysis(query types.WatchQueryRequest, queryResult *types.WatchQueryResponse) (types.QueryAnalysis, error) {

	var queryInformation = types.QueryAnalysis{}

	// ======== analyze projections ========

	// parse the find options to retrieve the projection as an object
	findOptions, err := parseFindOptionsString(query.Options)
	if err != nil {
		return types.QueryAnalysis{}, err
	}

	// Extract relevant fields from projection. which should be every key existing in the projection
	if findOptions.Projection == nil {
		queryInformation.NoProjection = true
	}
	queryInformation.ProjectionFields = flattenObject(findOptions.Projection)

	// add _id if not present so we track it. _id is always present in the returning data regardless of projection. we mimic this behavior here
	if !slices.Contains(queryInformation.ProjectionFields, "_id") {
		queryInformation.ProjectionFields = append(queryInformation.ProjectionFields, "_id")
	}

	// ======== analyze query ========

	// parse the query string to retrieve the filter as an object
	queryObject, err := parseQueryString(query.Query)
	if err != nil {
		return types.QueryAnalysis{}, err
	}

	// Extract relevant fields from query
	queryInformation.FilterFields, err = getRelevantFieldsFromQueryObject(queryObject)
	if err != nil {
		return types.QueryAnalysis{}, err
	}

	// ======== analyze sort ========

	// Extract relevant fields from sort. we only need the keys here as sorting order is not relevant in this case
	queryInformation.SortFields = extractSortFields(findOptions)

	// append default sort by id if not present
	if !slices.Contains(queryInformation.SortFields, types.SortField{
		Field: "_id",
		Order: -1,
	}) {
		queryInformation.SortFields = append(queryInformation.SortFields, types.SortField{
			Field: "_id",
			Order: -1,
		})
	}

	// ======== analyze window ========
	skip := int64(0)
	if findOptions.Skip != nil {
		skip = *findOptions.Skip
	}
	limit := int64(0)
	if findOptions.Limit != nil {
		limit = *findOptions.Limit
	}

	queryInformation.WindowStart = int(skip)
	queryInformation.WindowEnd = queryInformation.WindowStart + int(limit)
	queryInformation.WindowLimit = int(limit)

	return queryInformation, nil
}

func (m *MongoProvider) GetWatchedDocumentInfo(document types.Document, queryInformation types.QueryAnalysis) (types.WatchedDocument, error) {

	// Extract document ID using the database provider
	docID, err := GetIDFromEntry(document)
	if err != nil {
		fmt.Printf("Failed to extract document ID: %v\n", err)
		return types.WatchedDocument{}, err
	}

	// Extract sort values from document
	sortValues := getSortValuesFromDocument(document, queryInformation.SortFields)

	return types.WatchedDocument{
		ID:         docID,
		SortValues: sortValues,
	}, nil
}

func (m *MongoProvider) GetSortingOrder(docInfoA types.WatchedDocument, docInfoB types.WatchedDocument, queryInformation types.QueryAnalysis) int {
	return mongoSortingComparator(queryInformation.SortFields, docInfoA, docInfoB)
}

func (m *MongoProvider) GetDocumentID(document types.Document) string {
	documentID, err := GetIDFromEntry(document)
	if err != nil {
		log.Printf("Failed to extract document ID: %v\n", err)
		return ""
	}
	return documentID
}

func (m *MongoProvider) GetNewPositionForDocument(documents []types.WatchedDocument, oldIndex int, sortFields []types.SortField) int {
	return getNewPositionForDocument(documents, oldIndex, sortFields)
}

func (m *MongoProvider) GetPositionForDocumentInWindow(documents []types.WatchedDocument, document types.WatchedDocument, sortFields []types.SortField) int {
	// append the document at the end of the documents and then call getNewPositionForDocument
	appendedDocuments := append(documents, document)
	return getNewPositionForDocument(appendedDocuments, len(appendedDocuments)-1, sortFields)
}

func (m *MongoProvider) ProjectDocument(document types.Document, options string, nestedPath string) (types.Document, error) {
	return applyProjection(document, options, nestedPath)
}
