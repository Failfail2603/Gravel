package db

import (
	"fmt"
	"gravel/db/mongo"
	"gravel/types"
)

const (
	DBTypeMongoDB string = "mongodb"
)

type DBProvider interface {
	Connect() error
	Disconnect() error
	GetQueryAnalysis(query types.WatchQueryRequest, queryResult *types.WatchQueryResponse) (types.QueryAnalysis, error)
	Query(collection string, query string, findOptions string) []types.Document
	StartChangeStream(natsResponseChanneldbUpdates chan types.DBChangeStreamEvent)
	StopChangeStream()
	TestFilterWithDocument(filterJSON string, document types.Document) (bool, error)
	GetWatchedDocumentInfo(document types.Document, queryInformation types.QueryAnalysis) (types.WatchedDocument, error)
}

type DBService struct {
	Connection    DBProvider
	UpdateChannel chan types.DBChangeStreamEvent

	/**
	 * The currently open WatchQueries on the database client
	 */
	WatchQueries map[string]*WatchQuery
}

func newDBService(service DBProvider) (*DBService, error) {
	if err := service.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	return &DBService{Connection: service, WatchQueries: make(map[string]*WatchQuery)}, nil
}

// StartDBConnection initializes and returns a DBService for the specified database type.
// It currently supports only Mongotypes. Returns nil if the database type is unsupported.
func StartDBConnection(connectionRequest types.DatabaseConnectRequest) (*DBService, error) {
	switch connectionRequest.DBType {
	case DBTypeMongoDB:
		return newDBService(NewMongoProvider(connectionRequest.MongoURL))
	default:
		return nil, fmt.Errorf("unsupported database type: %s", connectionRequest.DBType)
	}
}

// NewMongoProvider creates a new MongoDB provider - this is a factory function
// that avoids cyclic imports by being defined in the db package
func NewMongoProvider(mongoURL string) DBProvider {
	return mongo.NewMongoProvider(mongoURL)
}
