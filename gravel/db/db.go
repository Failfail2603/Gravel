package db

import (
	"fmt"
	"gravel/db/mongo"
)

const (
	DBTypeMongoDB string = "mongodb"
)

type DBProvider interface {
	Connect() error
	Disconnect() error
	GetQueryAnalysis(query WatchQueryRequest) (QueryAnalysis, error)
	Query(collection string, query string, findOptions string) []interface{}
	StartChangeStream(natsResponseChanneldbUpdates chan DBChangeStreamEvent)
	StopChangeStream()
}

type WatchQuery struct {
	ClientID   string
	Hash       string
	Collection string
	Query      string
	Options    string

	// we dedube watchqueries by hash, so we need to count the number of connections to the same watchquery
	// the multiplexing to the different queries observables will be handled by the client
	NumberOfConnections int

	// these are some analytical fields which get computed at the register of the watchquery.
	// they hold information which is used later to determine if a change is relevant for the watchquery
	QueryInformation QueryAnalysis
}

type DBService struct {
	Connection    DBProvider
	UpdateChannel chan DBChangeStreamEvent

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
// It currently supports only MongoDB. Returns nil if the database type is unsupported.
func StartDBConnection(connectionRequest DatabaseConnectRequest) (*DBService, error) {
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

// as different dbs can be supported and the als hear to the same request we need to have different identifiers for each db type in the dbServices map
// this function returns the identifier for the given connection request. For example for mongo this can just be the mongo url
func getDBConnectionIdentifier(connectionRequest DatabaseConnectRequest) (string, error) {
	switch connectionRequest.DBType {
	case DBTypeMongoDB:
		return connectionRequest.MongoURL, nil
	default:
		return "", fmt.Errorf("unsupported database type: %s", connectionRequest.DBType)
	}
}
