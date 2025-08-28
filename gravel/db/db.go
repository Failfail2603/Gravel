package db

import (
	"fmt"
)

const (
	DBTypeMongoDB string = "mongodb"
)

type DBProvider interface {
	connect() error
	disconnect() error
	startChangeStream(db string, collection string, dbUpdates chan DBChangeStreamEvent)
	stopChangeStream(db string, collection string)
}

type DBService struct {
	Connection DBProvider
}

func newDBService(service DBProvider) (*DBService, error) {
	if err := service.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	return &DBService{Connection: service}, nil
}

// StartDBConnection initializes and returns a DBService for the specified database type.
// It currently supports only MongoDB. Returns nil if the database type is unsupported.
func StartDBConnection(connectionRequest DatabaseConnectRequest) (*DBService, error) {
	switch connectionRequest.DBType {
	case DBTypeMongoDB:
		return newDBService(generateMongoProvider(connectionRequest))
	default:
		return nil, fmt.Errorf("unsupported database type: %s", connectionRequest.DBType)
	}
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
