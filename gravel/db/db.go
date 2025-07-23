package db

import (
	"fmt"
	"time"
)

type DBChangeStreamEvent struct {
	Database  string      `json:"database"`
	Operation string      `json:"operation"`
	Document  interface{} `json:"document"`
	Timestamp time.Time   `json:"timestamp"`
}

type DatabaseConnectRequest struct {
	Database string `json:"database"`
	ClientID string `json:"client_id"`
}

type DatabaseConnectResponse struct {
	Status   string `json:"status"`
	Database string `json:"database"`
	Error    string `json:"error"`
}

type DBType string

const (
	DBTypeMongoDB DBType = "mongodb"
)

type DBProvider interface {
	connect() error
	Disconnect() error
	StartChangeStream(db string, collection string, dbUpdates chan DBChangeStreamEvent)
	StopChangeStream(db string, collection string)
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
func StartDBConnection(db DBType) (*DBService, error) {
	switch db {
	case DBTypeMongoDB:
		return newDBService(NewMongoProvider())
	default:
		return nil, fmt.Errorf("unsupported database type: %s", db)
	}
}
