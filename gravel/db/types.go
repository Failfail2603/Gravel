package db

import "time"

type DBChangeStreamEvent struct {
	Database  string      `json:"database"`
	Operation string      `json:"operation"`
	Document  interface{} `json:"document"`
	Timestamp time.Time   `json:"timestamp"`
}

type DatabaseConnectRequest struct {
	DBType   string `json:"dbType"`
	MongoURL string `json:"mongoUrl"`
	ClientID string `json:"clientID"`
}

type DatabaseConnectResponse struct {
	Status   string `json:"status"`
	Database string `json:"database"`
	ClientID string `json:"clientID"`
	Error    string `json:"error"`
}

type WatchQueryRequest struct {
	// The client id of the client that requested the watchquery
	ClientID string `json:"clientID"`

	// The hash of the query to identify the query on the client
	Hash string `json:"hash"`

	// The name of the collection to watch
	CollectionName string `json:"collectionName"`

	// The query to watch
	Query string `json:"query"`

	// The options for the query
	Options string `json:"options"`
}

type WatchQueryStopRequest struct {
	ClientID string `json:"clientID"`
	Hash     string `json:"hash"`
}

type WatchQueryResponse struct {
	Status string `json:"status"`

	Error string `json:"error"`
}
