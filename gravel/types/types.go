package types

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// FieldUpdate represents a single field change within a database change event
type FieldUpdate struct {
	Field     string      `json:"field"`     // The field path (e.g., "name", "address.city")
	Value     interface{} `json:"value"`     // The new value (nil for removed fields)
	Operation string      `json:"operation"` // "set" for updated/inserted fields, "unset" for removed fields
}

// DBChangeStreamEvent represents a database change event
type DBChangeStreamEvent struct {
	Database                 string        `json:"database"`
	Collection               string        `json:"collection"`
	Operation                string        `json:"operation"`
	ID                       string        `json:"id"`
	FullUpdate               interface{}   `json:"document"`
	FullDocument             interface{}   `json:"fullDocument"`             // The complete document after the change (for insert/update/replace)
	FullDocumentBeforeChange interface{}   `json:"fullDocumentBeforeChange"` // The complete document before the change (pre-image)
	Timestamp                time.Time     `json:"timestamp"`
	Updates                  []FieldUpdate `json:"updates"` // Individual field changes extracted from the change event
	// we might need to fetch a document multiple times while processing the updates for a single watchquery change event
	// to prevent this we cache the retrieved documents for a single event as the database should be
	UpdateCache map[int]Document `json:"-"`
	// ClusterTime from the change event for snapshot reads at that specific point in time
	ClusterTime *primitive.Timestamp `json:"-"`

	// track what indices got removed so we do not remove them again
	RemovedIndices []int `json:"-"`

	// ProcessedDocumentIDs tracks document IDs that have been processed at this ClusterTime
	// This is critical for insertMany where multiple documents share the same ClusterTime
	// and we need to exclude already-processed documents from subsequent queries
	ProcessedDocumentIDs []string `json:"-"`

	// BatchShiftOffset tracks cumulative window shifts in the current ClusterTime batch
	// Used to adjust query indices when multiple shifts occur at the same snapshot point
	// Positive = shifts up (query at lower indices), Negative = shifts down (query at higher indices)
	BatchShiftOffset int `json:"-"`
}

// DatabaseConnectRequest represents a request to connect to a database
type DatabaseConnectRequest struct {
	DBType   string `json:"dbType"`
	MongoURL string `json:"mongoUrl"`
	ClientID string `json:"clientID"`
}

// DatabaseConnectResponse represents a response from a database connection attempt
type DatabaseConnectResponse struct {
	Status   string `json:"status"`
	Database string `json:"database"`
	ClientID string `json:"clientID"`
	Error    string `json:"error"`
}

// WatchQueryRequest represents a request to watch a query
type WatchQueryRequest struct {
	ClientID       string `json:"clientID"`
	Hash           string `json:"hash"`
	CollectionName string `json:"collectionName"`
	Query          string `json:"query"`
	Options        string `json:"options"`
}

// KeepAliveRequest represents a keepalive ping from a client
type KeepAliveRequest struct {
	ClientID string `json:"clientID"`
}

// KeepAliveResponse represents the server's response to a keepalive ping
type KeepAliveResponse struct {
	Status string `json:"status"`
}

// WatchQueryStopRequest represents a request to stop watching a query
type WatchQueryStopRequest struct {
	ClientID string `json:"clientID"`
	Hash     string `json:"hash"`
}

// WatchQueryResponse represents a response from a watch query
type WatchQueryResponse struct {
	QueryHash string `json:"queryHash"`
	Type      string `json:"type"`
	Result    string `json:"result"`
}

type SortField struct {
	Field string `json:"field"`

	// 1 ascending, -1 descending
	Order int `json:"order"`
}

type WatchedDocument struct {
	ID string `json:"id"`

	// use the "any" type here as we do not know at compile time which types the sorted values have
	SortValues []interface{} `json:"sortValues"`
}

// Document represents a MongoDB document as a map with string keys and values of any type
type Document map[string]interface{}

// QueryAnalysis contains analysis information about a query
type QueryAnalysis struct {
	// the projected fields of the query
	ProjectionFields []string
	FilterFields     []string
	SortFields       []SortField
	// is skip
	WindowStart int
	// should be skip + limit
	WindowEnd   int
	WindowLimit int

	// no projection given. This means everything is projected automatically
	NoProjection bool
}

// DebugMessage represents a debug message
type DebugMessage struct {
	ClientID string `json:"clientID"`
	Status   string `json:"status"`
	Message  string `json:"message"`
	Error    string `json:"error"`
}
