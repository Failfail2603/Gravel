package db

// Re-export shared types for backward compatibility
import "gravel/db/shared"

type DBChangeStreamEvent = shared.DBChangeStreamEvent
type DatabaseConnectRequest = shared.DatabaseConnectRequest
type DatabaseConnectResponse = shared.DatabaseConnectResponse
type WatchQueryRequest = shared.WatchQueryRequest
type WatchQueryStopRequest = shared.WatchQueryStopRequest
type WatchQueryResponse = shared.WatchQueryResponse
type QueryAnalysis = shared.QueryAnalysis
type DebugMessage = shared.DebugMessage
