# Gravel Testing Suite

This directory contains the testing and development tools for the Gravel SDK.

## Structure

```
testing/
├── config.ts           # Configuration (query, options, ports, MongoDB URL)
├── dataGenerators.ts   # Faker-based data generation utilities
├── gravelClient.ts     # Gravel connection and watchQuery management
├── mongoClient.ts      # MongoDB client singleton
├── routes.ts           # Express API routes
├── server.ts           # Main server entry point
├── viewer.html         # Web UI for viewing and comparing data
└── README.md           # This file
```

## Files Overview

### `config.ts`
Contains all configuration constants:
- `PORT`: Express server port (default: 3000)
- `MONGO_URL`: MongoDB connection string
- `USE_BULK_OPERATIONS`: Toggle between bulk and individual operations
- `query`: MongoDB query filter
- `options`: Query options (sort, limit, skip, projection)

### `dataGenerators.ts`
Utility functions for generating random test data:
- `generateRandomEmail()`: Random email addresses
- `generateRandomAddress()`: Random address objects
- `generateRandomBirthday()`: Random birthdates
- `generateRandomUpdateFields()`: Random user field updates

### `mongoClient.ts`
MongoDB client management:
- `getMongoClient()`: Singleton MongoDB client
- `closeMongoClient()`: Cleanup function

### `gravelClient.ts`
Gravel SDK integration:
- `restartWatchQuery()`: Initialize/restart Gravel watchQuery
- `currentData`: Reactive data state
- `stopWatchQuery`: Cleanup function

### `routes.ts`
Express API endpoints:
- `GET /`: Serve viewer.html
- `GET /data`: Get current Gravel data
- `GET /simplequery`: Direct MongoDB query for comparison
- `POST /randomupdate`: Trigger random user updates

### `server.ts`
Main entry point that:
- Starts Express server
- Initializes Gravel watchQuery
- Handles graceful shutdown

### `viewer.html`
Web interface for:
- Viewing real-time Gravel data
- Comparing Gravel vs direct MongoDB results
- Triggering random updates
- Detecting data differences

## Usage

To run the test server:

```bash
# From the sdk/typescript directory
npm run dev  # or whatever script runs server.ts
```

Then open http://localhost:3000 in your browser.

## Features

- **Real-time Updates**: Watches MongoDB changes via Gravel SDK
- **Data Comparison**: Compares Gravel's cached data with direct MongoDB queries
- **Random Updates**: Tests change stream propagation
- **Visual Diff**: Highlights differences between data sources
