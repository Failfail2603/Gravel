import { ObjectId } from "mongodb";
import type { NatsConnection } from "nats";
import { Observable } from "rxjs/internal/Observable";
import { GravelDBs } from "../gravel.js";
import { createGravelClient, type GravelClient } from "./GravelClient.js";
import { watchQueryToObservable } from "./watchQueryObservable.js";

// #region Mongo search types

/** @public */
export declare type Sort =
  | string
  | Exclude<
      SortDirection,
      {
        readonly $meta: string;
      }
    >
  | ReadonlyArray<string>
  | {
      readonly [key: string]: SortDirection;
    }
  | ReadonlyMap<string, SortDirection>
  | ReadonlyArray<readonly [string, SortDirection]>
  | readonly [string, SortDirection];

/** @public */
export declare type SortDirection =
  | 1
  | -1
  | "asc"
  | "desc"
  | "ascending"
  | "descending"
  | {
      readonly $meta: string;
    };

type ProjectionValue = 1 | 0 | { [key: string]: ProjectionValue };

export interface GravelMongoWatchQueryFindOptions {
  projection?: Record<string, ProjectionValue>;
  sort?: Sort;
  skip?: number;
  limit?: number;

  // whether gravel should return the full document or only the changes
  returnFullDocument?: boolean;
}

export interface GravelMongoWatchQueryRequest {
  clientID: string;
  hash: string;
  collectionName: string;
  query: string;
  options?: string;
  reconnect?: boolean;
}

interface StoredQueryInfo {
  collectionName: string;
  query: Record<string, any>;
  options?: GravelMongoWatchQueryFindOptions;
}

// #endregion

// #region Mongo provider

export interface GravelMongoOptions {
  mongoUrl: string;
}

export interface GravelMongoClient extends GravelClient {
  watchQuery<T extends Record<string, any>>(
    collectionName: string,
    query: Record<string, any>,
    options?: GravelMongoWatchQueryFindOptions,
  ): Promise<{
    initialQuery: { result: Array<T> };
    changes: Observable<Array<T>>;
    stop: () => Promise<void>;
  }>;
}

/**
 * Preprocesses a query object to convert JavaScript RegExp objects to MongoDB $regex syntax.
 * This ensures that regex patterns are properly serialized when stringifying the query.
 *
 * @param obj - The query object to preprocess
 * @returns A new object with RegExp objects converted to MongoDB $regex syntax
 * @internal
 */
function preprocessRegexInQuery(obj: any): any {
  if (obj === null || typeof obj !== "object") {
    return obj;
  }

  if (obj instanceof ObjectId) {
    return { $oid: obj.toHexString() };
  }

  if (obj instanceof RegExp) {
    // Convert RegExp to MongoDB $regex syntax
    return {
      $regex: obj.source,
      $options: obj.flags,
    };
  }

  if (Array.isArray(obj)) {
    return obj.map((item) => preprocessRegexInQuery(item));
  }

  // Handle regular objects
  const result: any = {};
  for (const [key, value] of Object.entries(obj)) {
    result[key] = preprocessRegexInQuery(value);
  }
  return result;
}

/**
 * Creates a deterministic 16-character hash for MongoDB queries using djb2 algorithm.
 * Used for query deduplication and subscription multiplexing.
 *
 * @param collectionName - MongoDB collection name
 * @param query - MongoDB query object
 * @param options - Query options (projection, sort, skip, limit, returnFullDocument)
 * @returns 16-character hexadecimal hash string
 * @internal
 */
function hashQuery(
  collectionName: string,
  query: Record<string, any>,
  options?: GravelMongoWatchQueryFindOptions,
): string {
  // Preprocess the query to handle regex patterns before hashing
  const preprocessedQuery = preprocessRegexInQuery(query);

  // Create a deterministic string representation by sorting object keys
  const sortedQuery = JSON.stringify(
    preprocessedQuery,
    Object.keys(preprocessedQuery).sort(),
  );
  const sortedOptions = JSON.stringify(
    options || {},
    Object.keys(options || {}).sort(),
  );
  const input = `${collectionName}:${sortedQuery}:${sortedOptions}`;

  // Use a simple but effective hash algorithm (djb2)
  let hash = 5381;
  for (let i = 0; i < input.length; i++) {
    hash = (hash << 5) + hash + input.charCodeAt(i);
    hash = hash >>> 0; // Convert to unsigned 32-bit integer
  }

  // Convert to hex and pad to ensure exactly 16 characters
  const hexHash = hash.toString(16);

  // If hash is shorter than 16 chars, pad with leading zeros
  // If longer, take first 16 chars and add a secondary hash for uniqueness
  if (hexHash.length >= 16) {
    return hexHash.substring(0, 16);
  } else {
    // Create a secondary hash from the input to fill remaining characters
    let secondaryHash = 0;
    for (let i = 0; i < input.length; i++) {
      secondaryHash =
        (secondaryHash << 3) + secondaryHash + input.charCodeAt(i);
      secondaryHash = secondaryHash >>> 0;
    }
    const padding = secondaryHash.toString(16);
    return (hexHash + padding).substring(0, 16).padStart(16, "0");
  }
}

export async function generateMongoProvider(
  createNatsConnection: () => Promise<NatsConnection>,
  options: GravelMongoOptions,
): Promise<GravelMongoClient> {
  const base = await createGravelClient({
    createNatsConnection,
    dbProviderID: options.mongoUrl,
    channelPrefix: "gravel.mongo",
    connectRequest: {
      dbType: GravelDBs.MongoDB,
      mongoURL: options.mongoUrl,
    },
    buildWatchQueryRequest: (
      clientID: string,
      hash: string,
      storedQuery: StoredQueryInfo,
    ) =>
      ({
        clientID,
        hash,
        collectionName: storedQuery.collectionName,
        query: JSON.stringify(preprocessRegexInQuery(storedQuery.query)),
        options: JSON.stringify(storedQuery.options),
      }) satisfies GravelMongoWatchQueryRequest,
  });

  return {
    get clientID() {
      return base.clientID;
    },
    dbProviderID: base.dbProviderID,
    get debugCallback() {
      return base.debugCallback;
    },
    set debugCallback(callback) {
      base.debugCallback = callback;
    },
    close: () => base.close(),

    async watchQuery<T extends Record<string, any> = Record<string, any>>(
      collectionName: string,
      query: Record<string, any>,
      options?: GravelMongoWatchQueryFindOptions,
    ): Promise<{
      initialQuery: { result: Array<T> };
      changes: Observable<Array<T>>;
      stop: () => Promise<void>;
    }> {
      const queryHash = hashQuery(collectionName, query, options);

      const watchQueryPromise = base.registerWatchQuery<T>(
        queryHash,
        { collectionName, query, options } satisfies StoredQueryInfo,
        {
          clientID: base.clientID,
          hash: queryHash,
          collectionName,
          query: JSON.stringify(preprocessRegexInQuery(query)),
          options: JSON.stringify(options),
        } satisfies GravelMongoWatchQueryRequest,
      );

      const watchQuery = await watchQueryPromise;

      return {
        initialQuery: watchQuery.initialQuery,
        changes: watchQueryToObservable(Promise.resolve(watchQuery)),
        stop: watchQuery.stop,
      };
    },
  };
}

// #endregion
