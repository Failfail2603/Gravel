import type { NatsConnection } from "nats";
import { Subject } from "rxjs";
import { Observable } from "rxjs/internal/Observable";
import { v4 as uuidv4 } from "uuid";
import { GravelDBs } from "../gravel";
import { GravelChannels } from "../gravelChannels";
import type { GravelDatabaseConnectRequest } from "../natsMessages";
import type { GravelClient } from "./gravelClient";

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

export interface GravelMongoWatchQueryFindOptions {
  projection?: Record<string, 1 | 0>;
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
}

export interface GravelMongoWatchQueryStopRequest {
  clientID: string;
  hash: string;
}

export interface GravelMongoWatchQueryResponse {
  // the has of the query to identify the inital query on the client and give back the correct result
  queryHash: string;

  // the type of the result. can be "full" or "patch"
  type: "full" | "patch";

  // the result. can be a full document or a json patch depending on the type
  result: Array<Record<string, any>>;
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
    initialQuery: Array<T>;
    changes: Observable<Array<T>>;
    stop: () => Promise<void>;
  }>;
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
  // Create a deterministic string representation by sorting object keys
  const sortedQuery = JSON.stringify(query, Object.keys(query).sort());
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

interface GravelMongoSubscription<
  T extends Record<string, any> = Record<string, any>,
> {
  // this id is only used on the client to stop the correct observable. Gravel does not care which observable belongs to which webclient connected. We cannot just end all queries under a hash as gravel does not know which client is connected to which query.
  clientQueryId: string;
  initialQuery: Array<T>;
  changes: Subject<Array<T>>;
  stop: () => Promise<void>;
}

export async function generateMongoProvider(
  natsConnection: NatsConnection,
  options: GravelMongoOptions,
): Promise<GravelMongoClient> {
  // build a client id for the provider
  const clientID = uuidv4();

  // on getting the mongo provider, the sdk sends a connection request to gravel to ensure the database is correcly loaded.
  // gravel connects to the database and holds a connection open for all clients
  await natsConnection.request(
    GravelChannels.DatabaseConnect,
    JSON.stringify({
      dbType: GravelDBs.MongoDB,
      mongoURL: options.mongoUrl,
      clientID,
    } as GravelDatabaseConnectRequest),
    {
      timeout: 5000,
      reply: GravelChannels.GravelDebug,
      noMux: true,
    },
  );

  // the client gets a unique channel per client. Watchqueries are shared on one client and split to the corresponding queries
  const watchQueryChannel = "gravel.mongo.watchquery." + clientID;
  const watchQueryChannelInitial = "gravel.mongo.initial." + clientID;

  // generate the db provider id unique to the connection setting.
  // same connections will result in the same id
  // the mongo url should be unique in this case
  const dbProviderID = options.mongoUrl;

  // the active subscriptions of the client
  // the key will be a hash of the query so we can hold multiple handles and observables for the same kind of query and multiplex them on arrival
  // under a specifc query hash there will be all
  const activeSubscriptions = new Map<
    string,
    Array<GravelMongoSubscription<any>>
  >();

  // subscribe to the watchquery channel tro recieve updates
  natsConnection.subscribe(watchQueryChannel, {
    callback(err, msg) {
      if (err) {
        console.error(err);
        return;
      }

      const response = JSON.parse(
        msg.data.toString(),
      ) as GravelMongoWatchQueryResponse;

      // get the queries which should be updated
      const activeSubscriptionsForQuery = activeSubscriptions.get(
        response.queryHash,
      );

      if (!activeSubscriptionsForQuery) {
        return;
      }

      for (const subscription of activeSubscriptionsForQuery) {
        subscription.changes.next(response.result);
      }
    },
  });

  // the channel which recieves the initial query
  natsConnection.subscribe(watchQueryChannelInitial, {
    callback(err, msg) {
      if (err) {
        console.error(err);
        return;
      }

      const response = JSON.parse(
        msg.data.toString(),
      ) as GravelMongoWatchQueryResponse;

      // get the queries which should be updated
      const activeSubscriptionsForQuery = activeSubscriptions.get(
        response.queryHash,
      );

      if (!activeSubscriptionsForQuery) {
        return;
      }

      for (const subscription of activeSubscriptionsForQuery) {
        subscription.changes.next(response.result);
      }
    },
  });

  return {
    // the watchquery function
    clientID,
    dbProviderID,
    async watchQuery<T extends Record<string, any>>(
      collectionName: string,
      query: Record<string, any>,
      options?: GravelMongoWatchQueryFindOptions,
    ): Promise<GravelMongoSubscription<T>> {
      const updateSubject = new Subject<Array<T>>();

      // gravel gets a unique channel for every client, which is shared between watchqueries.
      // to differentiate between different watchqueries, we use a hash of the query
      const queryHash = hashQuery(collectionName, query, options);

      // if the query is already active, we return the existing subscription
      const activeSubscriptionsForQuery = activeSubscriptions.get(queryHash);

      const queryID = uuidv4();

      const subscription = {
        // we init the subscription with an empty array. Before we return it we need to wait for the inital result from gravel
        clientQueryId: queryID,
        initialQuery: [],
        changes: updateSubject,
        stop: async () => {
          console.log(
            "Stopping watchquery for client",
            clientID,
            "and query",
            queryHash,
            "with id",
            queryID,
          );
          // updateSubject.complete();

          const subs = activeSubscriptions.get(queryHash);

          // pull out the subscription from the active subscriptions
          subs?.forEach((subscription) => {
            if (subscription.clientQueryId === queryID) {
              subs?.splice(subs.indexOf(subscription), 1);
            }
          });

          // send stop request with query hash
          await natsConnection.request(
            "gravel.watchquery.stop",
            JSON.stringify({
              clientID,
              hash: queryHash,
            } satisfies GravelMongoWatchQueryStopRequest),
            {
              timeout: 5000,
              reply: GravelChannels.GravelDebug,
              noMux: true,
            },
          );

          console.log(
            "Stopped watchquery for client",
            clientID,
            "and query",
            queryHash,
            "with id",
            queryID,
          );
        },
      } satisfies GravelMongoSubscription<T>;

      if (activeSubscriptionsForQuery) {
        activeSubscriptionsForQuery.push(subscription);
      } else {
        activeSubscriptions.set(queryHash, [subscription]);
      }

      // call gravel to register the query
      await natsConnection.request(
        "gravel.watchquery",
        JSON.stringify({
          clientID,
          hash: queryHash,
          collectionName,
          query: JSON.stringify(query),
          options: JSON.stringify(options),
        } satisfies GravelMongoWatchQueryRequest),
        {
          timeout: 5000,
          reply: GravelChannels.GravelDebug,
          noMux: true,
        },
      );

      return subscription;
    },
  };
}

// #endregion
