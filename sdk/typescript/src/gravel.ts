import { connect, type Msg, type NatsConnection, type NatsError } from "nats";
import { type GravelClient } from "./db/gravelClient";
import {
  generateMongoProvider,
  type GravelMongoClient,
  type GravelMongoOptions,
} from "./db/mongo";
import {
  generateRedisProvider,
  type GravelRedisClient,
  type GravelRedisOptions,
} from "./db/redis";
import { GravelChannels } from "./gravelChannels";

export enum GravelDBs {
  MongoDB = "mongodb",
  Redis = "redis",
}

export interface GravelConnectOptions {
  debugChannelCallback?: (err: NatsError | null, msg: Msg) => void;
}

// Conditional type mapping for database clients
type DatabaseClientMap = {
  [GravelDBs.MongoDB]: GravelMongoClient;
  [GravelDBs.Redis]: GravelRedisClient;
};

type DatabaseOptionsClientMap = {
  [GravelDBs.MongoDB]: GravelMongoOptions;
  [GravelDBs.Redis]: GravelRedisOptions;
};

function getDBProviderID<T extends GravelDBs>(
  options: { db: T } & DatabaseOptionsClientMap[T],
): string {
  switch (options.db) {
    case GravelDBs.MongoDB:
      // @ts-expect-error we guarded the type via the function definitions and the switch
      return options.mongoUrl;
    case GravelDBs.Redis:
      return "redis";
  }
}

export interface Gravel {
  getDatabaseClient<T extends GravelDBs>(
    options: {
      db: T;
    } & DatabaseOptionsClientMap[T],
  ): Promise<DatabaseClientMap[T]>;
}

/**
 * Map of gravel connections
 * Each unique database will have a
 */
let gravelInstance: Gravel | null = null;

const databaseClients: Map<string, GravelClient> = new Map();

/**
 * Connects to Gravel initially and returns a NatsConnection
 *
 * @param options - Optional options for the connection
 * @returns A Promise that resolves to a NatsConnection
 */
async function connectToGravel(): Promise<NatsConnection> {
  const natsConnection = await connect();

  return natsConnection;
}

export async function getGravelConnection(
  gravelOptions?: GravelConnectOptions,
): Promise<Gravel> {
  if (gravelInstance) {
    return gravelInstance;
  }

  const natsConnection = await connectToGravel();

  gravelInstance = {
    getDatabaseClient: <T extends GravelDBs>(
      options: {
        db: T;
      } & DatabaseOptionsClientMap[T],
    ): Promise<DatabaseClientMap[T]> => {
      return new Promise(async (resolve, reject) => {
        // we make sure we only create one client per type and connection

        const dbProviderID = getDBProviderID(options);

        let existingClient = databaseClients.get(dbProviderID) as
          | DatabaseClientMap[T]
          | undefined;

        if (existingClient) {
          // unknown works here as the type is already known
          resolve(existingClient as unknown as DatabaseClientMap[T]);
        }

        switch (options.db) {
          case GravelDBs.MongoDB:
            existingClient = (await generateMongoProvider(
              natsConnection,
              options as GravelMongoOptions,
            )) as DatabaseClientMap[T];
            break;
          case GravelDBs.Redis:
            existingClient = (await generateRedisProvider(
              natsConnection,

              options as GravelRedisOptions,
            )) as DatabaseClientMap[T];
            break;
          default:
            reject(new Error(`Unsupported database type: ${options.db}`));
            return;
        }

        if (!existingClient) {
          reject(
            new Error(`Failed to create database client for ${options.db}`),
          );
        }

        // if the developer specifies a debug callback we subscribe to the debug channel and forward the messages to the callback
        if (gravelOptions?.debugChannelCallback) {
          existingClient.debugCallback = gravelOptions?.debugChannelCallback;

          const debugSubscription = natsConnection.subscribe(
            GravelChannels.GravelDebug + "." + existingClient.clientID,
          );
          debugSubscription.callback = gravelOptions?.debugChannelCallback;
        }

        databaseClients.set(existingClient.dbProviderID, existingClient);

        resolve(existingClient as DatabaseClientMap[T]);
      });
    },
  };

  return gravelInstance;
}
