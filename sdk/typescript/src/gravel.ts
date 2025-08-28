import { connect, type Msg, type NatsConnection, type NatsError } from "nats";
import { v4 as uuidv4 } from "uuid";
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

export interface Gravel {
  clientID: string;
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

const databaseClients: Map<GravelDBs, DatabaseClientMap> = new Map();

/**
 * Connects to Gravel initially and returns a NatsConnection
 *
 * @param options - Optional options for the connection
 * @returns A Promise that resolves to a NatsConnection
 */
async function connectToGravel(
  options?: GravelConnectOptions,
): Promise<NatsConnection> {
  const natsConnection = await connect();
  console.log("Connected to Gravel");

  const debugCallback =
    options?.debugChannelCallback ??
    ((err: NatsError | null, _: Msg) => {
      if (err) console.error("Gravel Error: ", err);
    });

  const debugSubscription = natsConnection.subscribe(
    GravelChannels.GravelDebug,
  );
  debugSubscription.callback = debugCallback;

  return natsConnection;
}

export async function getGravelConnection(
  options?: GravelConnectOptions,
): Promise<Gravel> {
  if (gravelInstance) {
    return gravelInstance;
  }

  const natsConnection = await connectToGravel(options);

  const clientID = uuidv4();

  gravelInstance = {
    clientID,
    getDatabaseClient: <T extends GravelDBs>(
      options: {
        db: T;
      } & DatabaseOptionsClientMap[T],
    ): Promise<DatabaseClientMap[T]> => {
      return new Promise(async (resolve, reject) => {
        let existingClient = databaseClients.get(options.db) as
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
              clientID,
              options as GravelMongoOptions,
            )) as DatabaseClientMap[T];
            break;
          case GravelDBs.Redis:
            existingClient = (await generateRedisProvider(
              natsConnection,
              clientID,
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

        resolve(existingClient as DatabaseClientMap[T]);
      });
    },
  };

  return gravelInstance;
}
