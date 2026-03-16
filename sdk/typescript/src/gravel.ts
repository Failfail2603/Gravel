import { connect, type Msg, type NatsConnection, type NatsError } from "nats";
import { type GravelClient } from "./db/GravelClient.js";
import {
  generateMongoProvider,
  type GravelMongoClient,
  type GravelMongoOptions,
} from "./db/GravelMongoClient.js";
export { watchQueryToObservable } from "./db/watchQueryObservable.js";
export type { WatchQueryResult } from "./db/watchQueryObservable.js";

export enum GravelDBs {
  MongoDB = "mongodb",
}

interface GravelConnectionOptions {
  gravelHost?: string;
  gravelPort?: number;
  timeoutMs?: number; // Connection timeout in milliseconds (default: 10000)
}

export interface GravelConnectOptions {
  debugChannelCallback?: (err: NatsError | null, msg: Msg) => void;
  connection: GravelConnectionOptions;
}

// Conditional type mapping for database clients
type DatabaseClientMap = {
  [GravelDBs.MongoDB]: GravelMongoClient;
};

type DatabaseOptionsClientMap = {
  [GravelDBs.MongoDB]: GravelMongoOptions;
};

function getDBProviderID<T extends GravelDBs>(
  options: { db: T } & DatabaseOptionsClientMap[T],
): string {
  switch (options.db) {
    case GravelDBs.MongoDB:
      return options.mongoUrl;
  }
}

export interface Gravel {
  getDatabaseClient<T extends GravelDBs>(
    options: {
      db: T;
    } & DatabaseOptionsClientMap[T],
  ): Promise<DatabaseClientMap[T]>;
  close(): Promise<void>;
}

/**
 * Connects to Gravel initially and returns a NatsConnection
 *
 * @param timeoutMs - Timeout in milliseconds (default: 10000)
 * @returns A Promise that resolves to a NatsConnection
 * @throws Error if connection times out
 */
async function connectToGravel(
  connectionOptions: GravelConnectionOptions,
  timeoutMs?: number,
): Promise<NatsConnection> {
  const timeout = timeoutMs ?? connectionOptions.timeoutMs ?? 10000;
  const host = connectionOptions.gravelHost ?? "127.0.0.1";
  const port = connectionOptions.gravelPort ?? 4222;
  const serverUrls = connectionOptions.gravelHost
    ? [`nats://${host}:${port}`]
    : [`nats://127.0.0.1:${port}`, `nats://localhost:${port}`];

  const natsConnection = await connect({
    servers: serverUrls,
    timeout,
    waitOnFirstConnect: false,
    maxReconnectAttempts: 0,
  });

  return natsConnection;
}

export async function intializeGravel(
  gravelOptions?: GravelConnectOptions,
): Promise<Gravel> {
  const connectionOptions = gravelOptions?.connection ?? {};

  const databaseClients: Map<string, GravelClient> = new Map();

  const createNatsConnection = async (
    timeoutMs?: number,
  ): Promise<NatsConnection> => connectToGravel(connectionOptions, timeoutMs);

  const gravelInstance: Gravel = {
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
              createNatsConnection,
              options as GravelMongoOptions,
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
        }

        databaseClients.set(existingClient.dbProviderID, existingClient);

        resolve(existingClient as DatabaseClientMap[T]);
      });
    },
    close: async (): Promise<void> => {
      for (const client of databaseClients.values()) {
        await client.close();
      }

      databaseClients.clear();

      console.log("Gravel client connections closed");
    },
  };

  return gravelInstance;
}
