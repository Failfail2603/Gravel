import { connect, type Msg, type NatsConnection, type NatsError } from "nats";
import { type GravelClient } from "./db/gravelClient.js";
import {
  generateMongoProvider,
  type GravelMongoClient,
  type GravelMongoOptions,
} from "./db/GravelMongoClient.js";
import { GravelChannels } from "./gravelChannels.js";

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
): Promise<NatsConnection> {
  const timeout = connectionOptions.timeoutMs ?? 10000;
  const host = connectionOptions.gravelHost ?? "localhost";
  const port = connectionOptions.gravelPort ?? 4222;

  const timeoutPromise = new Promise<never>((_, reject) => {
    setTimeout(() => {
      reject(
        new Error(
          `Gravel connection timed out after ${timeout}ms. Make sure you Gravel is running and you are using the correct connection string.`,
        ),
      );
    }, timeout);
  });

  const natsConnection = await Promise.race([
    connect({ servers: [`${host}:${port}`] }),
    timeoutPromise,
  ]);

  return natsConnection;
}

export async function intializeGravel(
  gravelOptions?: GravelConnectOptions,
): Promise<Gravel> {
  const connectionOptions = gravelOptions?.connection ?? {};

  const natsConnection = await connectToGravel(connectionOptions);

  const databaseClients: Map<string, GravelClient> = new Map();

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
              natsConnection,
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

          const debugSubscription = natsConnection.subscribe(
            GravelChannels.GravelDebug + "." + existingClient.clientID,
          );
          debugSubscription.callback = gravelOptions?.debugChannelCallback;
        }

        databaseClients.set(existingClient.dbProviderID, existingClient);

        resolve(existingClient as DatabaseClientMap[T]);
      });
    },
    close: async (): Promise<void> => {
      // Clear all database clients
      databaseClients.clear();

      // Drain and close the NATS connection
      await natsConnection.drain();

      console.log("NATS connection closed");
    },
  };

  return gravelInstance;
}
