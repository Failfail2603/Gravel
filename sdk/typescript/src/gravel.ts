import { type Msg, type NatsError } from "nats";
import { type GravelClient } from "./GravelClient.js";
import {
  generateMongoProvider,
  type GravelMongoClient,
  type GravelMongoOptions,
} from "./db/GravelMongoClient.js";
import type { GravelNatsConfig } from "./session.js";
export { watchQueryToObservable } from "./watchQueryObservable.js";
export type { WatchQueryResult } from "./watchQueryObservable.js";

export enum GravelDBs {
  MongoDB = "mongodb",
}

export interface GravelConnectOptions {
  debugChannelCallback?: (err: NatsError | null, msg: Msg) => void;
  connection: GravelNatsConfig;
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

export async function initializeGravel(
  gravelOptions?: GravelConnectOptions,
): Promise<Gravel> {
  const natsConfig: GravelNatsConfig = gravelOptions?.connection ?? {};

  const databaseClients: Map<string, GravelClient> = new Map();

  const gravelInstance: Gravel = {
    getDatabaseClient: async <T extends GravelDBs>(
      options: {
        db: T;
      } & DatabaseOptionsClientMap[T],
    ): Promise<DatabaseClientMap[T]> => {
      // we make sure we only create one client per type and connection
      const dbProviderID = getDBProviderID(options);

      const existingClient = databaseClients.get(dbProviderID) as
        | DatabaseClientMap[T]
        | undefined;

      if (existingClient) {
        return existingClient as unknown as DatabaseClientMap[T];
      }

      let newClient: DatabaseClientMap[T];

      switch (options.db) {
        case GravelDBs.MongoDB:
          newClient = (await generateMongoProvider(
            natsConfig,
            options as GravelMongoOptions,
          )) as DatabaseClientMap[T];
          break;
        default:
          throw new Error(`Unsupported database type: ${options.db}`);
      }

      // if the developer specifies a debug callback we subscribe to the debug channel and forward the messages to the callback
      if (gravelOptions?.debugChannelCallback) {
        newClient.debugCallback = gravelOptions.debugChannelCallback;
      }

      databaseClients.set(newClient.dbProviderID, newClient);

      return newClient;
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
