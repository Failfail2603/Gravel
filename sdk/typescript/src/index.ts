import { connect } from "@nats-io/transport-node";
import type { Msg, NatsConnection, NatsError } from "nats";

export enum DBType {
  MongoDB = "mongodb",
}

export enum GravelChannels {
  DatabaseConnect = "gravel.connect",
  GravelDebug = "gravel.debug",
}

export interface DatabaseConnectRequest {
  database: string;
  clientID: string;
}

export interface DatabaseConnectResponse {
  status: string;
  database: string;
  error: string;
}

export async function connectToGravel(): Promise<NatsConnection> {
  const natsConnection = await connect();
  console.log("Connected to Gravel");

  const debugSubscription = natsConnection.subscribe(
    GravelChannels.GravelDebug,
  );

  debugSubscription.callback((err: NatsError | null, msg: Msg) => {
    console.log("Debug Error received:", err);
    console.log("Debug Message received:", msg);
  });

  return natsConnection;
}

export async function connectToDatabase(
  natsConnection: NatsConnection,
  database: DBType,
): Promise<Msg> {
  const clientID = crypto.randomUUID();

  const response = await natsConnection.request(
    GravelChannels.DatabaseConnect,
    JSON.stringify({ database, clientID } as DatabaseConnectRequest),
    {
      timeout: 5000,
      reply: GravelChannels.GravelDebug,
      noMux: true,
    },
  );

  return response.json();
}

async function startTest() {
  console.log("Starting test...");
  const natsConnection = await connectToGravel();
  const response = await connectToDatabase(natsConnection, DBType.MongoDB);
  console.log(response);
}

void startTest();
