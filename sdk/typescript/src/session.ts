import { connect, type Msg, type NatsConnection, type NatsError } from "nats";
import { v4 as uuidv4 } from "uuid";
import { GravelChannels } from "./GravelChannels.js";
import type {
  GravelClientConfig,
  GravelWatchQueryResponse,
} from "./GravelClient.js";
import { KEEPALIVE_CHECK_INTERVAL_MS } from "./gravelKeepalive.js";
import type { QueryRegistry } from "./QueryRegistry.js";

// #region NATS connection

export interface GravelNatsConfig {
  gravelHost?: string;
  gravelPort?: number;
  timeoutMs?: number;
}

async function connectToNats(
  natsConfig: GravelNatsConfig,
  timeoutMs?: number,
): Promise<NatsConnection> {
  const timeout = timeoutMs ?? natsConfig.timeoutMs ?? 10000;
  const host = natsConfig.gravelHost ?? "127.0.0.1";
  const port = natsConfig.gravelPort ?? 4222;
  const serverUrls = natsConfig.gravelHost
    ? [`nats://${host}:${port}`]
    : [`nats://127.0.0.1:${port}`, `nats://localhost:${port}`];

  return connect({
    servers: serverUrls,
    timeout,
    waitOnFirstConnect: false,
    maxReconnectAttempts: 0,
  });
}

// #endregion

// #region Types

export interface SessionHandlers {
  onWatchQueryMessage: (response: GravelWatchQueryResponse) => void;
  onInitialQueryMessage: (response: GravelWatchQueryResponse) => void;
  debugCallback: () => ((err: NatsError | null, msg: Msg) => void) | undefined;
}

export interface ActiveSession {
  sessionClientID: string;
  natsConnection: NatsConnection;
}

// #endregion

// #region Session NATS subscriptions

function attachWatchQuerySubscription(
  natsConnection: NatsConnection,
  channelPrefix: string,
  session: ActiveSession,
  registry: QueryRegistry,
): void {
  natsConnection.subscribe(
    `${channelPrefix}.watchquery.${session.sessionClientID}`,
    {
      callback(err, msg) {
        if (err) {
          console.error("[Gravel] Watch query subscription error:", err);
          return;
        }

        const response = JSON.parse(
          msg.data.toString(),
        ) as GravelWatchQueryResponse;
        const { queryHash } = response;

        if (!registry.getActiveSubscriptions(queryHash)) {
          return;
        }

        const parsed = JSON.parse(response.result);

        if (response.type === "full") {
          registry.setCachedResult(queryHash, { result: parsed });
        }

        registry.broadcastChange(queryHash, {
          type: response.type,
          result: parsed,
        });
      },
    },
  );
}

function attachInitialQuerySubscription(
  natsConnection: NatsConnection,
  channelPrefix: string,
  session: ActiveSession,
  registry: QueryRegistry,
): void {
  natsConnection.subscribe(
    `${channelPrefix}.initial.${session.sessionClientID}`,
    {
      callback(err, msg) {
        if (err) {
          console.error("[Gravel] Initial query subscription error:", err);
          return;
        }

        const response = JSON.parse(
          Buffer.from(msg.data).toString("utf-8"),
        ) as GravelWatchQueryResponse;

        if (!registry.hasInitialSubscribers(response.queryHash)) {
          return;
        }

        registry.resolveInitialSubscribers(
          response.queryHash,
          JSON.parse(response.result),
        );
      },
    },
  );
}

function attachDebugSubscription(
  natsConnection: NatsConnection,
  session: ActiveSession,
  getDebugCallback: () =>
    | ((err: NatsError | null, msg: Msg) => void)
    | undefined,
): void {
  const debugSub = natsConnection.subscribe(
    `${GravelChannels.GravelDebug}.${session.sessionClientID}`,
  );

  debugSub.callback = (err, msg) => {
    const cb = getDebugCallback();
    cb?.(err, msg);
  };
}

export function attachSessionSubscriptions(
  natsConnection: NatsConnection,
  channelPrefix: string,
  session: ActiveSession,
  registry: QueryRegistry,
  getDebugCallback: () =>
    | ((err: NatsError | null, msg: Msg) => void)
    | undefined,
): void {
  attachWatchQuerySubscription(
    natsConnection,
    channelPrefix,
    session,
    registry,
  );
  attachInitialQuerySubscription(
    natsConnection,
    channelPrefix,
    session,
    registry,
  );
  attachDebugSubscription(natsConnection, session, getDebugCallback);
}

// #endregion

// #region Session lifecycle

export async function createSession(
  config: GravelClientConfig,
  registry: QueryRegistry,
  getDebugCallback: () =>
    | ((err: NatsError | null, msg: Msg) => void)
    | undefined,
  connectionTimeoutMs?: number,
): Promise<ActiveSession> {
  const sessionClientID = uuidv4();
  const natsConnection = await connectToNats(
    config.natsConfig,
    connectionTimeoutMs,
  );

  const connectRequest = {
    ...config.connectRequest,
    clientID: sessionClientID,
    keepAliveIntervalMs: KEEPALIVE_CHECK_INTERVAL_MS,
  };

  await natsConnection.request(
    GravelChannels.DatabaseConnect,
    JSON.stringify(connectRequest),
    {
      timeout: 5000,
      reply: `${GravelChannels.GravelDebug}.${sessionClientID}`,
      noMux: true,
    },
  );

  const session: ActiveSession = { sessionClientID, natsConnection };

  attachSessionSubscriptions(
    natsConnection,
    config.channelPrefix,
    session,
    registry,
    getDebugCallback,
  );

  return session;
}

// #endregion
