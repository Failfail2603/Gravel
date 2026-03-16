import type { Msg, NatsConnection, NatsError } from "nats";
import { Subject } from "rxjs";
import { GravelChannels } from "./GravelChannels.js";
import {
  buildKeepAliveRequest,
  isServerStaleKeepAliveResponse,
  KEEPALIVE_CHECK_INTERVAL_MS,
  KEEPALIVE_MAX_FAILURES,
  KEEPALIVE_REQUEST_TIMEOUT_MS,
  STALE_RECONNECT_TIMEOUT_MS,
  type GravelKeepAliveResponse,
} from "./gravelKeepalive.js";
import { createQueryRegistry, type QueryRegistry } from "./QueryRegistry.js";
import { createSession, type GravelNatsConfig } from "./session.js";

// #region Public interface

export interface GravelClient {
  // the unique client id for gravel. Consist of a uuid to differentiate a container instance of the client. This will be unique in the server environment to multiplex between different clients
  clientID: string;

  // this id is a unique key which should be unique to the project itself. This is used to multiplex between different database providers on the same client. Can be something like "mongodb + url" or "redis + url".
  // we can then hold a singleton to the gravel database withz the same url
  // ! this is not the same as the clientID as this can be the same for multiple containers hosting the same projects in a horizontal setup
  dbProviderID: string;

  // debug callback
  debugCallback?: (err: NatsError | null, msg: Msg) => void;

  close(): Promise<void>;
}

// #endregion

// #region Generic watch query types

export interface GravelWatchQueryResponse {
  // the hash of the query to identify the inital query on the client and give back the correct result
  queryHash: string;

  // the type of the result. can be "full" or "patch"
  type: "full" | "patch";

  // the result. can be a full document or a json patch depending on the type
  result: string;
}

export interface GravelWatchQueryStopRequest {
  clientID: string;
  hash: string;
}

export interface GravelSubscription<
  T extends Record<string, any> = Record<string, any>,
> {
  // this id is only used on the client to stop the correct observable. Gravel does not care which observable belongs to which webclient connected. We cannot just end all queries under a hash as gravel does not know which client is connected to which query.
  clientQueryId: string;
  initialQuery: { result: Array<T> };
  changes: Subject<unknown>;
  stop: () => Promise<void>;
}

// #endregion

// #region GravelClientBase

export interface GravelClientConfig {
  natsConfig: GravelNatsConfig;
  dbProviderID: string;
  // NATS channel prefix for this provider (e.g. "gravel.mongo")
  channelPrefix: string;
  // provider-specific connect request payload (clientID will be injected)
  connectRequest: Record<string, any>;
  buildWatchQueryRequest: (
    clientID: string,
    hash: string,
    storedInfo: any,
  ) => Record<string, any>;
}

export interface GravelClientBase extends GravelClient {
  // registers a watch query with Gravel, handling deduplication, keepalive, and reconnection
  registerWatchQuery<T extends Record<string, any>>(
    hash: string,
    storedInfo: any,
    request: Record<string, any>,
  ): Promise<GravelSubscription<T>>;
}

// #endregion

// #region Internal state

interface ClientState {
  clientID: string;
  connection: NatsConnection | undefined;
  debugCallback: ((err: NatsError | null, msg: Msg) => void) | undefined;
  isClosed: boolean;
  isStale: boolean;
  isReconnecting: boolean;
  keepaliveInFlight: boolean;
  keepaliveFailures: number;
}

function createInitialState(): ClientState {
  return {
    clientID: "",
    connection: undefined,
    debugCallback: undefined,
    isClosed: false,
    isStale: false,
    isReconnecting: false,
    keepaliveInFlight: false,
    keepaliveFailures: 0,
  };
}

// #endregion

// #region Session management

async function replaceSession(
  config: GravelClientConfig,
  state: ClientState,
  registry: QueryRegistry,
  connectionTimeoutMs?: number,
): Promise<void> {
  const previousConnection = state.connection;

  const session = await createSession(
    config,
    registry,
    () => state.debugCallback,
    connectionTimeoutMs,
  );

  state.clientID = session.sessionClientID;
  state.connection = session.natsConnection;

  previousConnection?.close();
}

async function ensureActiveSession(
  config: GravelClientConfig,
  state: ClientState,
  registry: QueryRegistry,
): Promise<void> {
  if (state.connection) return;
  await replaceSession(config, state, registry);
}

// #endregion

// #region Initial query request

async function requestInitialQuery<T extends Record<string, any>>(
  connection: NatsConnection,
  clientID: string,
  hash: string,
  request: Record<string, any>,
  registry: QueryRegistry,
): Promise<{ result: Array<T> }> {
  let resolveInitialQuery!: (value: { result: Array<T> }) => void;

  const initialQueryPromise = new Promise<{ result: Array<T> }>((resolve) => {
    resolveInitialQuery = resolve;
  });

  const resolver = (value: { result: Array<any> }) => {
    resolveInitialQuery(value as { result: Array<T> });
  };

  registry.registerInitialSubscriber(hash, resolver);

  try {
    await connection.request("gravel.watchquery", JSON.stringify(request), {
      timeout: 5000,
      reply: `${GravelChannels.GravelDebug}.${clientID}`,
      noMux: true,
    });
  } catch (error) {
    registry.removeInitialSubscriber(hash, resolver);
    throw error;
  }

  return initialQueryPromise;
}

// #endregion

// #region Reconnection

async function reissueAllQueries(
  config: GravelClientConfig,
  state: ClientState,
  registry: QueryRegistry,
): Promise<void> {
  if (!state.connection) {
    throw new Error("No active NATS connection available");
  }

  for (const [queryHash, storedQuery] of registry.getStoredQueries()) {
    const request = config.buildWatchQueryRequest(
      state.clientID,
      queryHash,
      storedQuery,
    );

    const initialQueryResult = await requestInitialQuery(
      state.connection,
      state.clientID,
      queryHash,
      request,
      registry,
    );

    registry.setCachedResult(queryHash, initialQueryResult);
    registry.broadcastChange(queryHash, initialQueryResult.result);
  }
}

async function recoverFromStaleConnection(
  config: GravelClientConfig,
  state: ClientState,
  registry: QueryRegistry,
): Promise<void> {
  if (state.isReconnecting) return;

  state.isReconnecting = true;
  try {
    console.log(
      "[Gravel] Client is stale - creating a fresh NATS connection and rebuilding subscriptions",
    );

    await replaceSession(config, state, registry, STALE_RECONNECT_TIMEOUT_MS);
    await reissueAllQueries(config, state, registry);

    state.keepaliveFailures = 0;
    state.isStale = false;
    console.log(
      "[Gravel] Successfully rebuilt client session and reissued all watchqueries",
    );
  } catch (error) {
    console.error(
      "[Gravel] Stale recovery failed, will retry on next keepalive interval:",
    );
    state.isStale = true;
  } finally {
    state.isReconnecting = false;
  }
}

// #endregion

// #region Keepalive

async function sendKeepAlive(
  state: ClientState,
): Promise<GravelKeepAliveResponse> {
  if (!state.connection) {
    throw new Error("No active NATS connection available");
  }

  console.log("[Gravel] Sending client keepalive for", state.clientID);

  const responseMessage = await state.connection.request(
    "gravel.keepalive",
    JSON.stringify(buildKeepAliveRequest(state.clientID)),
    { timeout: KEEPALIVE_REQUEST_TIMEOUT_MS },
  );

  const response = JSON.parse(
    responseMessage.data.toString(),
  ) as GravelKeepAliveResponse;

  console.log("[Gravel] Client keepalive succeeded for", state.clientID);

  return response;
}

async function runKeepalivetick(
  config: GravelClientConfig,
  state: ClientState,
  registry: QueryRegistry,
): Promise<void> {
  if (!registry.hasActiveSubscriptions()) return;
  if (state.isClosed || state.keepaliveInFlight || state.isReconnecting) return;

  state.keepaliveInFlight = true;
  try {
    if (state.isStale) {
      await recoverFromStaleConnection(config, state, registry);
      return;
    }

    const response = await sendKeepAlive(state);

    if (isServerStaleKeepAliveResponse(response)) {
      console.warn(
        `[Gravel] Gravel reported client ${state.clientID} as stale, rebuilding immediately`,
      );
      state.isStale = true;
      state.keepaliveFailures = KEEPALIVE_MAX_FAILURES;
      await recoverFromStaleConnection(config, state, registry);
      return;
    }

    state.keepaliveFailures = 0;
  } catch (error) {
    state.keepaliveFailures += 1;
    console.warn(
      `[Gravel] Client keepalive failed for ${state.clientID} (${state.keepaliveFailures}/${KEEPALIVE_MAX_FAILURES})`,
    );

    if (state.keepaliveFailures >= KEEPALIVE_MAX_FAILURES) {
      state.isStale = true;
      console.warn(
        "[Gravel] Marking client as stale after repeated keepalive failures",
      );
    }
  } finally {
    state.keepaliveInFlight = false;
  }
}

// #endregion

// #region Watch query stop

async function stopWatchQueryOnServer(
  state: ClientState,
  hash: string,
): Promise<void> {
  if (!state.connection || state.isStale) return;

  await state.connection.request(
    "gravel.watchquery.stop",
    JSON.stringify({
      clientID: state.clientID,
      hash,
    } satisfies GravelWatchQueryStopRequest),
    {
      timeout: 5000,
      reply: `${GravelChannels.GravelDebug}.${state.clientID}`,
      noMux: true,
    },
  );
}

// #endregion

// #region Public factory

export async function createGravelClient(
  config: GravelClientConfig,
): Promise<GravelClientBase> {
  const state = createInitialState();
  const registry = createQueryRegistry();

  await replaceSession(config, state, registry);

  const keepaliveInterval = setInterval(() => {
    runKeepalivetick(config, state, registry).catch((error) => {
      console.error("[Gravel] Unexpected error in keepalive tick:", error);
    });
  }, KEEPALIVE_CHECK_INTERVAL_MS);

  async function registerWatchQuery<T extends Record<string, any>>(
    hash: string,
    storedInfo: any,
    request: Record<string, any>,
  ): Promise<GravelSubscription<T>> {
    const existingSubscriptions = registry.getActiveSubscriptions(hash);

    if (existingSubscriptions && existingSubscriptions.length > 0) {
      const subscription = registry.createSubscription<T>(
        hash,
        storedInfo,
        (h) => stopWatchQueryOnServer(state, h),
      );
      subscription.initialQuery = (registry.getCachedResult(hash) as {
        result: Array<T>;
      }) ?? { result: [] };
      return subscription;
    }

    await ensureActiveSession(config, state, registry);

    if (!state.connection) {
      throw new Error("No active NATS connection available");
    }

    const subscription = registry.createSubscription<T>(hash, storedInfo, (h) =>
      stopWatchQueryOnServer(state, h),
    );

    const sessionBoundRequest = { ...request, clientID: state.clientID };

    const initialQueryResult = await requestInitialQuery<T>(
      state.connection,
      state.clientID,
      hash,
      sessionBoundRequest,
      registry,
    );

    subscription.initialQuery = initialQueryResult;
    registry.setCachedResult(hash, initialQueryResult);

    return subscription;
  }

  return {
    get clientID() {
      return state.clientID;
    },
    dbProviderID: config.dbProviderID,
    get debugCallback() {
      return state.debugCallback;
    },
    set debugCallback(cb) {
      state.debugCallback = cb;
    },
    async close() {
      if (state.isClosed) return;
      state.isClosed = true;
      clearInterval(keepaliveInterval);
      state.connection?.close();
      state.connection = undefined;
    },
    registerWatchQuery,
  };
}

// #endregion
