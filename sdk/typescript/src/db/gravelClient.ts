import type { Msg, NatsConnection, NatsError } from "nats";
import { Subject } from "rxjs";
import { v4 as uuidv4 } from "uuid";
import { GravelChannels } from "../gravelChannels.js";

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

export interface GravelKeepAliveRequest {
  clientID: string;
}

export interface GravelKeepAliveResponse {
  status: string;
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

// #region Keepalive constants

const KEEPALIVE_CHECK_INTERVAL_MS = 1000 * 20;
const KEEPALIVE_REQUEST_TIMEOUT_MS = 3000;
const KEEPALIVE_MAX_FAILURES = 3;
const STALE_RECONNECT_TIMEOUT_MS = 5000;

// #endregion

// #region GravelClientBase

export interface GravelClientConfig {
  createNatsConnection: (timeoutMs?: number) => Promise<NatsConnection>;
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

export async function createGravelClient(
  config: GravelClientConfig,
): Promise<GravelClientBase> {
  const { dbProviderID, channelPrefix } = config;

  const activeSubscriptions = new Map<string, Array<GravelSubscription<any>>>();
  const initalSubscriptions = new Map<
    string,
    Array<(value: { result: Array<any> }) => void>
  >();
  const storedQueries = new Map<string, any>();
  const cachedInitialResults = new Map<string, { result: Array<any> }>();
  const suppressedInitialFullUpdates = new Map<string, number>();

  const client: GravelClientBase = {
    clientID: "",
    dbProviderID,
    debugCallback: undefined,
    async close() {
      if (isClosed) {
        return;
      }

      isClosed = true;
      clearInterval(keepaliveInterval);
      currentConnection?.close();
      currentConnection = undefined;
    },
    registerWatchQuery,
  };

  let currentClientID = "";
  let currentConnection: NatsConnection | undefined;
  let debugSubscriptionClientID: string | undefined;
  let isStale = false;
  let isReconnecting = false;
  let keepaliveInFlight = false;
  let keepaliveFailures = 0;
  let isClosed = false;

  function ensureDebugSubscription(
    natsConnection: NatsConnection,
    sessionClientID: string,
  ) {
    if (
      !client.debugCallback ||
      debugSubscriptionClientID === sessionClientID
    ) {
      return;
    }

    const debugSubscription = natsConnection.subscribe(
      GravelChannels.GravelDebug + "." + sessionClientID,
    );
    debugSubscription.callback = (err, msg) => {
      if (currentClientID !== sessionClientID) {
        return;
      }

      client.debugCallback?.(err, msg);
    };
    debugSubscriptionClientID = sessionClientID;
  }

  async function requestInitialQuery<T extends Record<string, any>>(
    natsConnection: NatsConnection,
    sessionClientID: string,
    hash: string,
    request: Record<string, any>,
  ): Promise<{ result: Array<T> }> {
    let resolveInitialQuery!: (value: { result: Array<T> }) => void;
    const initialQueryPromise = new Promise<{ result: Array<T> }>((resolve) => {
      resolveInitialQuery = resolve;
    });

    const resolver = (value: { result: Array<any> }) => {
      resolveInitialQuery(value as { result: Array<T> });
    };

    const otherWaitingInitalSubscriptions = initalSubscriptions.get(hash);
    if (otherWaitingInitalSubscriptions) {
      otherWaitingInitalSubscriptions.push(resolver);
    } else {
      initalSubscriptions.set(hash, [resolver]);
    }

    try {
      await natsConnection.request(
        "gravel.watchquery",
        JSON.stringify(request),
        {
          timeout: 5000,
          reply: GravelChannels.GravelDebug + "." + sessionClientID,
          noMux: true,
        },
      );
    } catch (error) {
      const waitingSubscriptions = initalSubscriptions.get(hash);
      if (waitingSubscriptions) {
        const filteredWaitingSubscriptions = waitingSubscriptions.filter(
          (subscription) => subscription !== resolver,
        );
        if (filteredWaitingSubscriptions.length === 0) {
          initalSubscriptions.delete(hash);
        } else {
          initalSubscriptions.set(hash, filteredWaitingSubscriptions);
        }
      }

      throw error;
    }

    return initialQueryPromise;
  }

  function attachSessionSubscriptions(
    natsConnection: NatsConnection,
    sessionClientID: string,
  ) {
    natsConnection.subscribe(
      `${GravelChannels.ClientKeepAlive}.${sessionClientID}`,
      {
        callback(err, msg) {
          if (currentClientID !== sessionClientID) {
            return;
          }

          if (err) {
            console.error("[Gravel] Client keepalive subscription error:", err);
            return;
          }

          console.log(
            "[Gravel] Received server keepalive probe for client",
            sessionClientID,
          );
          msg.respond(
            JSON.stringify({
              clientID: sessionClientID,
            } satisfies GravelKeepAliveRequest),
          );
        },
      },
    );

    natsConnection.subscribe(`${channelPrefix}.watchquery.${sessionClientID}`, {
      callback(err, msg) {
        if (currentClientID !== sessionClientID) {
          return;
        }

        if (err) {
          console.error(err);
          return;
        }

        const response = JSON.parse(
          msg.data.toString(),
        ) as GravelWatchQueryResponse;

        const activeSubscriptionsForQuery = activeSubscriptions.get(
          response.queryHash,
        );

        if (!activeSubscriptionsForQuery) {
          return;
        }

        if (response.type === "full") {
          cachedInitialResults.set(response.queryHash, {
            result: JSON.parse(response.result),
          });

          const suppressedFullUpdates =
            suppressedInitialFullUpdates.get(response.queryHash) ?? 0;
          if (suppressedFullUpdates > 0) {
            if (suppressedFullUpdates === 1) {
              suppressedInitialFullUpdates.delete(response.queryHash);
            } else {
              suppressedInitialFullUpdates.set(
                response.queryHash,
                suppressedFullUpdates - 1,
              );
            }
            return;
          }
        }

        const parsedResult =
          response.type === "full"
            ? ({ type: "full", result: JSON.parse(response.result) } satisfies {
                type: "full";
                result: Array<any>;
              })
            : ({
                type: "patch",
                result: JSON.parse(response.result),
              } satisfies {
                type: "patch";
                result: unknown;
              });

        for (const subscription of activeSubscriptionsForQuery) {
          subscription.changes.next(parsedResult);
        }
      },
    });

    natsConnection.subscribe(`${channelPrefix}.initial.${sessionClientID}`, {
      callback(err, msg) {
        if (currentClientID !== sessionClientID) {
          return;
        }

        if (err) {
          console.error(err);
          return;
        }

        const response = JSON.parse(
          Buffer.from(msg.data).toString("utf-8"),
        ) as GravelWatchQueryResponse;

        const initalWaitingQueries = initalSubscriptions.get(
          response.queryHash,
        );

        if (!initalWaitingQueries) {
          return;
        }

        const parsedResult = JSON.parse(response.result);
        for (const resolveInitialQuery of initalWaitingQueries) {
          resolveInitialQuery({ result: parsedResult });
        }

        initalSubscriptions.delete(response.queryHash);
      },
    });

    ensureDebugSubscription(natsConnection, sessionClientID);
  }

  async function createSession(connectionTimeoutMs?: number) {
    const sessionClientID = uuidv4();
    const natsConnection =
      await config.createNatsConnection(connectionTimeoutMs);
    const connectRequest = {
      ...config.connectRequest,
      clientID: sessionClientID,
    };

    await natsConnection.request(
      GravelChannels.DatabaseConnect,
      JSON.stringify(connectRequest),
      {
        timeout: 5000,
        reply: GravelChannels.GravelDebug + "." + sessionClientID,
        noMux: true,
      },
    );

    attachSessionSubscriptions(natsConnection, sessionClientID);

    return {
      sessionClientID,
      natsConnection,
    };
  }

  async function replaceSession(connectionTimeoutMs?: number) {
    const previousConnection = currentConnection;
    const { sessionClientID, natsConnection } =
      await createSession(connectionTimeoutMs);

    currentClientID = sessionClientID;
    client.clientID = sessionClientID;
    currentConnection = natsConnection;
    debugSubscriptionClientID = undefined;
    ensureDebugSubscription(natsConnection, sessionClientID);

    previousConnection?.close();
  }

  async function ensureActiveSession() {
    if (currentConnection) {
      return;
    }

    await replaceSession();
  }

  async function reissueAllQueries() {
    if (!currentConnection) {
      throw new Error("No active NATS connection available");
    }

    for (const [queryHash, storedQuery] of storedQueries) {
      const request = config.buildWatchQueryRequest(
        currentClientID,
        queryHash,
        storedQuery,
      );
      suppressedInitialFullUpdates.set(
        queryHash,
        (suppressedInitialFullUpdates.get(queryHash) ?? 0) + 1,
      );
      const initialQueryResult = await requestInitialQuery(
        currentConnection,
        currentClientID,
        queryHash,
        request,
      );

      cachedInitialResults.set(queryHash, initialQueryResult);

      const activeSubscriptionsForQuery =
        activeSubscriptions.get(queryHash) ?? [];
      for (const subscription of activeSubscriptionsForQuery) {
        subscription.changes.next(initialQueryResult.result);
      }
    }
  }

  async function recoverFromStaleConnection() {
    if (isReconnecting) {
      return;
    }

    isReconnecting = true;

    try {
      console.log(
        "[Gravel] Client is stale - creating a fresh NATS connection and rebuilding subscriptions",
      );

      await replaceSession(STALE_RECONNECT_TIMEOUT_MS);
      await reissueAllQueries();

      keepaliveFailures = 0;
      isStale = false;
      console.log(
        "[Gravel] Successfully rebuilt client session and reissued all watchqueries",
      );
    } catch (error) {
      console.error(
        "[Gravel] Stale recovery failed, will retry on next keepalive interval:",
        error,
      );
      isStale = true;
    } finally {
      isReconnecting = false;
    }
  }

  async function sendKeepAlive() {
    if (!currentConnection) {
      throw new Error("No active NATS connection available");
    }

    console.log("[Gravel] Sending client keepalive for", currentClientID);
    const keepAliveResponseMessage = await currentConnection.request(
      "gravel.keepalive",
      JSON.stringify({
        clientID: currentClientID,
      } satisfies GravelKeepAliveRequest),
      { timeout: KEEPALIVE_REQUEST_TIMEOUT_MS },
    );

    const keepAliveResponse = JSON.parse(
      keepAliveResponseMessage.data.toString(),
    ) as GravelKeepAliveResponse;

    console.log("[Gravel] Client keepalive succeeded for", currentClientID);

    return keepAliveResponse;
  }

  await replaceSession();

  const keepaliveInterval = setInterval(async () => {
    if (activeSubscriptions.size === 0) {
      return;
    }
    if (isClosed || keepaliveInFlight || isReconnecting) {
      return;
    }

    keepaliveInFlight = true;
    try {
      if (isStale) {
        await recoverFromStaleConnection();
        return;
      }

      const keepAliveResponse = await sendKeepAlive();

      if (keepAliveResponse.status === "stale") {
        console.warn(
          `[Gravel] Gravel reported client ${currentClientID} as stale, rebuilding immediately`,
        );
        isStale = true;
        keepaliveFailures = KEEPALIVE_MAX_FAILURES;
        await recoverFromStaleConnection();
        return;
      }

      keepaliveFailures = 0;
    } catch (error) {
      keepaliveFailures += 1;
      console.warn(
        `[Gravel] Client keepalive failed for ${currentClientID} (${keepaliveFailures}/${KEEPALIVE_MAX_FAILURES})`,
        error,
      );

      if (keepaliveFailures >= KEEPALIVE_MAX_FAILURES) {
        isStale = true;
        console.warn(
          "[Gravel] Marking client as stale after repeated keepalive failures",
        );
      }
    } finally {
      keepaliveInFlight = false;
    }
  }, KEEPALIVE_CHECK_INTERVAL_MS);

  async function registerWatchQuery<T extends Record<string, any>>(
    hash: string,
    storedInfo: any,
    request: Record<string, any>,
  ): Promise<GravelSubscription<T>> {
    const updateSubject = new Subject<unknown>();

    const activeSubscriptionsForQuery = activeSubscriptions.get(hash);

    const queryID = uuidv4();

    const subscription = {
      clientQueryId: queryID,
      initialQuery: { result: [] as T[] },
      changes: updateSubject,
      stop: async () => {
        console.log(
          "Stopping watchquery for client",
          currentClientID,
          "and query",
          hash,
          "with id",
          queryID,
        );
        updateSubject.complete();

        const subs = activeSubscriptions.get(hash);
        const filteredSubscriptions =
          subs?.filter((entry) => entry.clientQueryId !== queryID) ?? [];

        if (filteredSubscriptions.length > 0) {
          activeSubscriptions.set(hash, filteredSubscriptions);
        } else {
          activeSubscriptions.delete(hash);
          storedQueries.delete(hash);
          cachedInitialResults.delete(hash);

          if (currentConnection && !isStale) {
            await currentConnection.request(
              "gravel.watchquery.stop",
              JSON.stringify({
                clientID: currentClientID,
                hash,
              } satisfies GravelWatchQueryStopRequest),
              {
                timeout: 5000,
                reply: GravelChannels.GravelDebug + "." + currentClientID,
                noMux: true,
              },
            );
          }
        }

        console.log(
          "Stopped watchquery for client",
          currentClientID,
          "and query",
          hash,
          "with id",
          queryID,
        );
      },
    } satisfies GravelSubscription<T>;

    if (activeSubscriptionsForQuery && activeSubscriptionsForQuery.length > 0) {
      activeSubscriptionsForQuery.push(subscription);
      const cached = cachedInitialResults.get(hash);
      subscription.initialQuery = (cached as { result: Array<T> }) ?? {
        result: [],
      };
      return subscription;
    }

    activeSubscriptions.set(hash, [subscription]);
    storedQueries.set(hash, storedInfo);

    await ensureActiveSession();

    if (!currentConnection) {
      throw new Error("No active NATS connection available");
    }

    const sessionBoundRequest = {
      ...request,
      clientID: currentClientID,
    };

    suppressedInitialFullUpdates.set(
      hash,
      (suppressedInitialFullUpdates.get(hash) ?? 0) + 1,
    );

    const initialQueryResult = await requestInitialQuery(
      currentConnection,
      currentClientID,
      hash,
      sessionBoundRequest,
    );
    subscription.initialQuery = initialQueryResult as { result: Array<T> };

    cachedInitialResults.set(hash, initialQueryResult);

    return subscription;
  }

  return client;
}

// #endregion
