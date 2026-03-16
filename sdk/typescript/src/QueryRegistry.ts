import { Subject } from "rxjs";
import { v4 as uuidv4 } from "uuid";
import type {
  GravelSubscription,
  GravelWatchQueryResponse,
} from "./GravelClient.js";

// #region Types

export interface QueryRegistryHandlers {
  onWatchQueryMessage: (response: GravelWatchQueryResponse) => void;
  onInitialQueryMessage: (response: GravelWatchQueryResponse) => void;
}

export interface QueryRegistry {
  hasActiveSubscriptions: () => boolean;
  getStoredQueries: () => ReadonlyMap<string, any>;
  getCachedResult: (hash: string) => { result: Array<any> } | undefined;
  getActiveSubscriptions: (
    hash: string,
  ) => Array<GravelSubscription<any>> | undefined;

  createSubscription: <T extends Record<string, any>>(
    hash: string,
    storedInfo: any,
    onLastSubscriberStopped: (hash: string) => Promise<void>,
  ) => GravelSubscription<T>;
  resolveInitialSubscribers: (hash: string, result: Array<any>) => void;
  registerInitialSubscriber: (
    hash: string,
    resolve: (value: { result: Array<any> }) => void,
  ) => void;
  removeInitialSubscriber: (
    hash: string,
    resolve: (value: { result: Array<any> }) => void,
  ) => void;
  hasInitialSubscribers: (hash: string) => boolean;

  setCachedResult: (hash: string, result: { result: Array<any> }) => void;
  broadcastChange: (hash: string, change: unknown) => void;

  removeSubscription: (hash: string, clientQueryId: string) => boolean;
  clearQuery: (hash: string) => void;
}

// #endregion

// #region Factory

export function createQueryRegistry(): QueryRegistry {
  const activeSubscriptions = new Map<string, Array<GravelSubscription<any>>>();
  const initialSubscriptions = new Map<
    string,
    Array<(value: { result: Array<any> }) => void>
  >();
  const storedQueries = new Map<string, any>();
  const cachedResults = new Map<string, { result: Array<any> }>();
  function createSubscription<T extends Record<string, any>>(
    hash: string,
    storedInfo: any,
    onLastSubscriberStopped: (hash: string) => Promise<void>,
  ): GravelSubscription<T> {
    const queryID = uuidv4();
    const updateSubject = new Subject<unknown>();

    const subscription: GravelSubscription<T> = {
      clientQueryId: queryID,
      initialQuery: { result: [] as T[] },
      changes: updateSubject,
      stop: async () => {
        updateSubject.complete();
        const wasLast = removeSubscription(hash, queryID);
        if (wasLast) {
          await onLastSubscriberStopped(hash);
        }
      },
    };

    storedQueries.set(hash, storedInfo);

    const existing = activeSubscriptions.get(hash);
    if (existing) {
      existing.push(subscription);
    } else {
      activeSubscriptions.set(hash, [subscription]);
    }

    return subscription;
  }

  function removeSubscription(hash: string, clientQueryId: string): boolean {
    const subs = activeSubscriptions.get(hash);
    if (!subs) return false;

    const remaining = subs.filter((s) => s.clientQueryId !== clientQueryId);
    const isLastSubscriber = remaining.length === 0;

    if (isLastSubscriber) {
      activeSubscriptions.delete(hash);
      storedQueries.delete(hash);
      cachedResults.delete(hash);
    } else {
      activeSubscriptions.set(hash, remaining);
    }

    return isLastSubscriber;
  }

  function clearQuery(hash: string): void {
    activeSubscriptions.delete(hash);
    storedQueries.delete(hash);
    cachedResults.delete(hash);
    initialSubscriptions.delete(hash);
  }

  function broadcastChange(hash: string, change: unknown): void {
    const subs = activeSubscriptions.get(hash);
    if (!subs) return;
    for (const sub of subs) {
      sub.changes.next(change);
    }
  }

  function registerInitialSubscriber(
    hash: string,
    resolve: (value: { result: Array<any> }) => void,
  ): void {
    const existing = initialSubscriptions.get(hash);
    if (existing) {
      existing.push(resolve);
    } else {
      initialSubscriptions.set(hash, [resolve]);
    }
  }

  function removeInitialSubscriber(
    hash: string,
    resolve: (value: { result: Array<any> }) => void,
  ): void {
    const existing = initialSubscriptions.get(hash);
    if (!existing) return;

    const remaining = existing.filter((r) => r !== resolve);
    if (remaining.length === 0) {
      initialSubscriptions.delete(hash);
    } else {
      initialSubscriptions.set(hash, remaining);
    }
  }

  function resolveInitialSubscribers(hash: string, result: Array<any>): void {
    const resolvers = initialSubscriptions.get(hash);
    if (!resolvers) return;

    for (const resolve of resolvers) {
      resolve({ result });
    }
    initialSubscriptions.delete(hash);
  }

  return {
    hasActiveSubscriptions: () => activeSubscriptions.size > 0,
    getStoredQueries: () => storedQueries,
    getCachedResult: (hash) => cachedResults.get(hash),
    getActiveSubscriptions: (hash) => activeSubscriptions.get(hash),

    createSubscription,
    resolveInitialSubscribers,
    registerInitialSubscriber,
    removeInitialSubscriber,
    hasInitialSubscribers: (hash) => initialSubscriptions.has(hash),

    setCachedResult: (hash, result) => cachedResults.set(hash, result),
    broadcastChange,

    removeSubscription,
    clearQuery,
  };
}

// #endregion
