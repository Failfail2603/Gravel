import patch, { type Operation } from "fast-json-patch";
import type { FindOptions } from "mongodb";
import type { Subscription } from "rxjs";
import { getGravelConnection, GravelDBs } from "../gravel.js";
import { MONGO_URL } from "./config.js";
import { getMongoClient } from "./mongoClient.js";
import { NOOP_CHANGE, watchQuery as oldWatchQueryFn } from "./oldWatchQuery.js";

export interface ConfusionMatrix {
  truePositive: number;
  trueNegative: number;
  falsePositive: number;
  falseNegative: number;
}

export interface WatcherState {
  currentData: any[];
  lastUpdateReceived: boolean;
  updateCount: number;
  lastUpdateTimestamp: number | null;
  lastUpdateWasNoop: boolean;
}

export interface ExperimentWatchers {
  gravelState: WatcherState;
  oldWatchQueryState: WatcherState;
  startWatching: (
    collectionName: string,
    query: Record<string, any>,
    options: FindOptions,
  ) => Promise<void>;
  stopWatching: () => Promise<void>;
  waitForUpdates: (timeoutMs?: number) => Promise<void>;
  resetUpdateFlags: () => void;
  getGroundTruth: (
    collectionName: string,
    query: Record<string, any>,
    options: FindOptions,
  ) => Promise<any[]>;
}

let gravel: Awaited<ReturnType<typeof getGravelConnection>> | null = null;
let gravelMongoClient: any = null;
let stopGravelWatchQuery: (() => Promise<void>) | null = null;
let gravelSubscription: Subscription | null = null;
let oldWatchQuerySubscription: Subscription | null = null;

const gravelState: WatcherState = {
  currentData: [],
  lastUpdateReceived: false,
  updateCount: 0,
  lastUpdateTimestamp: null,
  lastUpdateWasNoop: false,
};

const oldWatchQueryState: WatcherState = {
  currentData: [],
  lastUpdateReceived: false,
  updateCount: 0,
  lastUpdateTimestamp: null,
  lastUpdateWasNoop: false,
};

// Promise resolvers for awaiting updates
let gravelUpdateResolver: (() => void) | null = null;
let oldWatchQueryUpdateResolver: (() => void) | null = null;

async function initializeGravel(): Promise<void> {
  if (!gravel) {
    gravel = await getGravelConnection({ timeoutMs: 30000 });
  }
  if (!gravelMongoClient) {
    gravelMongoClient = await gravel.getDatabaseClient({
      db: GravelDBs.MongoDB,
      mongoUrl: MONGO_URL,
    });
  }
}

async function startWatching(
  collectionName: string,
  query: Record<string, any>,
  options: FindOptions,
): Promise<void> {
  // Stop any existing watchers
  await stopWatching();

  // Initialize Gravel
  await initializeGravel();

  // Reset states
  gravelState.currentData = [];
  gravelState.lastUpdateReceived = false;
  gravelState.updateCount = 0;
  gravelState.lastUpdateWasNoop = false;
  oldWatchQueryState.currentData = [];
  oldWatchQueryState.lastUpdateReceived = false;
  oldWatchQueryState.updateCount = 0;
  oldWatchQueryState.lastUpdateWasNoop = false;

  // Start Gravel watchQuery
  const { initialQuery, changes, stop } = await gravelMongoClient.watchQuery(
    collectionName,
    query,
    options,
  );
  stopGravelWatchQuery = stop;

  // Set initial data for Gravel
  gravelState.currentData = initialQuery.result || initialQuery;
  console.log(
    `Gravel initial data: ${gravelState.currentData.length} documents`,
  );

  // Subscribe to Gravel changes
  gravelSubscription = changes.subscribe((patches: Operation[]) => {
    // Always mark that we received an update (even if noop)
    gravelState.lastUpdateReceived = true;
    gravelState.updateCount++;
    gravelState.lastUpdateTimestamp = Date.now();

    if (patches.length > 0) {
      // Apply patches to current data
      const currentDoc = { result: gravelState.currentData };
      const patchResult = patch.applyPatch(currentDoc, patches, false, false);
      gravelState.currentData = patchResult.newDocument.result;
      gravelState.lastUpdateWasNoop = false;
    } else {
      // If patches.length === 0, it's a noop - data stays the same
      gravelState.lastUpdateWasNoop = true;
    }

    // Resolve waiting promise
    if (gravelUpdateResolver) {
      gravelUpdateResolver();
      gravelUpdateResolver = null;
    }
  });

  // Start oldWatchQuery
  const oldWatchQuery$ = oldWatchQueryFn(collectionName, query, options);

  // Track if we've received initial data
  let isFirstEmit = true;

  oldWatchQuerySubscription = oldWatchQuery$.subscribe({
    next: (data) => {
      if (isFirstEmit) {
        // This is the initial query result
        if (data === NOOP_CHANGE) {
          console.error("OldWatchQuery: Unexpected NOOP_CHANGE as first emit");
          return;
        }
        oldWatchQueryState.currentData = data;
        console.log(`OldWatchQuery initial data: ${data.length} documents`);
        isFirstEmit = false;
      } else {
        // Always mark that we received an update (even if noop)
        oldWatchQueryState.lastUpdateReceived = true;
        oldWatchQueryState.updateCount++;
        oldWatchQueryState.lastUpdateTimestamp = Date.now();

        // Check if this is a noop change
        if (data === NOOP_CHANGE) {
          // Change was filtered out - don't update data
          console.log(`OldWatchQuery noop change`);
          oldWatchQueryState.lastUpdateWasNoop = true;
          // Data stays the same for noops
        } else {
          // This is a real update with new data
          oldWatchQueryState.currentData = data;
          oldWatchQueryState.lastUpdateWasNoop = false;
        }

        // Resolve waiting promise
        if (oldWatchQueryUpdateResolver) {
          oldWatchQueryUpdateResolver();
          oldWatchQueryUpdateResolver = null;
        }
      }
    },
    error: (err) => {
      console.error("OldWatchQuery error:", err);
    },
  });

  // Give oldWatchQuery time to receive initial data
  await new Promise((resolve) => setTimeout(resolve, 500));
}

async function stopWatching(): Promise<void> {
  console.log("Stopping watchers...");

  // Stop Gravel subscription first
  if (gravelSubscription && !gravelSubscription.closed) {
    gravelSubscription.unsubscribe();
    gravelSubscription = null;
  }

  // Stop Gravel watchQuery on server side
  if (stopGravelWatchQuery) {
    await stopGravelWatchQuery();
    stopGravelWatchQuery = null;
  }

  // Stop oldWatchQuery subscription
  if (oldWatchQuerySubscription && !oldWatchQuerySubscription.closed) {
    oldWatchQuerySubscription.unsubscribe();
    oldWatchQuerySubscription = null;
  }

  console.log("Watchers stopped");
}

async function waitForUpdates(timeoutMs: number = 5000): Promise<void> {
  // Wait for Gravel update (it always sends a response - patch or noop)
  const gravelPromise = new Promise<void>((resolve) => {
    if (gravelState.lastUpdateReceived) {
      resolve();
    } else {
      gravelUpdateResolver = resolve;
    }
  });

  // For oldWatchQuery, we need to wait a bit since it may not send anything (noop case)
  // We use a short timeout since oldWatchQuery only emits when it thinks there's a change
  const oldWatchQueryPromise = new Promise<void>((resolve) => {
    if (oldWatchQueryState.lastUpdateReceived) {
      resolve();
    } else {
      oldWatchQueryUpdateResolver = resolve;
      // Set a timeout for oldWatchQuery noop detection
      setTimeout(() => {
        if (oldWatchQueryUpdateResolver) {
          oldWatchQueryUpdateResolver();
          oldWatchQueryUpdateResolver = null;
        }
      }, 1000); // Give it 1 second to respond
    }
  });

  // Wait for both with overall timeout
  await Promise.race([
    Promise.all([gravelPromise, oldWatchQueryPromise]),
    new Promise<void>((resolve) => setTimeout(resolve, timeoutMs)),
  ]);
}

function resetUpdateFlags(): void {
  gravelState.lastUpdateReceived = false;
  gravelState.lastUpdateTimestamp = null;
  gravelState.lastUpdateWasNoop = false;
  oldWatchQueryState.lastUpdateReceived = false;
  oldWatchQueryState.lastUpdateTimestamp = null;
  oldWatchQueryState.lastUpdateWasNoop = false;
}

async function getGroundTruth(
  collectionName: string,
  query: Record<string, any>,
  options: FindOptions,
): Promise<any[]> {
  const client = await getMongoClient();
  const collection = client.db().collection(collectionName);

  let cursor = collection.find(query);

  if (options.sort) {
    cursor = cursor.sort(options.sort);
  }
  if (options.skip) {
    cursor = cursor.skip(options.skip);
  }
  if (options.limit) {
    cursor = cursor.limit(options.limit);
  }
  if (options.projection) {
    cursor = cursor.project(options.projection);
  }

  return cursor.toArray();
}

export async function closeWatchers(): Promise<void> {
  await stopWatching();

  if (gravel) {
    await gravel.close();
    gravel = null;
    gravelMongoClient = null;
  }
}

export function createExperimentWatchers(): ExperimentWatchers {
  return {
    gravelState,
    oldWatchQueryState,
    startWatching,
    stopWatching,
    waitForUpdates,
    resetUpdateFlags,
    getGroundTruth,
  };
}

/**
 * Compare implementation state against ground truth and determine confusion matrix outcome
 */
export function classifyOutcome(
  stateBefore: any[],
  stateAfter: any[],
  groundTruth: any[],
  receivedUpdate: boolean,
  wasNoop: boolean,
): "TP" | "TN" | "FP" | "FN" {
  // Did the ground truth actually change?
  const groundTruthChanged = !arraysEqual(stateBefore, groundTruth);

  if (groundTruthChanged && !wasNoop) {
    // Check if the update was correct
    if (arraysEqual(stateAfter, groundTruth)) {
      return "TP"; // Correctly reported change
    } else {
      return "FP"; // Reported change but got wrong result (still counts as FP - incorrect update)
    }
  } else if (groundTruthChanged && wasNoop) {
    return "FN"; // Missed a change (includes noops when ground truth changed)
  } else if (!groundTruthChanged && !wasNoop) {
    return "FP"; // Reported change when none occurred
  } else if (!groundTruthChanged && wasNoop) {
    return "TN"; // Correctly identified no change (noop when ground truth didn't change)
  } else {
    return "TN"; // Correctly ignored (no update received, no change occurred)
  }
}

/**
 * Deep comparison of two values with support for unordered object keys and nested structures
 */
/**
 * Normalize ObjectID to string for comparison
 */
function normalizeValue(value: any): any {
  // Handle ObjectID with $oid property
  if (value && typeof value === "object" && value.$oid) {
    return value.$oid;
  }
  // Handle MongoDB ObjectId instances
  if (value && typeof value === "object" && value._bsontype === "ObjectId") {
    return value.toString();
  }
  return value;
}

function deepEqual(a: any, b: any, path: string = "root"): boolean {
  // Normalize potential ObjectIDs
  a = normalizeValue(a);
  b = normalizeValue(b);

  // Handle null and undefined
  if (a === b) return true;
  if (a == null || b == null) {
    console.log(
      `[deepEqual] Null/undefined mismatch at ${path}: a=${a}, b=${b}`,
    );
    return false;
  }

  // Handle Date objects before type check
  if (a instanceof Date && b instanceof Date) {
    const equal = a.getTime() === b.getTime();
    if (!equal) {
      console.log(
        `[deepEqual] Date mismatch at ${path}: a=${a.toISOString()}, b=${b.toISOString()}`,
      );
    }
    return equal;
  }

  // Handle Date vs non-Date mismatch
  if (a instanceof Date || b instanceof Date) {
    console.log(
      `[deepEqual] Date type mismatch at ${path}: a instanceof Date=${a instanceof Date}, b instanceof Date=${b instanceof Date}`,
    );
    return false;
  }

  // Type check
  if (typeof a !== typeof b) {
    console.log(
      `[deepEqual] Type mismatch at ${path}: typeof a=${typeof a}, typeof b=${typeof b}`,
    );
    return false;
  }

  // Handle primitives
  if (typeof a !== "object") {
    const equal = a === b;
    if (!equal) {
      console.log(`[deepEqual] Primitive mismatch at ${path}: a=${a}, b=${b}`);
    }
    return equal;
  }

  // Handle arrays
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) {
      console.log(
        `[deepEqual] Array length mismatch at ${path}: a.length=${a.length}, b.length=${b.length}`,
      );
      return false;
    }
    for (let i = 0; i < a.length; i++) {
      if (!deepEqual(a[i], b[i], `${path}[${i}]`)) return false;
    }
    return true;
  }

  // Handle array vs non-array mismatch
  if (Array.isArray(a) || Array.isArray(b)) {
    console.log(
      `[deepEqual] Array type mismatch at ${path}: Array.isArray(a)=${Array.isArray(a)}, Array.isArray(b)=${Array.isArray(b)}`,
    );
    return false;
  }

  // Handle objects with unordered keys
  if (typeof a === "object" && typeof b === "object") {
    const keysA = Object.keys(a).sort();
    const keysB = Object.keys(b).sort();

    if (keysA.length !== keysB.length) {
      console.log(
        `[deepEqual] Object key count mismatch at ${path}: keysA.length=${keysA.length}, keysB.length=${keysB.length}`,
      );
      console.log(`[deepEqual] keysA=${JSON.stringify(keysA)}`);
      console.log(`[deepEqual] keysB=${JSON.stringify(keysB)}`);
      return false;
    }

    for (let i = 0; i < keysA.length; i++) {
      if (keysA[i] !== keysB[i]) {
        console.log(
          `[deepEqual] Object key mismatch at ${path}: keysA[${i}]=${keysA[i]}, keysB[${i}]=${keysB[i]}`,
        );
        return false;
      }
    }

    // Recursively compare each property value
    for (const key of keysA) {
      if (!deepEqual(a[key], b[key], `${path}.${key}`)) return false;
    }
    return true;
  }

  console.log(`[deepEqual] Unhandled case at ${path}`);
  return false;
}

/**
 * Deep comparison of two arrays of documents
 */
function arraysEqual(a: any[], b: any[]): boolean {
  const result = deepEqual(a, b, "documents");
  if (!result) {
    console.log(
      `[arraysEqual] Arrays not equal. Lengths: a=${a.length}, b=${b.length}`,
    );
    console.log("--- End comparison ---");
  }
  return result;
}
