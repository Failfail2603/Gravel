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
  initialBytes: number;
  totalUpdateBytes: number;
  lastUpdateBytes: number;
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
  initialBytes: 0,
  totalUpdateBytes: 0,
  lastUpdateBytes: 0,
};

const oldWatchQueryState: WatcherState = {
  currentData: [],
  lastUpdateReceived: false,
  updateCount: 0,
  lastUpdateTimestamp: null,
  lastUpdateWasNoop: false,
  initialBytes: 0,
  totalUpdateBytes: 0,
  lastUpdateBytes: 0,
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
  gravelState.initialBytes = 0;
  gravelState.totalUpdateBytes = 0;
  gravelState.lastUpdateBytes = 0;
  oldWatchQueryState.currentData = [];
  oldWatchQueryState.lastUpdateReceived = false;
  oldWatchQueryState.updateCount = 0;
  oldWatchQueryState.lastUpdateWasNoop = false;
  oldWatchQueryState.initialBytes = 0;
  oldWatchQueryState.totalUpdateBytes = 0;
  oldWatchQueryState.lastUpdateBytes = 0;

  // Start Gravel watchQuery
  const { initialQuery, changes, stop } = await gravelMongoClient.watchQuery(
    collectionName,
    query,
    options,
  );
  stopGravelWatchQuery = stop;

  // Set initial data for Gravel
  gravelState.currentData = initialQuery.result || initialQuery;
  gravelState.initialBytes = Buffer.byteLength(
    JSON.stringify(gravelState.currentData),
    "utf-8",
  );
  console.log(
    `Gravel initial data: ${gravelState.currentData.length} documents (${gravelState.initialBytes} bytes)`,
  );

  // Subscribe to Gravel changes
  gravelSubscription = changes.subscribe((patches: Operation[]) => {
    // Always mark that we received an update (even if noop)
    gravelState.lastUpdateReceived = true;
    gravelState.updateCount++;
    gravelState.lastUpdateTimestamp = Date.now();

    // Track bytes for this update
    const updateBytes = Buffer.byteLength(JSON.stringify(patches), "utf-8");
    gravelState.lastUpdateBytes = updateBytes;
    gravelState.totalUpdateBytes += updateBytes;

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
        oldWatchQueryState.initialBytes = Buffer.byteLength(
          JSON.stringify(data),
          "utf-8",
        );
        console.log(
          `OldWatchQuery initial data: ${data.length} documents (${oldWatchQueryState.initialBytes} bytes)`,
        );
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
          oldWatchQueryState.lastUpdateBytes = 0;
          // Data stays the same for noops
        } else {
          // This is a real update with new data
          const updateBytes = Buffer.byteLength(JSON.stringify(data), "utf-8");
          oldWatchQueryState.lastUpdateBytes = updateBytes;
          oldWatchQueryState.totalUpdateBytes += updateBytes;
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
  gravelState.lastUpdateBytes = 0;
  oldWatchQueryState.lastUpdateReceived = false;
  oldWatchQueryState.lastUpdateTimestamp = null;
  oldWatchQueryState.lastUpdateWasNoop = false;
  oldWatchQueryState.lastUpdateBytes = 0;
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

function deepEqual(obj1: any, obj2: any, path = "") {
  // Handle strict equality (primitives, same reference)
  if (obj1 === obj2) return true;

  // Handle null/undefined
  if (obj1 == null || obj2 == null) {
    if (obj1 !== obj2) {
      console.log(
        `[deepEqual] Null/undefined mismatch at ${path}:`,
        obj1,
        "vs",
        obj2,
      );
    }
    return false;
  }

  // Handle Date objects
  if (obj1 instanceof Date && obj2 instanceof Date) {
    const equal = obj1.getTime() === obj2.getTime();
    if (!equal) {
      console.log(`[deepEqual] Date mismatch at ${path}:`, obj1, "vs", obj2);
    }
    return equal;
  }

  // Handle Date vs string comparison
  if (obj1 instanceof Date && typeof obj2 === "string") {
    const date2 = new Date(obj2);
    if (!isNaN(date2.getTime())) {
      const equal = obj1.getTime() === date2.getTime();
      if (!equal) {
        console.log(
          `[deepEqual] Date vs string mismatch at ${path}:`,
          obj1,
          "vs",
          obj2,
        );
      }
      return equal;
    }
  }

  if (typeof obj1 === "string" && obj2 instanceof Date) {
    const date1 = new Date(obj1);
    if (!isNaN(date1.getTime())) {
      const equal = date1.getTime() === obj2.getTime();
      if (!equal) {
        console.log(
          `[deepEqual] String vs Date mismatch at ${path}:`,
          obj1,
          "vs",
          obj2,
        );
      }
      return equal;
    }
  }

  // Handle primitives
  if (typeof obj1 !== "object" || typeof obj2 !== "object") {
    // Special case: Compare ISO date strings by parsing them
    if (typeof obj1 === "string" && typeof obj2 === "string") {
      // Check if both look like ISO date strings
      const isoDateRegex =
        /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z?$/;
      if (isoDateRegex.test(obj1) && isoDateRegex.test(obj2)) {
        const date1 = new Date(obj1);
        const date2 = new Date(obj2);
        // Only compare as dates if both are valid dates
        if (!isNaN(date1.getTime()) && !isNaN(date2.getTime())) {
          const equal = date1.getTime() === date2.getTime();
          if (!equal) {
            console.log(
              `[deepEqual] Date string mismatch at ${path}:`,
              obj1,
              "vs",
              obj2,
              "(timestamps:",
              date1.getTime(),
              "vs",
              date2.getTime(),
              ")",
            );
          }
          return equal;
        }
      }
    }

    const equal = obj1 === obj2;
    if (!equal) {
      console.log(
        `[deepEqual] Primitive mismatch at ${path}:`,
        obj1,
        "vs",
        obj2,
      );
    }
    return equal;
  }

  // Handle arrays
  if (Array.isArray(obj1) && Array.isArray(obj2)) {
    if (obj1.length !== obj2.length) {
      console.log(
        `[deepEqual] Array length mismatch at ${path}:`,
        obj1.length,
        "vs",
        obj2.length,
      );
      return false;
    }

    // Recursively compare each element (handles nested objects, arrays, dates)
    for (let i = 0; i < obj1.length; i++) {
      if (!deepEqual(obj1[i], obj2[i], `${path}[${i}]`)) {
        console.log(`[deepEqual] Array element mismatch at ${path}[${i}]`);
        return false;
      }
    }
    return true;
  }

  // One is array, other is not
  if (Array.isArray(obj1) !== Array.isArray(obj2)) {
    console.log(
      `[deepEqual] Type mismatch at ${path}: one is array, other is not`,
    );
    return false;
  }

  // Compare objects property by property (order-independent)
  const keys1 = Object.keys(obj1).sort();
  const keys2 = Object.keys(obj2).sort();

  // Different number of properties
  if (keys1.length !== keys2.length) {
    console.log(
      `[deepEqual] Object keys count mismatch at ${path}:`,
      keys1.length,
      "vs",
      keys2.length,
    );
    console.log(`[deepEqual] Keys1:`, keys1);
    console.log(`[deepEqual] Keys2:`, keys2);
    return false;
  }

  // Check all keys match
  for (let i = 0; i < keys1.length; i++) {
    if (keys1[i] !== keys2[i]) {
      console.log(
        `[deepEqual] Key mismatch at ${path}:`,
        keys1[i],
        "vs",
        keys2[i],
      );
      console.log(`[deepEqual] All keys1:`, keys1);
      console.log(`[deepEqual] All keys2:`, keys2);
      return false;
    }
  }

  // Recursively compare all property values
  for (const key of keys1) {
    if (!deepEqual(obj1[key], obj2[key], `${path}.${key}`)) {
      console.log(`[deepEqual] Property value mismatch at ${path}.${key}`);
      return false;
    }
  }

  return true;
}

// Helper function to compare arrays for equality
function arraysEqual(a: any[], b: any[]): boolean {
  if (!Array.isArray(a) || !Array.isArray(b)) return false;
  if (a.length !== b.length) return false;

  // Sort both arrays by _id for comparison
  const sortedA = [...a].sort((x, y) =>
    String(x._id).localeCompare(String(y._id)),
  );
  const sortedB = [...b].sort((x, y) =>
    String(x._id).localeCompare(String(y._id)),
  );

  // Use deep equality instead of JSON.stringify to handle property order differences
  for (let i = 0; i < sortedA.length; i++) {
    sortedA[i]._id =
      typeof sortedA[i]._id === "object"
        ? sortedA[i]._id.toString()
        : sortedA[i]._id;
    sortedB[i]._id =
      typeof sortedB[i]._id === "object"
        ? sortedB[i]._id.toString()
        : sortedB[i]._id;
    if (!deepEqual(sortedA[i], sortedB[i])) {
      return false;
    }
  }

  return true;
}
