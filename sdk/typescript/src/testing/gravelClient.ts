import patch, { type Operation } from "fast-json-patch";
import type { Msg, NatsError } from "nats";
import type { Subscription } from "rxjs";
import { getGravelConnection, GravelDBs } from "../gravel.js";
import { MONGO_URL, options, query } from "./config.js";
import { addGravelUpdates } from "./metrics.js";
import { makingUpdates } from "./routes.js";

let gravel: Awaited<ReturnType<typeof getGravelConnection>> | null = null;
let gravelMongoClient: any = null;

export let currentData: { result: any[] } = { result: [] };
let stopWatchQueryHandle: (() => Promise<void>) | null = null;
let gravelChangesSubscription: Subscription | null = null;
let lastUpdateTimestamp: number | null = null;

// Track active intervals/timeouts for cleanup
const activeIntervals = new Set<NodeJS.Timeout>();
const activeTimeouts = new Set<NodeJS.Timeout>();

// Promise resolvers for awaiting next update
let nextUpdateResolvers: Array<() => void> = [];

export function resetLastUpdateTimestamp() {
  lastUpdateTimestamp = null;
}

async function initializeGravel() {
  if (!gravel) {
    try {
      gravel = await getGravelConnection({
        debugChannelCallback: (err: NatsError | null, msg: Msg) => {
          console.log(
            "Gravel Debug: ",
            JSON.stringify(JSON.parse(msg.data.toString()), null, 2),
          );
          if (err) {
            console.log("\x1b[31mGravel Error: ", err, "\x1b[0m");
          }
        },
      });
      if (!gravelMongoClient) {
        gravelMongoClient = await gravel.getDatabaseClient({
          db: GravelDBs.MongoDB,
          mongoUrl: MONGO_URL,
        });
      }
    } catch (error) {
      console.error("Gravel connection error:", error);
      return;
    }
  }
}

export async function stopWatchQuery() {
  // Unsubscribe from changes subscription
  if (gravelChangesSubscription && !gravelChangesSubscription.closed) {
    gravelChangesSubscription.unsubscribe();
    gravelChangesSubscription = null;
    console.log("Gravel changes subscription unsubscribed");
  }

  // Call the stop function from watchQuery
  if (stopWatchQueryHandle) {
    await stopWatchQueryHandle();
    stopWatchQueryHandle = null;
  }
}

export async function restartWatchQuery() {
  // Stop existing watch query if running
  if (stopWatchQueryHandle) {
    console.log("Stopping existing watchQuery...");
    await stopWatchQueryHandle();
    stopWatchQueryHandle = null;
  }

  // Ensure Gravel is initialized
  await initializeGravel();

  if (!gravelMongoClient) {
    return;
  }

  console.log("Starting new watchQuery with updated parameters...");
  const { initialQuery, changes, stop } = await gravelMongoClient.watchQuery(
    "users",
    query,
    options,
  );

  // Store the stop function
  stopWatchQueryHandle = stop;

  // Maintain current state of the data
  currentData = initialQuery;

  addGravelUpdates(1, JSON.stringify(currentData).length);

  // Store the subscription so we can unsubscribe later
  gravelChangesSubscription = changes.subscribe((patches: Operation[]) => {
    lastUpdateTimestamp = Date.now();
    // Apply JSON patches to the current data
    console.log(JSON.stringify(patches, null, 2));
    if (patches.length > 0) {
      addGravelUpdates(1, JSON.stringify(patches).length);
    }
    const patchResult = patch.applyPatch(
      currentData,
      patches as Operation[],
      false,
      false,
    );
    currentData = patchResult.newDocument;

    // Resolve all waiting promises (both patch and noop messages)
    const resolvers = [...nextUpdateResolvers];
    nextUpdateResolvers = [];
    resolvers.forEach((resolve) => resolve());
  });

  console.log("WatchQuery restarted successfully");
}

/**
 * Check if Gravel has settled (no updates for 2 seconds)
 */
export function hasSettled(): boolean {
  if (!lastUpdateTimestamp) {
    lastUpdateTimestamp = Date.now();
    return false;
  }
  const timeSinceLastUpdate = Date.now() - lastUpdateTimestamp;
  return timeSinceLastUpdate >= 2000;
}

/**
 * Wait for Gravel to settle (no updates for 2 seconds)
 */
export async function waitForSettled(): Promise<void> {
  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      const checkInterval = setInterval(() => {
        if (hasSettled() && !makingUpdates) {
          console.log("Has settled");
          clearInterval(checkInterval);
          activeIntervals.delete(checkInterval);
          resolve();
        }
      }, 50); // Check every 50ms
      activeIntervals.add(checkInterval);
      activeTimeouts.delete(timeout);
    }, 1000);
    activeTimeouts.add(timeout);
  });
}

/**
 * Wait for the next Gravel update (patch or noop)
 * This resolves immediately when Gravel processes a database change
 */
export async function waitForNextUpdate(): Promise<void> {
  return new Promise((resolve) => {
    nextUpdateResolvers.push(resolve);
  });
}

/**
 * Close Gravel connection and clean up all resources
 */
export async function closeGravel() {
  // Clear all active intervals
  for (const interval of activeIntervals) {
    clearInterval(interval);
  }
  activeIntervals.clear();

  // Clear all active timeouts
  for (const timeout of activeTimeouts) {
    clearTimeout(timeout);
  }
  activeTimeouts.clear();

  // Stop watch query first (unsubscribes and calls stop handle)
  await stopWatchQuery();

  // Close the Gravel NATS connection (this is critical!)
  if (gravel) {
    try {
      await gravel.close();
      console.log("Gravel NATS connection closed");
    } catch (error) {
      console.error("Error closing Gravel connection:", error);
    }
    gravel = null;
    gravelMongoClient = null;
  }
}
