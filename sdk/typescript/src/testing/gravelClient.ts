import patch, { type Operation } from "fast-json-patch";
import type { Msg, NatsError } from "nats";
import { getGravelConnection, GravelDBs } from "../gravel.js";
import { MONGO_URL, options, query } from "./config.js";
import { addGravelUpdates } from "./metrics.js";
import { makingUpdates } from "./routes.js";

let gravel: Awaited<ReturnType<typeof getGravelConnection>> | null = null;
let gravelMongoClient: any = null;

export let currentData: { result: any[] } = { result: [] };
let stopWatchQueryHandle: (() => Promise<void>) | null = null;
let lastUpdateTimestamp: number | null = null;

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

export function stopWatchQuery() {
  if (stopWatchQueryHandle) {
    stopWatchQueryHandle();
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

  changes.subscribe((patches: Operation[]) => {
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
  return timeSinceLastUpdate >= 5000;
}

/**
 * Wait for Gravel to settle (no updates for 2 seconds)
 */
export async function waitForSettled(): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(() => {
      const checkInterval = setInterval(() => {
        if (hasSettled() && !makingUpdates) {
          console.log("Has settled");
          clearInterval(checkInterval);
          resolve();
        }
      }, 100); // Check every 100ms
    }, 1000);
  });
}
