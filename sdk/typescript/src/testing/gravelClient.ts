import patch, { type Operation } from "fast-json-patch";
import type { Msg, NatsError } from "nats";
import { getGravelConnection, GravelDBs } from "../gravel.js";
import { MONGO_URL, options, query } from "./config.js";
import { addGravelUpdates } from "./metrics.js";

let gravel: Awaited<ReturnType<typeof getGravelConnection>> | null = null;
let gravelMongoClient: any = null;

export let currentData: { result: any[] } = { result: [] };
let stopWatchQueryHandle: (() => Promise<void>) | null = null;

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

  changes.subscribe((patches: Operation[]) => {
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
