import express from "express";
import type { Server } from "http";
import type { Subscription } from "rxjs";
import { options, PORT, query } from "./config.js";
import { closeGravel, restartWatchQuery } from "./gravelClient.js";
import { addOldWatchQueryUpdates } from "./metrics.js";
import { closeMongoClient, getMongoClient } from "./mongoClient.js";
import { stopOldWatchQuery, watchQuery } from "./oldWatchQuery.js";
import { router } from "./routes.js";

// Store oldWatchQuery data similar to how Gravel stores currentData
export let oldWatchQueryData: any[] = [];

const app = express();

// Enable JSON body parsing
app.use(express.json());

// Register routes
app.use(router);

// Store server and subscription references for cleanup
let httpServer: Server | null = null;
let oldWatchQuerySubscription: Subscription | null = null;

// Start server
httpServer = app.listen(PORT, () => {
  console.log(`Express server running at http://localhost:${PORT}`);
});

async function startTest() {
  const mongoClient = await getMongoClient();

  const col = mongoClient.db().collection("users");

  await col.createIndex({ debitor: 1 });

  await restartWatchQuery();

  // Store the subscription so we can unsubscribe later
  oldWatchQuerySubscription = watchQuery("users", query, options).subscribe(
    (data) => {
      oldWatchQueryData = data;
      addOldWatchQueryUpdates(1, JSON.stringify(data).length);
    },
  );

  // Handle Ctrl+C gracefully
  process.on("SIGINT", async () => {
    console.log("\nReceived SIGINT (Ctrl+C). Stopping gracefully...");

    // Force exit after 5 seconds if cleanup hangs
    const forceExitTimeout = setTimeout(() => {
      console.log("Cleanup took too long, forcing exit...");
      process.exit(0);
    }, 5000);

    try {
      // Unsubscribe from old watch query observable
      if (oldWatchQuerySubscription && !oldWatchQuerySubscription.closed) {
        oldWatchQuerySubscription.unsubscribe();
        console.log("Old watch query subscription unsubscribed");
      }

      // Stop the old watch query system
      await stopOldWatchQuery();

      // Close Gravel client (stops watch query and cleans up)
      await closeGravel();

      // Close MongoDB client
      await closeMongoClient();

      // Close HTTP server
      if (httpServer) {
        await new Promise<void>((resolve) => {
          httpServer!.close(() => {
            console.log("HTTP server closed");
            resolve();
          });
        });
      }

      console.log("All systems stopped successfully.");
      clearTimeout(forceExitTimeout);
      process.exit(0);
    } catch (error) {
      console.error("Error during shutdown:", error);
      clearTimeout(forceExitTimeout);
      process.exit(1);
    }
  });

  // Keep the process running
  console.log("Press Ctrl+C to stop.");
}

void startTest();
