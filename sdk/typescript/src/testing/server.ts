import express from "express";
import { options, PORT, query } from "./config.js";
import { restartWatchQuery, stopWatchQuery } from "./gravelClient.js";
import { closeMongoClient, getMongoClient } from "./mongoClient.js";
import { stopOldWatchQuery, watchQuery } from "./oldWatchQuery.js";
import { router } from "./routes.js";

const app = express();

// Enable JSON body parsing
app.use(express.json());

// Register routes
app.use(router);

// Start server
app.listen(PORT, () => {
  console.log(`Express server running at http://localhost:${PORT}`);
});

async function startTest() {
  const mongoClient = await getMongoClient();

  const col = mongoClient.db().collection("users");

  await col.createIndex({ debitor: 1 });

  await restartWatchQuery();

  watchQuery("users", query, options).subscribe((data) => {});

  // Handle Ctrl+C gracefully
  process.on("SIGINT", async () => {
    console.log("\nReceived SIGINT (Ctrl+C). Stopping gracefully...");
    try {
      // Stop the Gravel client watch query
      if (stopWatchQuery) {
        await stopWatchQuery();
      }
      // Stop the old watch query system
      await stopOldWatchQuery();
      // Close MongoDB client
      await closeMongoClient();
      console.log("All systems stopped successfully.");
      process.exit(0);
    } catch (error) {
      console.error("Error during shutdown:", error);
      process.exit(1);
    }
  });

  // Keep the process running
  console.log("Press Ctrl+C to stop.");
}

void startTest();
