import express from "express";
import { PORT } from "./config.js";
import { restartWatchQuery, stopWatchQuery } from "./gravelClient.js";
import { closeMongoClient } from "./mongoClient.js";
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
  await restartWatchQuery();

  // Handle Ctrl+C gracefully
  process.on("SIGINT", async () => {
    console.log("\nReceived SIGINT (Ctrl+C). Stopping gracefully...");
    if (stopWatchQuery) {
      await stopWatchQuery();
    }
    await closeMongoClient();
    process.exit(0);
  });

  // Keep the process running
  console.log("Press Ctrl+C to stop.");
}

void startTest();
