import express from "express";
import type { Server } from "http";
import { PORT } from "./config.js";
import { closeMongoClient, getMongoClient } from "./mongoClient.js";
import { router } from "./routes.js";

const app = express();

// Enable JSON body parsing
app.use(express.json());

// Register routes
app.use(router);

// Store server reference for cleanup
let httpServer: Server | null = null;

// Start server
httpServer = app.listen(PORT, () => {
  console.log(`Express server running at http://localhost:${PORT}`);
});

// Create index on startup
async function initializeDatabase() {
  try {
    const mongoClient = await getMongoClient();
    const col = mongoClient.db().collection("users");
    await col.createIndex({ debitor: 1 });
    console.log("Database index created");
  } catch (error) {
    console.error("Error initializing database:", error);
  }
}

// Handle Ctrl+C gracefully
process.on("SIGINT", async () => {
  console.log("\nReceived SIGINT (Ctrl+C). Stopping gracefully...");

  // Force exit after 10 seconds if cleanup hangs
  const forceExitTimeout = setTimeout(() => {
    console.log("Cleanup took too long, forcing exit...");
    process.exit(0);
  }, 10000);

  try {
    // Import stopExperiment dynamically to avoid circular dependency
    const { stopExperiment, isExperimentRunning } =
      await import("./experimentRunner.js");

    // Stop any running experiment first
    if (isExperimentRunning()) {
      console.log("Stopping running experiment...");
      stopExperiment();
      // Give experiment time to stop gracefully
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }

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

    console.log("Server stopped successfully.");
    clearTimeout(forceExitTimeout);
    process.exit(0);
  } catch (error) {
    console.error("Error during shutdown:", error);
    clearTimeout(forceExitTimeout);
    process.exit(1);
  }
});

void initializeDatabase();
