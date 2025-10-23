import express, { type Request, type Response } from "express";
import patch, { type Operation } from "fast-json-patch";
import { MongoClient } from "mongodb";
import type { Msg, NatsError } from "nats";
import path from "path";
import { fileURLToPath } from "url";
import type { GravelMongoWatchQueryFindOptions } from "./db/mongo.js";
import { getGravelConnection, GravelDBs } from "./gravel.js";

let query: any = {
  debitor: { $lte: 20000 },
};

let options: GravelMongoWatchQueryFindOptions = {
  sort: {
    debitor: -1,
    _id: -1,
  },
  limit: 20,
  skip: 20,
  projection: {
    _id: 1,
    email: 1,
    address: { street: 1 },
    role: 1,
    debitor: 1,
  },
};

// ES module equivalent of __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

let currentData: { result: any[] } = { result: [] };
let stopWatchQuery: (() => Promise<void>) | null = null;

// Setup Express server
const app = express();
const PORT = 3000;

// Enable JSON body parsing
app.use(express.json());

// Serve static HTML
app.get("/", (req: Request, res: Response) => {
  res.sendFile(path.join(__dirname, "viewer.html"));
});

// API endpoint to get current data
app.get("/data", (req: Request, res: Response) => {
  // Check if currentData has a result property and return just the array
  const dataToSend =
    currentData && typeof currentData === "object" && "result" in currentData
      ? currentData.result
      : currentData;
  res.json(dataToSend);
});

app.get("/simplequery", async (req: Request, res: Response) => {
  try {
    // Connect to MongoDB directly
    const client = new MongoClient("mongodb://localhost:27017/gravel_db", {
      directConnection: true,
      replicaSet: "rs0",
    });
    await client.connect();

    // Get database and collection
    const collection = client.db().collection("users");

    // Execute query with options
    const results = await collection
      .find(query)
      .sort(options.sort || {})
      .skip(options.skip || 0)
      .limit(options.limit || 0)
      .project(options.projection || {})
      .toArray();

    // Close connection
    await client.close();

    // Return results
    res.json({ result: results });
  } catch (error) {
    console.error("MongoDB query error:", error);
    res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

// API endpoint to set query and options
app.post("/setquery", async (req: Request, res: Response) => {
  try {
    const { query: newQuery, options: newOptions } = req.body;

    if (newQuery !== undefined) {
      query = newQuery;
    }
    if (newOptions !== undefined) {
      options = newOptions;
    }

    console.log("Updated query:", JSON.stringify(query, null, 2));
    console.log("Updated options:", JSON.stringify(options, null, 2));

    // Restart watchQuery with new parameters
    await restartWatchQuery();

    res.json({
      success: true,
      message: "Query and options updated successfully",
      query,
      options,
    });
  } catch (error) {
    console.error("Error setting query:", error);
    res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

// API endpoint to get current query and options
app.get("/getquery", (req: Request, res: Response) => {
  res.json({ query, options });
});

app.listen(PORT, () => {
  console.log(`Express server running at http://localhost:${PORT}`);
});

let gravel: Awaited<ReturnType<typeof getGravelConnection>> | null = null;
let gravelMongoClient: any = null;

async function initializeGravel() {
  if (!gravel) {
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
  }
  if (!gravelMongoClient) {
    gravelMongoClient = await gravel.getDatabaseClient({
      db: GravelDBs.MongoDB,
      mongoUrl: "mongodb://localhost:27017/gravel_db",
    });
  }
}

async function restartWatchQuery() {
  // Stop existing watch query if running
  if (stopWatchQuery) {
    console.log("Stopping existing watchQuery...");
    await stopWatchQuery();
    stopWatchQuery = null;
  }

  // Ensure Gravel is initialized
  await initializeGravel();

  if (!gravelMongoClient) {
    throw new Error("Gravel MongoDB client not initialized");
  }

  console.log("Starting new watchQuery with updated parameters...");
  const { initialQuery, changes, stop } = await gravelMongoClient.watchQuery(
    "users",
    query,
    options,
  );

  // Store the stop function
  stopWatchQuery = stop;

  // Maintain current state of the data
  currentData = initialQuery;

  changes.subscribe((patches: Operation[]) => {
    // Apply JSON patches to the current data
    console.log(JSON.stringify(patches, null, 2));
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

async function test() {
  await restartWatchQuery();

  // Handle Ctrl+C gracefully
  process.on("SIGINT", async () => {
    console.log("\nReceived SIGINT (Ctrl+C). Stopping gracefully...");
    if (stopWatchQuery) {
      await stopWatchQuery();
    }
    process.exit(0);
  });

  // Keep the process running
  console.log("Watching for changes... Press Ctrl+C to stop.");
}

void test();
