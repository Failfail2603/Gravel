import { faker } from "@faker-js/faker";
import express, { type Request, type Response } from "express";
import patch, { type Operation } from "fast-json-patch";
import { MongoClient } from "mongodb";
import type { Msg, NatsError } from "nats";
import path from "path";
import { fileURLToPath } from "url";
import type { GravelMongoWatchQueryFindOptions } from "./db/mongo.js";
import { getGravelConnection, GravelDBs } from "./gravel.js";

let query: any = {
  role: "admin",
  $and: [
    {
      debitor: {
        $gt: 200000,
      },
    },
    {
      debitor: {
        $lt: 800000,
      },
    },
  ],
};

let options: GravelMongoWatchQueryFindOptions = {
  sort: {
    role: 1,
    debitor: -1,
    _id: -1,
  },
  skip: 70000,
  limit: 2000,
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
    // Get singleton MongoDB client
    const client = await getMongoClient();

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

// Helper function to generate random email using faker
function generateRandomEmail(): string {
  return faker.internet.email();
}

// Helper function to generate random address fields using faker
function generateRandomAddress(): object {
  return {
    street: faker.location.streetAddress(),
    city: faker.location.city(),
    state: faker.location.state(),
    zip: faker.location.zipCode(),
    country: faker.location.country(),
  };
}

// Helper function to generate random birthday (18-80 years ago) using faker
function generateRandomBirthday(): Date {
  return faker.date.birthdate({ min: 18, max: 80, mode: "age" });
}

// Helper function to generate random update fields
function generateRandomUpdateFields(): object {
  const possibleFields = ["email", "debitor", "role", "address", "birthday"];
  // Update 1-4 random fields
  const numFields = Math.floor(Math.random() * 4) + 1;
  const fieldsToUpdate = possibleFields
    .sort(() => Math.random() - 0.5)
    .slice(0, numFields);

  const updateData: any = {
    last_updated: new Date(),
  };

  for (const field of fieldsToUpdate) {
    switch (field) {
      case "email":
        updateData.email = generateRandomEmail();
        break;
      case "debitor":
        updateData.debitor = Math.floor(Math.random() * 999000) + 1000;
        break;
      case "role":
        const roles = ["user", "admin", "moderator"];
        updateData.role = roles[Math.floor(Math.random() * roles.length)];
        break;
      case "address":
        updateData.address = generateRandomAddress();
        break;
      case "birthday":
        updateData.birthday = generateRandomBirthday();
        break;
    }
  }

  return updateData;
}

// Configuration: Set to true for bulk operations, false for individual updateOne loop
const USE_BULK_OPERATIONS = true;

// API endpoint to make random updates to multiple users with multiple fields
app.post("/randomupdate", async (req: Request, res: Response) => {
  try {
    // Get singleton MongoDB client
    const client = await getMongoClient();

    // Get database and collection
    const collection = client.db().collection("users");

    // Get total user count to determine how many to update
    const totalUsers = await collection.countDocuments({});

    if (totalUsers === 0) {
      res.status(404).json({ error: "No users found in database" });
      return;
    }

    // Determine random number of users to update (1-10% of total, min 1, max 50)
    const maxUpdates = Math.min(50, Math.max(1, Math.floor(totalUsers / 10)));
    const numUpdates = Math.floor(Math.random() * maxUpdates) + 1;

    // Get random users using $sample aggregation
    const users = await collection
      .aggregate([
        // {
        //   $match: {
        //     role: "admin",
        //     $and: [
        //       {
        //         debitor: {
        //           $gt: 200000,
        //         },
        //       },
        //       {
        //         debitor: {
        //           $lt: 800000,
        //         },
        //       },
        //     ],
        //   },
        // },
        // {
        //   $sort: {
        //     role: 1,
        //     debitor: -1,
        //     _id: -1,
        //   },
        // },
        // { $limit: 20 },

        { $sample: { size: numUpdates } },
      ])
      .toArray();

    if (users.length === 0) {
      res.status(404).json({ error: "No users found to update" });
      return;
    }

    let modifiedCount = 0;

    if (USE_BULK_OPERATIONS) {
      // Prepare bulk write operations
      const bulkOps = users.map((user) => {
        const updateFields = generateRandomUpdateFields();
        return {
          updateOne: {
            filter: { _id: user._id },
            update: {
              $set: updateFields,
            },
          },
        };
      });

      // Execute bulk write
      const bulkResult = await collection.bulkWrite(bulkOps);
      modifiedCount = bulkResult.modifiedCount;
    } else {
      // Execute individual updateOne operations in a loop

      // const updateFields = generateRandomUpdateFields();
      const result = await collection.updateMany(
        { _id: { $in: users.map((user) => user._id) } },
        {
          $set: {
            debitor: 10000,
          },
        },
      );
      modifiedCount += result.modifiedCount;

      // await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    // Collect update information for response
    const updateInfo = users.map((user) => ({
      _id: user._id,
      email: user.email,
      updatedFields: ["debitor"],
    }));

    // Return the update information
    res.json({
      success: true,
      documentsUpdated: modifiedCount,
      totalDocumentsProcessed: users.length,
      updates: updateInfo,
      method: USE_BULK_OPERATIONS ? "bulk" : "loop",
    });
  } catch (error) {
    console.error("Random update error:", error);
    res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

app.listen(PORT, () => {
  console.log(`Express server running at http://localhost:${PORT}`);
});

let gravel: Awaited<ReturnType<typeof getGravelConnection>> | null = null;
let gravelMongoClient: any = null;
let mongoClient: MongoClient | null = null;

// Singleton MongoDB client getter
async function getMongoClient(): Promise<MongoClient> {
  if (!mongoClient) {
    mongoClient = new MongoClient("mongodb://localhost:27017/gravel_db", {
      directConnection: true,
      replicaSet: "rs0",
    });
    await mongoClient.connect();
    console.log("MongoDB client connected");
  }
  return mongoClient;
}

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
    if (mongoClient) {
      await mongoClient.close();
      console.log("MongoDB client closed");
    }
    process.exit(0);
  });

  // Keep the process running
  console.log("Watching for changes... Press Ctrl+C to stop.");
}

void test();
