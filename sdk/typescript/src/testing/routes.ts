import express, { type Request, type Response } from "express";
import path from "path";
import { fileURLToPath } from "url";
import { options, query, USE_BULK_OPERATIONS } from "./config.js";
import { generateRandomUpdateFields } from "./dataGenerators.js";
import {
  currentData,
  restartWatchQuery,
  stopWatchQuery,
} from "./gravelClient.js";
import { getMongoClient } from "./mongoClient.js";
import { regenerateDatabase } from "./regenerateDatabase.js";

// ES module equivalent of __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const router = express.Router();

// Serve static HTML
router.get("/", (req: Request, res: Response) => {
  res.sendFile(path.join(__dirname, "viewer.html"));
});

// API endpoint to get current data
router.get("/data", (req: Request, res: Response) => {
  // Check if currentData has a result property and return just the array
  const dataToSend =
    currentData && typeof currentData === "object" && "result" in currentData
      ? currentData.result
      : currentData;
  res.json(dataToSend);
});

router.get("/simplequery", async (req: Request, res: Response) => {
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

// API endpoint to make random updates to multiple users with multiple fields
router.post("/randomupdate", async (req: Request, res: Response) => {
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
      .aggregate([{ $sample: { size: numUpdates } }])
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
      const result = await collection.updateMany(
        { _id: { $in: users.map((user) => user._id) } },
        {
          $set: {
            debitor: 10000,
          },
        },
      );
      modifiedCount += result.modifiedCount;
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

router.post("/redodb", async (req: Request, res: Response) => {
  try {
    await stopWatchQuery();
    await regenerateDatabase(500000);
    await restartWatchQuery();
    res.json({ success: true });
  } catch (error) {
    console.error("Database regeneration error:", error);
    res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});
