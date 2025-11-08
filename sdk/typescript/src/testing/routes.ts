import express, { type Request, type Response } from "express";
import { ObjectId } from "mongodb";
import path from "path";
import { fileURLToPath } from "url";
import {
  collectionSize,
  options,
  overrideDeleteNumber,
  overrideInsertNumber,
  overrideUpdateNumber,
  query,
  type GravelTestData,
} from "./config.js";
import {
  generateRandomAddress,
  generateRandomBirthday,
  generateRandomDebitor,
  generateRandomEmail,
  generateRandomRoles,
  generateRandomSepa,
  generateRandomTags,
  generateRandomUpdateFields,
} from "./dataGenerators.js";
import {
  currentData,
  restartWatchQuery,
  stopWatchQuery,
} from "./gravelClient.js";
import {
  addDataBaseUpdates,
  databaseUpdates,
  gravelUpdates,
  oldWatchQueryUpdates,
  gravelBytes,
  oldWatchQueryBytes,
} from "./metrics.js";
import { getMongoClient } from "./mongoClient.js";
import { regenerateDatabase } from "./regenerateDatabase.js";
import { oldWatchQueryData } from "./server.js";

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

router.get("/oldwatchquery", (req: Request, res: Response) => {
  res.json(oldWatchQueryData);
});

router.get("/metrics", (req: Request, res: Response) => {
  res.json({
    databaseUpdates,
    gravelUpdates,
    oldWatchQueryUpdates,
    gravelBytes,
    oldWatchQueryBytes,
  });
});

// API endpoint to make random updates, deletions, and insertions to multiple users
router.post("/randomupdate", async (req: Request, res: Response) => {
  try {
    // Get singleton MongoDB client
    const client = await getMongoClient();

    // Get database and collection
    const collection = client.db().collection<GravelTestData>("users");

    // Get total user count
    const totalUsers = await collection.countDocuments({});

    if (totalUsers === 0) {
      res.status(404).json({ error: "No users found in database" });
      return;
    }

    // Calculate the target size to maintain (within 5% of collectionSize)
    const targetSize = collectionSize;
    const sizeDelta = totalUsers - targetSize;

    // Determine operations distribution
    const maxOpsPerType = Math.min(
      50,
      Math.max(1, Math.floor(totalUsers / 100)),
    );

    // Calculate number of operations
    let numDeletes = Math.floor(Math.random() * maxOpsPerType) + 1;
    let numInserts = Math.floor(Math.random() * maxOpsPerType) + 1;
    const numUpdates = Math.floor(Math.random() * maxOpsPerType) + 1;

    // Adjust deletions and insertions to maintain size near target
    if (sizeDelta > 0) {
      // Too many documents - favor deletions
      numDeletes = Math.min(
        numDeletes + Math.floor(sizeDelta * 0.1),
        maxOpsPerType,
      );
      numInserts = Math.max(1, Math.floor(numInserts * 0.5));
    } else if (sizeDelta < 0) {
      // Too few documents - favor insertions
      numInserts = Math.min(
        numInserts + Math.floor(Math.abs(sizeDelta) * 0.1),
        maxOpsPerType,
      );
      numDeletes = Math.max(1, Math.floor(numDeletes * 0.5));
    }

    // Ensure we don't delete all documents
    numDeletes = Math.min(numDeletes, Math.floor(totalUsers * 0.05));

    const bulkOps: any[] = [];
    const operationStats = {
      updates: 0,
      deletes: 0,
      inserts: 0,
    };

    // 1. UPDATES - Get random users to update
    const usersToUpdate = await collection
      .aggregate([{ $sample: { size: overrideUpdateNumber ?? numUpdates } }])
      .toArray();

    for (const user of usersToUpdate) {
      const updateFields = generateRandomUpdateFields();
      bulkOps.push({
        updateOne: {
          filter: { _id: user._id },
          update: { $set: updateFields },
        },
      });
      operationStats.updates++;
    }

    // 2. DELETIONS - Get random users to delete
    const usersToDelete = await collection
      .aggregate([{ $sample: { size: overrideDeleteNumber ?? numDeletes } }])
      .toArray();

    for (const user of usersToDelete) {
      bulkOps.push({
        deleteOne: {
          filter: { _id: user._id },
        },
      });
      operationStats.deletes++;
    }

    // 3. INSERTIONS - Create new documents
    for (let i = 0; i < (overrideInsertNumber ?? numInserts); i++) {
      const newDocument: GravelTestData = {
        _id: new ObjectId(),
        email: generateRandomEmail(),
        roles: generateRandomRoles(),
        address: generateRandomAddress(),
        debitor: generateRandomDebitor(),
        tags: generateRandomTags(),
        sepa: generateRandomSepa(),
      };

      // Optional fields - 50% chance for archived
      if (Math.random() > 0.5) {
        newDocument.archived = Math.random() > 0.5;
      }

      // Optional fields - 70% chance for birthday
      if (Math.random() > 0.3) {
        newDocument.birthday = generateRandomBirthday();
      }

      bulkOps.push({
        insertOne: {
          document: newDocument,
        },
      });
      operationStats.inserts++;
    }

    // Execute bulk write operations
    const bulkResult = await collection.bulkWrite(bulkOps, { ordered: false });

    // Get updated count
    const newTotalUsers = await collection.countDocuments({});

    addDataBaseUpdates(
      (bulkResult.modifiedCount || 0) +
        (bulkResult.deletedCount || 0) +
        (bulkResult.insertedCount || 0),
    );

    // Return the operation information
    res.json({
      success: true,
      operations: {
        updates: bulkResult.modifiedCount || 0,
        deletes: bulkResult.deletedCount || 0,
        inserts: bulkResult.insertedCount || 0,
      },
      documentCount: {
        before: totalUsers,
        after: newTotalUsers,
        target: targetSize,
        delta: newTotalUsers - targetSize,
      },
    });
  } catch (error) {
    console.error("Random operations error:", error);
    res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

router.post("/redodb", async (req: Request, res: Response) => {
  try {
    await stopWatchQuery();
    await regenerateDatabase(collectionSize);
    await restartWatchQuery();
    res.json({ success: true });
  } catch (error) {
    console.error("Database regeneration error:", error);
    res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});
