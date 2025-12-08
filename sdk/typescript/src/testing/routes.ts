import express, { type Request, type Response } from "express";
import { ObjectId } from "mongodb";
import path from "path";
import { fileURLToPath } from "url";
import {
  type GravelTestData,
  collectionSize,
  customBulkOperationGenerator,
  options,
  query,
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
  resetLastUpdateTimestamp,
  restartWatchQuery,
  stopWatchQuery,
  waitForNextUpdate,
  waitForSettled,
} from "./gravelClient.js";
import {
  addDataBaseUpdates,
  databaseUpdates,
  gravelBytes,
  gravelUpdates,
  oldWatchQueryBytes,
  oldWatchQueryUpdates,
} from "./metrics.js";
import { getMongoClient } from "./mongoClient.js";
import { regenerateDatabase } from "./regenerateDatabase.js";
import { oldWatchQueryData } from "./server.js";

// ES module equivalent of __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const router = express.Router();

export let makingUpdates = false;

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
// Now performs only ONE operation type per call for measurement purposes
router.post("/randomupdate", async (req: Request, res: Response) => {
  try {
    makingUpdates = true;
    // Get singleton MongoDB client
    const client = await getMongoClient();

    // Get database and collection
    const collection = client.db().collection<GravelTestData>("users");

    // Get total user count
    const totalUsers = await collection.countDocuments({});

    if (totalUsers === 0) {
      res.status(404).json({ error: "No users found in database" });
      makingUpdates = false;
      return;
    }

    const targetSize = collectionSize;
    const sizeDelta = totalUsers - targetSize;

    let bulkOps: any[] = [];
    let operationStats = {
      updates: 0,
      deletes: 0,
      inserts: 0,
      replaces: 0,
    };
    let operationType = "";

    // Check if custom bulk operation generator is defined
    if (customBulkOperationGenerator) {
      const customResult = await customBulkOperationGenerator(
        collection,
        totalUsers,
      );
      bulkOps = customResult.bulkOps;
      operationStats = customResult.stats;
      operationType = "custom";
    } else {
      // Randomly choose ONE operation type
      const operations = ["update", "replace", "delete", "insert"];

      // Adjust operation selection based on collection size
      let selectedOp;
      if (sizeDelta > targetSize * 0.1) {
        // Too many documents - favor deletions
        selectedOp =
          Math.random() < 0.6
            ? "delete"
            : operations[Math.floor(Math.random() * operations.length)];
      } else if (sizeDelta < -targetSize * 0.1) {
        // Too few documents - favor insertions
        selectedOp =
          Math.random() < 0.6
            ? "insert"
            : operations[Math.floor(Math.random() * operations.length)];
      } else {
        // Normal distribution
        selectedOp = operations[Math.floor(Math.random() * operations.length)];
      }

      operationType = selectedOp;

      switch (selectedOp) {
        case "update": {
          // UPDATES - Get one random user to update
          const usersToUpdate = await collection
            .aggregate([{ $sample: { size: 1 } }])
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
          break;
        }

        case "replace": {
          // REPLACES - Get one random user to replace with completely new data
          const usersToReplace = await collection
            .aggregate([{ $sample: { size: 1 } }])
            .toArray();

          for (const user of usersToReplace) {
            const replacementDocument: GravelTestData = {
              _id: user._id, // Keep the same _id
              email: generateRandomEmail(),
              roles: generateRandomRoles(),
              address: generateRandomAddress(),
              debitor: generateRandomDebitor(),
              tags: generateRandomTags(),
              sepa: generateRandomSepa(),
            };

            // Optional fields - 50% chance for archived
            if (Math.random() > 0.5) {
              replacementDocument.archived = Math.random() > 0.5;
            }

            // Optional fields - 70% chance for birthday
            if (Math.random() > 0.3) {
              replacementDocument.birthday = generateRandomBirthday();
            }

            bulkOps.push({
              replaceOne: {
                filter: { _id: user._id },
                replacement: replacementDocument,
              },
            });
            operationStats.replaces++;
          }
          break;
        }

        case "delete": {
          // DELETIONS - Get one random user to delete (but ensure we don't delete all)
          if (totalUsers > Math.max(100, targetSize * 0.1)) {
            const usersToDelete = await collection
              .aggregate([{ $sample: { size: 1 } }])
              .toArray();

            for (const user of usersToDelete) {
              bulkOps.push({
                deleteOne: {
                  filter: { _id: user._id },
                },
              });
              operationStats.deletes++;
            }
          } else {
            // Skip deletion if collection is too small, do an insert instead
            operationType = "insert";
            const newDocument: GravelTestData = {
              _id: new ObjectId(),
              email: generateRandomEmail(),
              roles: generateRandomRoles(),
              address: generateRandomAddress(),
              debitor: generateRandomDebitor(),
              tags: generateRandomTags(),
              sepa: generateRandomSepa(),
            };

            if (Math.random() > 0.5) {
              newDocument.archived = Math.random() > 0.5;
            }
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
          break;
        }

        case "insert": {
          // INSERTIONS - Create one new document
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
          break;
        }
      }
    }

    // Execute bulk write operations (now only 1 operation)
    const bulkResult = await collection.bulkWrite(bulkOps, { ordered: false });

    // Wait for Gravel to process the update (patch or noop)
    // This replaces the old timeout-based /settled approach
    await waitForNextUpdate();

    // Get updated count
    const newTotalUsers = await collection.countDocuments({});

    addDataBaseUpdates(
      (bulkResult.modifiedCount || 0) +
        (bulkResult.deletedCount || 0) +
        (bulkResult.insertedCount || 0),
    );

    // Return the operation information
    makingUpdates = false;
    resetLastUpdateTimestamp();
    res.json({
      success: true,
      operationType,
      operations: {
        updates: operationStats.updates,
        deletes: operationStats.deletes,
        inserts: operationStats.inserts,
        replaces: operationStats.replaces,
      },
      bulkResult: {
        matched: bulkResult.matchedCount || 0,
        modified: bulkResult.modifiedCount || 0,
        deleted: bulkResult.deletedCount || 0,
        inserted: bulkResult.insertedCount || 0,
      },
      documentCount: {
        before: totalUsers,
        after: newTotalUsers,
        target: targetSize,
        delta: newTotalUsers - targetSize,
      },
    });
  } catch (error) {
    makingUpdates = false;
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

// API endpoint to check if Gravel has settled
router.get("/settled", async (req: Request, res: Response) => {
  try {
    // Wait for Gravel to settle (no updates for 2 seconds)
    await waitForSettled();
    res.json({ settled: true });
  } catch (error) {
    console.error("Error waiting for settled state:", error);
    res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

// Measurement statistics storage
interface MeasurementStats {
  gravel: {
    truePositive: number;
    trueNegative: number;
    falsePositive: number;
    falseNegative: number;
  };
  oldWatchQuery: {
    truePositive: number;
    trueNegative: number;
    falsePositive: number;
    falseNegative: number;
  };
  totalTests: number;
}

let measurementStats: MeasurementStats = {
  gravel: {
    truePositive: 0,
    trueNegative: 0,
    falsePositive: 0,
    falseNegative: 0,
  },
  oldWatchQuery: {
    truePositive: 0,
    trueNegative: 0,
    falsePositive: 0,
    falseNegative: 0,
  },
  totalTests: 0,
};

// Get measurement statistics
router.get("/measurements", (req: Request, res: Response) => {
  res.json(measurementStats);
});

// Reset measurement statistics
router.post("/measurements/reset", (req: Request, res: Response) => {
  measurementStats = {
    gravel: {
      truePositive: 0,
      trueNegative: 0,
      falsePositive: 0,
      falseNegative: 0,
    },
    oldWatchQuery: {
      truePositive: 0,
      trueNegative: 0,
      falsePositive: 0,
      falseNegative: 0,
    },
    totalTests: 0,
  };
  res.json({ success: true, message: "Measurements reset" });
});

// Record a measurement
router.post("/measurements/record", (req: Request, res: Response) => {
  const { measurements } = req.body;

  if (!measurements || !Array.isArray(measurements)) {
    res.status(400).json({ error: "Missing or invalid measurements array" });
    return;
  }

  const validOutcomes = [
    "truePositive",
    "trueNegative",
    "falsePositive",
    "falseNegative",
  ];

  // Process each measurement
  for (const measurement of measurements) {
    const { system, outcome } = measurement;

    if (!system || !outcome) {
      res
        .status(400)
        .json({ error: "Missing system or outcome in measurement" });
      return;
    }

    if (system !== "gravel" && system !== "oldWatchQuery") {
      res
        .status(400)
        .json({ error: "Invalid system. Must be 'gravel' or 'oldWatchQuery'" });
      return;
    }

    if (!validOutcomes.includes(outcome)) {
      res.status(400).json({ error: "Invalid outcome" });
      return;
    }

    const systemKey = system as "gravel" | "oldWatchQuery";
    measurementStats[systemKey][
      outcome as keyof typeof measurementStats.gravel
    ]++;
  }

  // Only increment totalTests once per request
  measurementStats.totalTests++;

  res.json({ success: true, stats: measurementStats });
});
