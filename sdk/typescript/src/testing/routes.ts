import express, { type Request, type Response } from "express";
import type { Operation } from "fast-json-patch";
import patch from "fast-json-patch";
import path from "path";
import type { Subscription } from "rxjs";
import { fileURLToPath } from "url";
import { getGravelConnection, GravelDBs } from "../gravel.js";
import { experimentQueries, MONGO_URL } from "./config.js";
import {
  getLiveExperimentState,
  isExperimentRunning,
  runExperimentSuite,
  stopExperiment,
} from "./experimentRunner.js";
import "./randomGenerator.js";

// ES module equivalent of __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const router = express.Router();

// Serve static HTML
router.get("/", (req: Request, res: Response) => {
  res.sendFile(path.join(__dirname, "viewer.html"));
});

// Start the experiment suite
router.post("/experiment", async (req: Request, res: Response) => {
  try {
    // Check if experiment is already running
    if (isExperimentRunning()) {
      res.status(409).json({ error: "An experiment is already running" });
      return;
    }

    // Get optional parameters from request body
    const {
      seed,
      updatesPerQuery,
      repetitionsPerQuery,
      collectionSize,
      outputDir,
    } = req.body;

    console.log("Starting experiment suite via API...");

    // Run experiment suite (this is a long-running operation)
    void runExperimentSuite({
      seed: seed ?? undefined,
      updatesPerQuery: updatesPerQuery ?? undefined,
      repetitionsPerQuery: repetitionsPerQuery ?? undefined,
      collectionSize: collectionSize ?? undefined,
      outputDir: outputDir ?? undefined,
    });

    res.status(200).json({
      success: true,
    });
  } catch (error) {
    console.error("Experiment suite error:", error);
    res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

// Stop the experiment
router.post("/experiment/stop", (req: Request, res: Response) => {
  try {
    if (!isExperimentRunning()) {
      res.status(400).json({ error: "No experiment is currently running" });
      return;
    }

    stopExperiment();
    res.json({ success: true, message: "Stop request sent" });
  } catch (error) {
    console.error("Error stopping experiment:", error);
    res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

// Get live experiment state (for polling)
router.get("/experiment/state", (req: Request, res: Response) => {
  res.json(getLiveExperimentState());
});

// ============ Manual WatchQuery Testing ============

let manualGravel: Awaited<ReturnType<typeof getGravelConnection>> | null = null;
let manualGravelClient: any = null;
let manualStopWatchQuery: (() => Promise<void>) | null = null;
let manualSubscription: Subscription | null = null;
let manualCurrentData: any[] = [];
let manualQueryIndex: number | null = null;

async function initManualGravel(): Promise<void> {
  if (!manualGravel) {
    manualGravel = await getGravelConnection({ timeoutMs: 30000 });
  }
  if (!manualGravelClient) {
    manualGravelClient = await manualGravel.getDatabaseClient({
      db: GravelDBs.MongoDB,
      mongoUrl: MONGO_URL,
    });
  }
}

// Start manual watchquery on an experimental query by index
router.post("/manual/start/:index", async (req: Request, res: Response) => {
  try {
    const index = parseInt(req.params.index, 10);

    if (isNaN(index) || index < 0 || index >= experimentQueries.length) {
      res.status(400).json({
        error: `Invalid index. Must be 0-${experimentQueries.length - 1}`,
        availableQueries: experimentQueries.map((q, i) => ({
          index: i,
          name: q.name,
        })),
      });
      return;
    }

    // Stop existing watchquery if any
    if (manualSubscription) {
      manualSubscription.unsubscribe();
      manualSubscription = null;
    }
    if (manualStopWatchQuery) {
      await manualStopWatchQuery();
      manualStopWatchQuery = null;
    }

    await initManualGravel();

    const queryConfig = experimentQueries[index];
    console.log(`\n========== MANUAL WATCHQUERY START ==========`);
    console.log(`Query: ${queryConfig.name} (index ${index})`);
    console.log(`Filter:`, JSON.stringify(queryConfig.query, null, 2));
    console.log(`Options:`, JSON.stringify(queryConfig.options, null, 2));

    const { initialQuery, changes, stop } = await manualGravelClient.watchQuery(
      "users",
      queryConfig.query,
      queryConfig.options,
    );
    manualStopWatchQuery = stop;
    manualQueryIndex = index;

    // Set initial data
    manualCurrentData = initialQuery.result || initialQuery;
    console.log(
      `\n--- Initial Data (${manualCurrentData.length} documents) ---`,
    );
    console.log(JSON.stringify(manualCurrentData, null, 2));

    // Subscribe to changes
    manualSubscription = changes.subscribe((patches: Operation[]) => {
      console.log(`\n--- Patch Received ---`);
      console.log(`Patches:`, JSON.stringify(patches, null, 2));

      if (patches.length > 0) {
        const currentDoc = { result: manualCurrentData };
        const patchResult = patch.applyPatch(currentDoc, patches, false, false);
        manualCurrentData = patchResult.newDocument.result;
        console.log(`Updated data (${manualCurrentData.length} documents):`);
        console.log(JSON.stringify(manualCurrentData, null, 2));
      } else {
        console.log(`NOOP - no changes to data`);
      }
    });

    res.json({
      success: true,
      queryName: queryConfig.name,
      queryIndex: index,
      initialDocumentCount: manualCurrentData.length,
    });
  } catch (error) {
    console.error("Error starting manual watchquery:", error);
    res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

// Stop manual watchquery
router.post("/manual/stop", async (req: Request, res: Response) => {
  try {
    if (!manualSubscription && !manualStopWatchQuery) {
      res.status(400).json({ error: "No manual watchquery is running" });
      return;
    }

    console.log(`\n========== MANUAL WATCHQUERY STOP ==========`);

    if (manualSubscription) {
      manualSubscription.unsubscribe();
      manualSubscription = null;
    }
    if (manualStopWatchQuery) {
      await manualStopWatchQuery();
      manualStopWatchQuery = null;
    }

    const stoppedIndex = manualQueryIndex;
    manualQueryIndex = null;
    manualCurrentData = [];

    console.log(`Manual watchquery stopped`);

    res.json({ success: true, stoppedQueryIndex: stoppedIndex });
  } catch (error) {
    console.error("Error stopping manual watchquery:", error);
    res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown error",
    });
  }
});

// Get available queries
router.get("/manual/queries", (req: Request, res: Response) => {
  res.json({
    queries: experimentQueries.map((q, i) => ({
      index: i,
      name: q.name,
      query: q.query,
      options: q.options,
    })),
    activeQueryIndex: manualQueryIndex,
  });
});
