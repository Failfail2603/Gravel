import express, { type Request, type Response } from "express";
import path from "path";
import { fileURLToPath } from "url";
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
