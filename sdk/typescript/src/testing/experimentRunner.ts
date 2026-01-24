import { ObjectId } from "mongodb";
import {
  type GravelTestData,
  DEFAULT_COLLECTION_SIZE,
  DEFAULT_EXPERIMENT_SEED,
  experimentQueries,
  REPETITIONS_PER_QUERY,
  UPDATES_PER_QUERY,
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
  type ConfusionMatrix,
  classifyOutcome,
  closeWatchers,
  createExperimentWatchers,
} from "./experimentWatchers.js";
import { getMongoClient } from "./mongoClient.js";
import { resetRandomGenerators } from "./randomGenerator.js";
import { regenerateDatabase } from "./regenerateDatabase.js";

export interface ExperimentConfig {
  seed?: number;
  updatesPerQuery?: number;
  repetitionsPerQuery?: number;
  collectionSize?: number;
  outputDir?: string;
}

export interface UpdateMetric {
  updateNumber: number;
  operationType: string;
  groundTruthChanged: boolean;
  gravelOutcome: "TP" | "TN" | "FP" | "FN";
  oldWatchQueryOutcome: "TP" | "TN" | "FP" | "FN";
  gravelCorrect: boolean;
  oldWatchQueryCorrect: boolean;
  durationMs: number;
  gravelLatencyMs: number;
  oldWatchQueryLatencyMs: number;
}

export interface RepetitionResult {
  repetitionNumber: number;
  gravelConfusionMatrix: ConfusionMatrix;
  oldWatchQueryConfusionMatrix: ConfusionMatrix;
  metrics: UpdateMetric[];
  startTime: number;
  endTime: number;
}

export interface QueryResult {
  queryIndex: number;
  queryName: string;
  query: Record<string, any>;
  repetitions: RepetitionResult[];
  aggregatedGravelMatrix: ConfusionMatrix;
  aggregatedOldWatchQueryMatrix: ConfusionMatrix;
}

export interface ExperimentResult {
  experimentId: string;
  seed: number;
  updatesPerQuery: number;
  repetitionsPerQuery: number;
  collectionSize: number;
  startTime: number;
  endTime: number;
  queryResults: QueryResult[];
  totalGravelMatrix: ConfusionMatrix;
  totalOldWatchQueryMatrix: ConfusionMatrix;
}

let experimentRunning = false;
let stopRequested = false;

export interface LiveExperimentState {
  running: boolean;
  experimentId: string | null;
  seed: number;
  updatesPerQuery: number;
  repetitionsPerQuery: number;
  totalQueries: number;
  currentQueryIndex: number;
  currentQueryName: string;
  currentRepetition: number;
  currentUpdateNumber: number;
  phase:
    | "idle"
    | "regenerating_db"
    | "starting_watchers"
    | "running_updates"
    | "complete";
  startTime: number | null;
  lastUpdateTime: number | null;
  completedQueries: {
    queryIndex: number;
    queryName: string;
    durationMs: number;
    gravelMatrix: [number, number, number, number]; // [TP, TN, FP, FN]
    oldWatchQueryMatrix: [number, number, number, number]; // [TP, TN, FP, FN]
  }[];
  currentGravelMatrix: [number, number, number, number]; // [TP, TN, FP, FN]
  currentOldWatchQueryMatrix: [number, number, number, number]; // [TP, TN, FP, FN]
  aggregatedGravelMatrix: [number, number, number, number]; // [TP, TN, FP, FN]
  aggregatedOldWatchQueryMatrix: [number, number, number, number]; // [TP, TN, FP, FN]
  lastOperationType: string;
  lastGravelOutcome: string;
  lastOldWatchQueryOutcome: string;
  lastGravelLatencyMs: number;
  lastOldWatchQueryLatencyMs: number;
  latencyHistory: {
    updateNumber: number;
    gravelLatencyMs: number;
    oldWatchQueryLatencyMs: number;
  }[];
  error: string | null;
}

let liveState: LiveExperimentState = createInitialLiveState();

function createInitialLiveState(): LiveExperimentState {
  return {
    running: false,
    experimentId: null,
    seed: DEFAULT_EXPERIMENT_SEED,
    updatesPerQuery: UPDATES_PER_QUERY,
    repetitionsPerQuery: REPETITIONS_PER_QUERY,
    totalQueries: 0,
    currentQueryIndex: 0,
    currentQueryName: "",
    currentRepetition: 0,
    currentUpdateNumber: 0,
    phase: "idle",
    startTime: null,
    lastUpdateTime: null,
    completedQueries: [],
    currentGravelMatrix: [0, 0, 0, 0],
    currentOldWatchQueryMatrix: [0, 0, 0, 0],
    aggregatedGravelMatrix: [0, 0, 0, 0],
    aggregatedOldWatchQueryMatrix: [0, 0, 0, 0],
    lastOperationType: "",
    lastGravelOutcome: "",
    lastOldWatchQueryOutcome: "",
    lastGravelLatencyMs: 0,
    lastOldWatchQueryLatencyMs: 0,
    latencyHistory: [],
    error: null,
  };
}

function createEmptyMatrix(): ConfusionMatrix {
  return {
    truePositive: 0,
    trueNegative: 0,
    falsePositive: 0,
    falseNegative: 0,
  };
}

function matrixToArray(
  matrix: ConfusionMatrix,
): [number, number, number, number] {
  return [
    matrix.truePositive,
    matrix.trueNegative,
    matrix.falsePositive,
    matrix.falseNegative,
  ];
}

function updateAggregatedMatrices(): void {
  const aggregatedGravel: [number, number, number, number] = [0, 0, 0, 0];
  const aggregatedOld: [number, number, number, number] = [0, 0, 0, 0];

  // Add all completed queries
  for (const q of liveState.completedQueries) {
    aggregatedGravel[0] += q.gravelMatrix[0]; // TP
    aggregatedGravel[1] += q.gravelMatrix[1]; // TN
    aggregatedGravel[2] += q.gravelMatrix[2]; // FP
    aggregatedGravel[3] += q.gravelMatrix[3]; // FN
    aggregatedOld[0] += q.oldWatchQueryMatrix[0];
    aggregatedOld[1] += q.oldWatchQueryMatrix[1];
    aggregatedOld[2] += q.oldWatchQueryMatrix[2];
    aggregatedOld[3] += q.oldWatchQueryMatrix[3];
  }

  // Add current query if running
  if (liveState.running) {
    aggregatedGravel[0] += liveState.currentGravelMatrix[0];
    aggregatedGravel[1] += liveState.currentGravelMatrix[1];
    aggregatedGravel[2] += liveState.currentGravelMatrix[2];
    aggregatedGravel[3] += liveState.currentGravelMatrix[3];
    aggregatedOld[0] += liveState.currentOldWatchQueryMatrix[0];
    aggregatedOld[1] += liveState.currentOldWatchQueryMatrix[1];
    aggregatedOld[2] += liveState.currentOldWatchQueryMatrix[2];
    aggregatedOld[3] += liveState.currentOldWatchQueryMatrix[3];
  }

  liveState.aggregatedGravelMatrix = aggregatedGravel;
  liveState.aggregatedOldWatchQueryMatrix = aggregatedOld;
}

function addToMatrix(
  matrix: ConfusionMatrix,
  outcome: "TP" | "TN" | "FP" | "FN",
): void {
  switch (outcome) {
    case "TP":
      matrix.truePositive++;
      break;
    case "TN":
      matrix.trueNegative++;
      break;
    case "FP":
      matrix.falsePositive++;
      break;
    case "FN":
      matrix.falseNegative++;
      break;
  }
}

function aggregateMatrices(matrices: ConfusionMatrix[]): ConfusionMatrix {
  return matrices.reduce(
    (acc, m) => ({
      truePositive: acc.truePositive + m.truePositive,
      trueNegative: acc.trueNegative + m.trueNegative,
      falsePositive: acc.falsePositive + m.falsePositive,
      falseNegative: acc.falseNegative + m.falseNegative,
    }),
    createEmptyMatrix(),
  );
}

export function isExperimentRunning(): boolean {
  return experimentRunning;
}

export function stopExperiment(): void {
  if (experimentRunning) {
    console.log("Stop requested by user");
    stopRequested = true;
    liveState.error = "Stopped by user";
  }
}

export function getLiveExperimentState(): LiveExperimentState {
  return { ...liveState };
}

export function generateExperimentId(): string {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `experiment_${timestamp}`;
}

async function performRandomUpdate(
  collection: any,
  totalUsers: number,
): Promise<{ operationType: string }> {
  const targetSize = DEFAULT_COLLECTION_SIZE;
  const sizeDelta = totalUsers - targetSize;

  let bulkOps: any[] = [];
  const operations = ["update", "replace", "delete", "insert"];

  let selectedOp: string;
  if (sizeDelta > targetSize * 0.1) {
    selectedOp =
      Math.random() < 0.6
        ? "delete"
        : operations[Math.floor(Math.random() * operations.length)];
  } else if (sizeDelta < -targetSize * 0.1) {
    selectedOp =
      Math.random() < 0.6
        ? "insert"
        : operations[Math.floor(Math.random() * operations.length)];
  } else {
    selectedOp = operations[Math.floor(Math.random() * operations.length)];
  }

  switch (selectedOp) {
    case "update": {
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
      }
      break;
    }
    case "replace": {
      const usersToReplace = await collection
        .aggregate([{ $sample: { size: 1 } }])
        .toArray();
      for (const user of usersToReplace) {
        const replacementDocument: GravelTestData = {
          _id: user._id,
          email: generateRandomEmail(),
          roles: generateRandomRoles(),
          address: generateRandomAddress(),
          debitor: generateRandomDebitor(),
          tags: generateRandomTags(),
          sepa: generateRandomSepa(),
        };
        if (Math.random() > 0.5) {
          replacementDocument.archived = Math.random() > 0.5;
        }
        if (Math.random() > 0.3) {
          replacementDocument.birthday = generateRandomBirthday();
        }
        bulkOps.push({
          replaceOne: {
            filter: { _id: user._id },
            replacement: replacementDocument,
          },
        });
      }
      break;
    }
    case "delete": {
      if (totalUsers > Math.max(100, targetSize * 0.1)) {
        const usersToDelete = await collection
          .aggregate([{ $sample: { size: 1 } }])
          .toArray();
        for (const user of usersToDelete) {
          bulkOps.push({ deleteOne: { filter: { _id: user._id } } });
        }
      } else {
        selectedOp = "insert";
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
        bulkOps.push({ insertOne: { document: newDocument } });
      }
      break;
    }
    case "insert": {
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
      bulkOps.push({ insertOne: { document: newDocument } });
      break;
    }
  }

  if (bulkOps.length > 0) {
    await collection.bulkWrite(bulkOps, { ordered: false });
  }

  return { operationType: selectedOp };
}

async function runRepetition(
  queryIndex: number,
  queryName: string,
  query: Record<string, any>,
  options: any,
  repetitionNumber: number,
  updatesPerQuery: number,
  dbCollectionSize: number,
  seed: number,
): Promise<RepetitionResult> {
  const client = await getMongoClient();
  const collection = client.db().collection<GravelTestData>("users");
  const watchers = createExperimentWatchers();

  const gravelMatrix = createEmptyMatrix();
  const oldWatchQueryMatrix = createEmptyMatrix();
  const metrics: UpdateMetric[] = [];
  const startTime = Date.now();

  resetRandomGenerators(seed);

  liveState.phase = "regenerating_db";
  console.log(
    `    Regenerating database with ${dbCollectionSize} documents...`,
  );
  await regenerateDatabase(dbCollectionSize, "users");

  liveState.phase = "starting_watchers";
  console.log(`    Starting watchers...`);
  await watchers.startWatching("users", query, options);

  liveState.phase = "running_updates";
  liveState.currentRepetition = repetitionNumber;
  liveState.currentGravelMatrix = [0, 0, 0, 0];
  liveState.currentOldWatchQueryMatrix = [0, 0, 0, 0];

  // Reset latency history for new repetition
  if (repetitionNumber === 1) {
    liveState.latencyHistory = [];
  }

  console.log(`    Running ${updatesPerQuery} updates...`);

  for (let i = 0; i < updatesPerQuery; i++) {
    if (stopRequested) {
      console.log("    Stopping due to user request...");
      break;
    }

    liveState.currentUpdateNumber = i + 1;

    const gravelStateBefore = [...watchers.gravelState.currentData];
    const oldWatchQueryStateBefore = [
      ...watchers.oldWatchQueryState.currentData,
    ];

    watchers.resetUpdateFlags();

    const updateStartTime = Date.now();
    const totalUsers = await collection.countDocuments({});
    const { operationType } = await performRandomUpdate(collection, totalUsers);
    const updateIssuedTime = Date.now();

    await watchers.waitForUpdates(3000);

    const groundTruth = await watchers.getGroundTruth("users", query, options);

    const gravelStateAfter = [...watchers.gravelState.currentData];
    const oldWatchQueryStateAfter = [
      ...watchers.oldWatchQueryState.currentData,
    ];

    const gravelOutcome = classifyOutcome(
      gravelStateBefore,
      gravelStateAfter,
      groundTruth,
      watchers.gravelState.lastUpdateReceived,
      watchers.gravelState.lastUpdateWasNoop,
    );
    const oldWatchQueryOutcome = classifyOutcome(
      oldWatchQueryStateBefore,
      oldWatchQueryStateAfter,
      groundTruth,
      watchers.oldWatchQueryState.lastUpdateReceived,
      watchers.oldWatchQueryState.lastUpdateWasNoop,
    );

    addToMatrix(gravelMatrix, gravelOutcome);
    addToMatrix(oldWatchQueryMatrix, oldWatchQueryOutcome);

    const updateEndTime = Date.now();

    const groundTruthChanged =
      JSON.stringify(gravelStateBefore) !== JSON.stringify(groundTruth);

    // Calculate latencies: time from update issued to watcher response (clamped at 0ms)
    const gravelLatencyMs = watchers.gravelState.lastUpdateTimestamp
      ? Math.max(0, watchers.gravelState.lastUpdateTimestamp - updateIssuedTime)
      : 0;
    const oldWatchQueryLatencyMs = watchers.oldWatchQueryState
      .lastUpdateTimestamp
      ? Math.max(
          0,
          watchers.oldWatchQueryState.lastUpdateTimestamp - updateIssuedTime,
        )
      : 0;

    const metric: UpdateMetric = {
      updateNumber: i + 1,
      operationType,
      groundTruthChanged,
      gravelOutcome,
      oldWatchQueryOutcome,
      gravelCorrect: gravelOutcome === "TP" || gravelOutcome === "TN",
      oldWatchQueryCorrect:
        oldWatchQueryOutcome === "TP" || oldWatchQueryOutcome === "TN",
      durationMs: updateEndTime - updateStartTime,
      gravelLatencyMs,
      oldWatchQueryLatencyMs,
    };

    metrics.push(metric);

    liveState.lastUpdateTime = updateEndTime;
    liveState.lastOperationType = operationType;
    liveState.lastGravelOutcome = gravelOutcome;
    liveState.lastOldWatchQueryOutcome = oldWatchQueryOutcome;
    liveState.lastGravelLatencyMs = gravelLatencyMs;
    liveState.lastOldWatchQueryLatencyMs = oldWatchQueryLatencyMs;

    // Add to latency history for graphing (keep last 500 points)
    liveState.latencyHistory.push({
      updateNumber: i + 1,
      gravelLatencyMs,
      oldWatchQueryLatencyMs,
    });
    if (liveState.latencyHistory.length > 500) {
      liveState.latencyHistory.shift();
    }

    liveState.currentGravelMatrix = matrixToArray(gravelMatrix);
    liveState.currentOldWatchQueryMatrix = matrixToArray(oldWatchQueryMatrix);
    updateAggregatedMatrices();

    if ((i + 1) % 100 === 0) {
      const gravelAccuracy =
        ((gravelMatrix.truePositive + gravelMatrix.trueNegative) / (i + 1)) *
        100;
      const oldAccuracy =
        ((oldWatchQueryMatrix.truePositive + oldWatchQueryMatrix.trueNegative) /
          (i + 1)) *
        100;
      console.log(
        `      ${i + 1}/${updatesPerQuery} - Gravel: ${gravelAccuracy.toFixed(1)}% | OldWatchQuery: ${oldAccuracy.toFixed(1)}%`,
      );
    }
  }

  await watchers.stopWatching();

  const endTime = Date.now();

  return {
    repetitionNumber,
    gravelConfusionMatrix: gravelMatrix,
    oldWatchQueryConfusionMatrix: oldWatchQueryMatrix,
    metrics,
    startTime,
    endTime,
  };
}

export async function runExperimentSuite(
  config: ExperimentConfig = {},
): Promise<ExperimentResult> {
  const {
    seed = DEFAULT_EXPERIMENT_SEED,
    updatesPerQuery = UPDATES_PER_QUERY,
    repetitionsPerQuery = REPETITIONS_PER_QUERY,
    collectionSize: dbCollectionSize = DEFAULT_COLLECTION_SIZE,
    outputDir = "./experiment_results",
  } = config;

  if (experimentRunning) {
    throw new Error("An experiment is already running");
  }

  experimentRunning = true;
  const experimentId = generateExperimentId();
  const startTime = Date.now();

  liveState = {
    ...createInitialLiveState(),
    running: true,
    experimentId,
    seed,
    updatesPerQuery,
    repetitionsPerQuery,
    totalQueries: experimentQueries.length,
    startTime,
  };

  console.log("=".repeat(70));
  console.log(`Starting Experiment Suite: ${experimentId}`);
  console.log(`Seed: ${seed}`);
  console.log(`Updates per query: ${updatesPerQuery}`);
  console.log(`Repetitions per query: ${repetitionsPerQuery}`);
  console.log(`Collection size: ${dbCollectionSize}`);
  console.log(`Number of queries: ${experimentQueries.length}`);
  console.log("=".repeat(70));

  const queryResults: QueryResult[] = [];

  try {
    for (let qi = 0; qi < experimentQueries.length; qi++) {
      if (stopRequested) {
        console.log("\nExperiment stopped by user request");
        break;
      }

      const experimentQuery = experimentQueries[qi];
      console.log(
        `\n[Query ${qi + 1}/${experimentQueries.length}] "${experimentQuery.name}"`,
      );

      liveState.currentQueryIndex = qi;
      liveState.currentQueryName = experimentQuery.name;

      const repetitions: RepetitionResult[] = [];

      for (let rep = 1; rep <= repetitionsPerQuery; rep++) {
        if (stopRequested) {
          console.log("  Stopping repetitions due to user request...");
          break;
        }

        console.log(`  Repetition ${rep}/${repetitionsPerQuery}:`);

        const repetitionResult = await runRepetition(
          qi,
          experimentQuery.name,
          experimentQuery.query,
          experimentQuery.options,
          rep,
          updatesPerQuery,
          dbCollectionSize,
          seed,
        );

        repetitions.push(repetitionResult);

        const gravelAcc =
          ((repetitionResult.gravelConfusionMatrix.truePositive +
            repetitionResult.gravelConfusionMatrix.trueNegative) /
            updatesPerQuery) *
          100;
        const oldAcc =
          ((repetitionResult.oldWatchQueryConfusionMatrix.truePositive +
            repetitionResult.oldWatchQueryConfusionMatrix.trueNegative) /
            updatesPerQuery) *
          100;

        console.log(
          `    Completed in ${repetitionResult.endTime - repetitionResult.startTime}ms`,
        );
        console.log(
          `    Gravel: ${gravelAcc.toFixed(2)}% accuracy | OldWatchQuery: ${oldAcc.toFixed(2)}% accuracy`,
        );
      }

      const aggregatedGravelMatrix = aggregateMatrices(
        repetitions.map((r) => r.gravelConfusionMatrix),
      );
      const aggregatedOldWatchQueryMatrix = aggregateMatrices(
        repetitions.map((r) => r.oldWatchQueryConfusionMatrix),
      );

      const queryResult: QueryResult = {
        queryIndex: qi,
        queryName: experimentQuery.name,
        query: experimentQuery.query,
        repetitions,
        aggregatedGravelMatrix,
        aggregatedOldWatchQueryMatrix,
      };

      queryResults.push(queryResult);

      liveState.completedQueries.push({
        queryIndex: qi,
        queryName: experimentQuery.name,
        durationMs: repetitions.reduce(
          (sum, r) => sum + (r.endTime - r.startTime),
          0,
        ),
        gravelMatrix: matrixToArray(aggregatedGravelMatrix),
        oldWatchQueryMatrix: matrixToArray(aggregatedOldWatchQueryMatrix),
      });
      updateAggregatedMatrices();

      const totalUpdates = updatesPerQuery * repetitionsPerQuery;
      const gravelTotal =
        aggregatedGravelMatrix.truePositive +
        aggregatedGravelMatrix.trueNegative;
      const oldTotal =
        aggregatedOldWatchQueryMatrix.truePositive +
        aggregatedOldWatchQueryMatrix.trueNegative;

      console.log(`\n  Query "${experimentQuery.name}" Summary:`);
      console.log(
        `    Gravel:         TP=${aggregatedGravelMatrix.truePositive} TN=${aggregatedGravelMatrix.trueNegative} FP=${aggregatedGravelMatrix.falsePositive} FN=${aggregatedGravelMatrix.falseNegative} (${((gravelTotal / totalUpdates) * 100).toFixed(2)}% correct)`,
      );
      console.log(
        `    OldWatchQuery:  TP=${aggregatedOldWatchQueryMatrix.truePositive} TN=${aggregatedOldWatchQueryMatrix.trueNegative} FP=${aggregatedOldWatchQueryMatrix.falsePositive} FN=${aggregatedOldWatchQueryMatrix.falseNegative} (${((oldTotal / totalUpdates) * 100).toFixed(2)}% correct)`,
      );
    }

    const totalGravelMatrix = aggregateMatrices(
      queryResults.map((q) => q.aggregatedGravelMatrix),
    );
    const totalOldWatchQueryMatrix = aggregateMatrices(
      queryResults.map((q) => q.aggregatedOldWatchQueryMatrix),
    );

    const endTime = Date.now();

    const result: ExperimentResult = {
      experimentId,
      seed,
      updatesPerQuery,
      repetitionsPerQuery,
      collectionSize: dbCollectionSize,
      startTime,
      endTime,
      queryResults,
      totalGravelMatrix,
      totalOldWatchQueryMatrix,
    };

    console.log("\n" + "=".repeat(70));
    console.log(`Experiment Suite Complete: ${experimentId}`);
    console.log(`Total duration: ${endTime - startTime}ms`);
    console.log("\nFinal Confusion Matrices:");
    console.log(
      `  Gravel:         TP=${totalGravelMatrix.truePositive} TN=${totalGravelMatrix.trueNegative} FP=${totalGravelMatrix.falsePositive} FN=${totalGravelMatrix.falseNegative}`,
    );
    console.log(
      `  OldWatchQuery:  TP=${totalOldWatchQueryMatrix.truePositive} TN=${totalOldWatchQueryMatrix.trueNegative} FP=${totalOldWatchQueryMatrix.falsePositive} FN=${totalOldWatchQueryMatrix.falseNegative}`,
    );
    console.log("=".repeat(70));

    liveState.phase = "complete";
    liveState.running = false;

    await closeWatchers();

    return result;
  } catch (error) {
    liveState.error = error instanceof Error ? error.message : "Unknown error";
    liveState.phase = "idle";
    liveState.running = false;

    await closeWatchers();
    throw error;
  } finally {
    experimentRunning = false;
    stopRequested = false;
  }
}
