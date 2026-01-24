import fs from "fs";
import path from "path";

interface ConfusionMatrix {
  truePositive: number;
  trueNegative: number;
  falsePositive: number;
  falseNegative: number;
}

// Define interfaces locally to avoid circular dependency issues
interface UpdateMetric {
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

interface RepetitionResult {
  repetitionNumber: number;
  gravelConfusionMatrix: ConfusionMatrix;
  oldWatchQueryConfusionMatrix: ConfusionMatrix;
  metrics: UpdateMetric[];
  startTime: number;
  endTime: number;
}

interface QueryResult {
  queryIndex: number;
  queryName: string;
  query: Record<string, any>;
  repetitions: RepetitionResult[];
  aggregatedGravelMatrix: ConfusionMatrix;
  aggregatedOldWatchQueryMatrix: ConfusionMatrix;
}

interface ExperimentResult {
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

/**
 * Save experiment results to CSV files
 */
export async function saveExperimentToCSV(
  result: ExperimentResult,
  outputDir: string = "./experiment_results",
): Promise<string[]> {
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  const savedFiles: string[] = [];
  const expDir = path.join(outputDir, result.experimentId);

  if (!fs.existsSync(expDir)) {
    fs.mkdirSync(expDir, { recursive: true });
  }

  // Save detailed metrics per query/repetition
  for (const queryResult of result.queryResults) {
    for (const rep of queryResult.repetitions) {
      const filename = `query_${queryResult.queryIndex}_rep_${rep.repetitionNumber}_metrics.csv`;
      const filepath = path.join(expDir, filename);
      const csvContent = metricsToCSV(rep.metrics);
      fs.writeFileSync(filepath, csvContent, "utf-8");
      savedFiles.push(filepath);
    }
  }

  // Save confusion matrix summary per query
  const queryMatrixFile = path.join(expDir, "query_confusion_matrices.csv");
  const queryMatrixContent = queryConfusionMatricesToCSV(result);
  fs.writeFileSync(queryMatrixFile, queryMatrixContent, "utf-8");
  savedFiles.push(queryMatrixFile);

  // Save experiment summary
  const summaryFile = path.join(expDir, "experiment_summary.csv");
  const summaryContent = experimentSummaryToCSV(result);
  fs.writeFileSync(summaryFile, summaryContent, "utf-8");
  savedFiles.push(summaryFile);

  console.log(`Saved ${savedFiles.length} CSV files to ${expDir}`);
  return savedFiles;
}

function metricsToCSV(metrics: UpdateMetric[]): string {
  if (metrics.length === 0) return "";

  const headers = [
    "updateNumber",
    "operationType",
    "groundTruthChanged",
    "gravelOutcome",
    "oldWatchQueryOutcome",
    "gravelCorrect",
    "oldWatchQueryCorrect",
    "durationMs",
    "gravelLatencyMs",
    "oldWatchQueryLatencyMs",
  ];

  const rows: string[] = [headers.join(",")];

  for (const m of metrics) {
    rows.push(
      [
        m.updateNumber,
        m.operationType,
        m.groundTruthChanged,
        m.gravelOutcome,
        m.oldWatchQueryOutcome,
        m.gravelCorrect,
        m.oldWatchQueryCorrect,
        m.durationMs,
        m.gravelLatencyMs,
        m.oldWatchQueryLatencyMs,
      ].join(","),
    );
  }

  return rows.join("\n");
}

function queryConfusionMatricesToCSV(result: ExperimentResult): string {
  const headers = [
    "queryIndex",
    "queryName",
    "system",
    "truePositive",
    "trueNegative",
    "falsePositive",
    "falseNegative",
    "accuracy",
    "precision",
    "recall",
    "f1Score",
  ];

  const rows: string[] = [headers.join(",")];

  for (const q of result.queryResults) {
    // Gravel row
    const gravelMetrics = calculateMetrics(q.aggregatedGravelMatrix);
    rows.push(
      [
        q.queryIndex,
        `"${q.queryName}"`,
        "gravel",
        q.aggregatedGravelMatrix.truePositive,
        q.aggregatedGravelMatrix.trueNegative,
        q.aggregatedGravelMatrix.falsePositive,
        q.aggregatedGravelMatrix.falseNegative,
        gravelMetrics.accuracy.toFixed(4),
        gravelMetrics.precision.toFixed(4),
        gravelMetrics.recall.toFixed(4),
        gravelMetrics.f1Score.toFixed(4),
      ].join(","),
    );

    // OldWatchQuery row
    const oldMetrics = calculateMetrics(q.aggregatedOldWatchQueryMatrix);
    rows.push(
      [
        q.queryIndex,
        `"${q.queryName}"`,
        "oldWatchQuery",
        q.aggregatedOldWatchQueryMatrix.truePositive,
        q.aggregatedOldWatchQueryMatrix.trueNegative,
        q.aggregatedOldWatchQueryMatrix.falsePositive,
        q.aggregatedOldWatchQueryMatrix.falseNegative,
        oldMetrics.accuracy.toFixed(4),
        oldMetrics.precision.toFixed(4),
        oldMetrics.recall.toFixed(4),
        oldMetrics.f1Score.toFixed(4),
      ].join(","),
    );
  }

  return rows.join("\n");
}

function experimentSummaryToCSV(result: ExperimentResult): string {
  const gravelMetrics = calculateMetrics(result.totalGravelMatrix);
  const oldMetrics = calculateMetrics(result.totalOldWatchQueryMatrix);

  const lines = [
    "Experiment Summary",
    `experimentId,${result.experimentId}`,
    `seed,${result.seed}`,
    `updatesPerQuery,${result.updatesPerQuery}`,
    `repetitionsPerQuery,${result.repetitionsPerQuery}`,
    `collectionSize,${result.collectionSize}`,
    `totalQueries,${result.queryResults.length}`,
    `totalDurationMs,${result.endTime - result.startTime}`,
    "",
    "Total Confusion Matrices",
    "system,truePositive,trueNegative,falsePositive,falseNegative,accuracy,precision,recall,f1Score",
    `gravel,${result.totalGravelMatrix.truePositive},${result.totalGravelMatrix.trueNegative},${result.totalGravelMatrix.falsePositive},${result.totalGravelMatrix.falseNegative},${gravelMetrics.accuracy.toFixed(4)},${gravelMetrics.precision.toFixed(4)},${gravelMetrics.recall.toFixed(4)},${gravelMetrics.f1Score.toFixed(4)}`,
    `oldWatchQuery,${result.totalOldWatchQueryMatrix.truePositive},${result.totalOldWatchQueryMatrix.trueNegative},${result.totalOldWatchQueryMatrix.falsePositive},${result.totalOldWatchQueryMatrix.falseNegative},${oldMetrics.accuracy.toFixed(4)},${oldMetrics.precision.toFixed(4)},${oldMetrics.recall.toFixed(4)},${oldMetrics.f1Score.toFixed(4)}`,
  ];

  return lines.join("\n");
}

function calculateMetrics(matrix: ConfusionMatrix): {
  accuracy: number;
  precision: number;
  recall: number;
  f1Score: number;
} {
  const total =
    matrix.truePositive +
    matrix.trueNegative +
    matrix.falsePositive +
    matrix.falseNegative;

  const accuracy =
    total > 0 ? (matrix.truePositive + matrix.trueNegative) / total : 0;

  const precision =
    matrix.truePositive + matrix.falsePositive > 0
      ? matrix.truePositive / (matrix.truePositive + matrix.falsePositive)
      : 0;

  const recall =
    matrix.truePositive + matrix.falseNegative > 0
      ? matrix.truePositive / (matrix.truePositive + matrix.falseNegative)
      : 0;

  const f1Score =
    precision + recall > 0
      ? (2 * precision * recall) / (precision + recall)
      : 0;

  return { accuracy, precision, recall, f1Score };
}
