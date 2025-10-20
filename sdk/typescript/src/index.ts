import express, { type Request, type Response } from "express";
import patch, { type Operation } from "fast-json-patch";
import type { Msg, NatsError } from "nats";
import path from "path";
import { fileURLToPath } from "url";
import { getGravelConnection, GravelDBs } from "./gravel.js";

// ES module equivalent of __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function test() {
  const gravel = await getGravelConnection({
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
  const gravelMongoClient = await gravel.getDatabaseClient({
    db: GravelDBs.MongoDB,
    mongoUrl: "mongodb://localhost:27017/gravel_db",
  });

  const { initialQuery, changes, stop } = await gravelMongoClient.watchQuery(
    "users",
    {
      email: /keep/,
    },
    {
      sort: {
        debitor: -1,
      },

      projection: {
        _id: 1,
        email: 1,
        address: { street: 1 },
        role: 1,
        debitor: 1,
      },
    },
  );

  // Maintain current state of the data
  console.log("Initial Query:");
  console.log(JSON.stringify(initialQuery, null, 2));
  let currentData = initialQuery;

  // Setup Express server
  const app = express();
  const PORT = 3000;

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

  app.listen(PORT, () => {
    console.log(`Express server running at http://localhost:${PORT}`);
  });

  changes.subscribe((patches) => {
    // Apply JSON patches to the current data
    console.log("Received patches:");
    console.log(JSON.stringify(patches, null, 2));
    const patchResult = patch.applyPatch(
      currentData,
      patches as Operation[],
      false,
      false,
    );
    currentData = patchResult.newDocument;

    console.log("Updated Array after applying patches:");
    console.log(JSON.stringify(currentData, null, 2));
  });

  // Handle Ctrl+C gracefully
  process.on("SIGINT", async () => {
    console.log("\nReceived SIGINT (Ctrl+C). Stopping gracefully...");
    await stop();
    process.exit(0);
  });

  // Keep the process running
  console.log("Watching for changes... Press Ctrl+C to stop.");
}

void test();
