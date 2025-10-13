import { applyPatch, type Operation } from "fast-json-patch";
import type { Msg, NatsError } from "nats";
import { getGravelConnection, GravelDBs } from "./gravel";

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
    {},
    {
      skip: 0,
      limit: 4,
      projection: { email: 1, address: { street: 1 } },
    },
  );

  // Maintain current state of the data
  console.log("Initial Query:");
  console.log(JSON.stringify(initialQuery, null, 2));
  let currentData = initialQuery;

  changes.subscribe((patches) => {
    // Apply JSON patches to the current data
    console.log("Received patches:");
    console.log(JSON.stringify(patches, null, 2));
    const patchResult = applyPatch(
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
