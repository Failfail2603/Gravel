import type { Msg, NatsError } from "nats";
import { getGravelConnection, GravelDBs } from "./gravel";

async function test() {
  const gravel = await getGravelConnection({
    debugChannelCallback: (err: NatsError | null, msg: Msg) => {
      console.log("Gravel Debug: ", msg.data.toString());
      console.log("Gravel Error: ", err);
    },
  });
  const gravelMongoClient = await gravel.getDatabaseClient({
    db: GravelDBs.MongoDB,
    mongoUrl: "mongodb://localhost:27017/gravel_db",
  });

  const { initialQuery, changes, stop } = await gravelMongoClient.watchQuery(
    "users",
    { role: "user" },
    {
      skip: 0,
      limit: 10,
      projection: { email: 1, address: { street: 1 } },
    },
  );

  console.log(initialQuery);

  changes.subscribe((change) => {
    console.log(change);
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
