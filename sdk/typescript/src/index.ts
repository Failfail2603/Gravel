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

  const { initialQuery, changes } = await gravelMongoClient.watchQuery(
    "users",
    { role: "user" },
  );

  console.log(initialQuery);

  changes.subscribe((change) => {
    console.log(change);
  });
}

void test();
