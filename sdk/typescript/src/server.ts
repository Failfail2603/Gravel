import { ObjectId } from "mongodb";
import {
  GravelDBs,
  intializeGravel,
  watchQueryToObservable,
} from "./gravel.js";

async function startStuff() {
  console.log("Starting stuff...");

  const gravelClient = await intializeGravel();
  console.log("Gravel client initialized");

  const mongo = await gravelClient.getDatabaseClient({
    db: GravelDBs.MongoDB,
    mongoUrl: "mongodb://localhost:27017/gravel_db",
  });

  const yeet = watchQueryToObservable(
    mongo.watchQuery("users", {
      _id: new ObjectId("699c746df9356c077cf4ecaf"),
    }),
  );

  yeet.subscribe((d) => console.log(JSON.stringify(d, null, 2)));
}

startStuff();
