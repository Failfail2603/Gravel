import { ObjectId } from "mongodb";
import {
  GravelDBs,
  initializeGravel,
  watchQueryToObservable,
} from "./Gravel.js";

async function startStuff() {
  console.log("Starting stuff...");

  const gravelClient = await initializeGravel();
  console.log("Gravel client initialized");

  const mongo = await gravelClient.getDatabaseClient({
    db: GravelDBs.MongoDB,
    mongoUrl: "mongodb://mongo:27017/gravel_db",
  });

  const yeet = watchQueryToObservable(
    mongo.watchQuery(
      "users",
      {
        _id: {
          $in: [new ObjectId("69d10869af6d3755d212dc60")],
        },
      },
      {
        projection: {
          _id: 1,
          email: 1,
          address: 1,
        },
        explainGravel: true,
      },
    ),
  );

  yeet.subscribe((d: unknown) => console.log(JSON.stringify(d, null, 2)));
}

startStuff();
