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
    mongoUrl: "mongodb://localhost:27017/gravel_db",
  });

  const yeet = watchQueryToObservable(
    mongo.watchQuery(
      "users",
      {
        _id: {
          $in: [
            new ObjectId("699c7469f9356c077cf4ecae"),
            new ObjectId("699c7467f9356c077cf4ecad"),
          ],
        },
      },
      {
        projection: {
          _id: 1,
          email: 1,
          address: 1,
        },
      },
    ),
  );

  yeet.subscribe((d: unknown) => console.log(JSON.stringify(d, null, 2)));
}

startStuff();
