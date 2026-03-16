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
    mongo.watchQuery(
      "users",
      {
        email: "Selmer_Bode-Gerlach@hotmail.com",
      },
      {
        projection: {
          _id: 1,
          email: 1,
        },
      },
    ),
  );

  yeet.subscribe((d) => console.log(JSON.stringify(d, null, 2)));
}

startStuff();
