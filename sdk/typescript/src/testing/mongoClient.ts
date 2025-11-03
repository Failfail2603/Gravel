import { MongoClient } from "mongodb";
import { MONGO_URL } from "./config.js";

let mongoClient: MongoClient | null = null;

export async function getMongoClient(): Promise<MongoClient> {
  if (!mongoClient) {
    mongoClient = new MongoClient(MONGO_URL, {
      directConnection: true,
      replicaSet: "rs0",
    });
    await mongoClient.connect();
    console.log("MongoDB client connected");
  }
  return mongoClient;
}

export async function closeMongoClient(): Promise<void> {
  if (mongoClient) {
    await mongoClient.close();
    mongoClient = null;
    console.log("MongoDB client closed");
  }
}
