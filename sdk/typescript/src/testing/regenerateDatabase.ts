import { ObjectId } from "mongodb";
import type { GravelTestData } from "./config.js";
import {
  generateRandomAddress,
  generateRandomBirthday,
  generateRandomDebitor,
  generateRandomEmail,
  generateRandomRoles,
  generateRandomSepa,
  generateRandomTags,
} from "./dataGenerators.js";
import { getMongoClient } from "./mongoClient.js";
import "./randomGenerator.js";

export async function regenerateDatabase(
  count: number,
  collectionName: string = "users",
): Promise<ObjectId[]> {
  console.log(
    `Regenerating database with ${count} documents in collection "${collectionName}"...`,
  );

  const mongoClient = await getMongoClient();
  const db = mongoClient.db();
  const collection = db.collection<GravelTestData>(collectionName);

  // Clear existing data
  console.log(`Clearing existing data from "${collectionName}"...`);
  await collection.deleteMany({});
  console.log("Collection cleared.");

  // Generate documents
  console.log(`Generating ${count} documents...`);
  const documents: GravelTestData[] = [];

  for (let i = 0; i < count; i++) {
    const document: GravelTestData = {
      _id: new ObjectId(),
      email: generateRandomEmail(),
      roles: generateRandomRoles(),
      address: generateRandomAddress(),
      debitor: generateRandomDebitor(),
      tags: generateRandomTags(),
      sepa: generateRandomSepa(),
    };

    // Optional fields - 50% chance for archived
    if (Math.random() > 0.5) {
      document.archived = Math.random() > 0.5;
    }

    // Optional fields - 70% chance for birthday
    if (Math.random() > 0.3) {
      document.birthday = generateRandomBirthday();
    }

    documents.push(document);

    // Log progress every 10000 documents
    if ((i + 1) % 10000 === 0) {
      console.log(`Generated ${i + 1}/${count} documents...`);
    }
  }

  console.log(`Inserting ${count} documents...`);

  // Insert in batches for better performance
  const batchSize = 1000;
  for (let i = 0; i < documents.length; i += batchSize) {
    const batch = documents.slice(i, i + batchSize);
    await collection.insertMany(batch, { ordered: false });

    if ((i + batchSize) % 10000 === 0 || i + batchSize >= documents.length) {
      console.log(
        `Inserted ${Math.min(i + batchSize, documents.length)}/${count} documents...`,
      );
    }
  }

  console.log(
    `Successfully regenerated "${collectionName}" with ${count} documents!`,
  );

  return documents.slice(0, Math.min(100, documents.length)).map((d) => d._id);
}
