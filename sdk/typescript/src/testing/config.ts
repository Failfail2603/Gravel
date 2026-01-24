import { Collection, ObjectId } from "mongodb";
import type { GravelMongoWatchQueryFindOptions } from "../db/mongo.js";

export const PORT = 3000;
export const MONGO_URL = "mongodb://localhost:27017/gravel_db";
export const USE_BULK_OPERATIONS = true;

// Experiment configuration
export const UPDATES_PER_QUERY = 20;
export const REPETITIONS_PER_QUERY = 3;
export const DEFAULT_EXPERIMENT_SEED = 123456789;
export const DEFAULT_COLLECTION_SIZE = 50000;

// Custom bulk operation generator type
export interface BulkOperationStats {
  updates: number;
  deletes: number;
  inserts: number;
  replaces: number;
}

export type CustomBulkOperationGenerator = (
  collection: Collection<GravelTestData>,
  totalUsers: number,
) => Promise<{
  bulkOps: any[];
  stats: BulkOperationStats;
}>;

export const overrideUpdateNumber: number | null = null;
export const overrideDeleteNumber: number | null = null;
export const overrideInsertNumber: number | null = null;
export const overrideReplaceNumber: number | null = null;

export interface GravelTestData {
  _id: ObjectId;
  email: string;
  archived?: boolean;
  roles: { startedAt: Date; role: string; contexts: string[] }[];
  birthday?: Date;
  address: { street: string; zip: string; city: string };
  debitor: number; // int
  tags: string[][];
  sepa: {
    name: string;
    startDate?: Date;
    endDate?: Date;
    price: number; // float
    bankAccount: {
      iban: string;
      bic: string;
      openedAt?: Date;
      active: boolean;
    };
  }[];
}

export const experimentQueries: {
  name: string;
  query: Record<string, any>;
  options: GravelMongoWatchQueryFindOptions;
}[] = [
  // simple subscribe by _id
  // {
  //   name: "single_document_by_id",
  //   query: { _id: new ObjectId("693aa602358f323fc2b27129") },
  //   options: {
  //     sort: {
  //       _id: 1,
  //     },
  //   },
  // },
  // {
  //   name: "editors_first_page",
  //   query: {
  //     roles: { $elemMatch: { role: "editor" } },
  //     debitor: { $lte: 500000 },
  //   },
  //   options: {
  //     sort: {
  //       debitor: 1,
  //       birthday: 1,
  //       _id: 1,
  //     },
  //     skip: 0,
  //     limit: 20,
  //     projection: {
  //       _id: 1,
  //       email: 1,
  //       archived: 1,
  //       address: { street: 1, city: 1 },
  //       debitor: 1,
  //       birthday: 1,
  //     },
  //   },
  // },
  {
    name: "test",
    query: {
      roles: { $elemMatch: { role: "yeet" } },
    },
    options: {
      sort: {
        debitor: 1,
        birthday: 1,
        _id: 1,
      },
      skip: 2,
      limit: 20,
      projection: {
        _id: 1,
        email: 1,
        archived: 1,
        address: { street: 1, city: 1 },
        debitor: 1,
        birthday: 1,
      },
    },
  },
  {
    name: "editors_deep_pagination",
    query: {
      roles: { $elemMatch: { role: "editor" } },
      debitor: { $lte: 500000 },
    },
    options: {
      sort: {
        debitor: 1,
        birthday: 1,
        _id: 1,
      },
      skip: 30000,
      limit: 1000,
      projection: {
        _id: 1,
        email: 1,
        archived: 1,
        address: { street: 1, city: 1 },
        debitor: 1,
        birthday: 1,
      },
    },
  },
];

export const query = experimentQueries[0].query;

export const options = experimentQueries[0].options;

/**
 * Example: Replace 2 non-matching documents with matching ones above the window
 * Uncomment and assign to customBulkOperationGenerator to use
 */
export const twoNonMatchingToMatching: CustomBulkOperationGenerator = async (
  collection,
  totalUsers,
) => {
  const {
    generateRandomEmail,
    generateRandomAddress,
    generateRandomTags,
    generateRandomSepa,
  } = await import("./dataGenerators.js");

  const bulkOps: any[] = [];
  const stats: BulkOperationStats = {
    updates: 0,
    deletes: 0,
    inserts: 0,
    replaces: 0,
  };

  // Find 2 non-matching documents
  const nonMatchingUsers = await collection
    .find({
      $or: [
        { roles: { $not: { $elemMatch: { role: "editor" } } } },
        { debitor: { $gt: 500000 } },
      ],
    })
    .limit(1)
    .toArray();

  // Replace them with matching documents that sort above the window
  for (const user of nonMatchingUsers) {
    const replacementDocument: GravelTestData = {
      _id: user._id,
      email: generateRandomEmail(),
      roles: [
        {
          role: "editor", // Match the query
          startedAt: new Date(2020, 0, 1),
          contexts: ["test"],
        },
      ],
      address: generateRandomAddress(),
      debitor: 279506, // Match the query (debitor <= 500000)
      tags: generateRandomTags(),
      sepa: generateRandomSepa(),
      birthday: new Date(1970, 0, 1), // Sort above the window
    };

    bulkOps.push({
      replaceOne: {
        filter: { _id: user._id },
        replacement: replacementDocument,
      },
    });
    stats.replaces++;
  }

  return { bulkOps, stats };
};

/**
 * Custom bulk operation generator
 * Set to null to use default random operations
 * Set to a function to provide your own bulk operations
 */
export const customBulkOperationGenerator: CustomBulkOperationGenerator | null =
  null;
