import type { ObjectId } from "mongodb";
import type { GravelMongoWatchQueryFindOptions } from "../db/mongo.js";

export const PORT = 3000;
export const MONGO_URL = "mongodb://localhost:27017/gravel_db";
export const USE_BULK_OPERATIONS = true;
export const collectionSize = 500000;

export const overrideUpdateNumber: number | null = 0;
export const overrideDeleteNumber: number | null = 0;
export const overrideInsertNumber: number | null = 10;

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

export const query: any = {
  roles: { $elemMatch: { role: "user" } },
  debitor: { $gte: 500000 },
};

export const options: GravelMongoWatchQueryFindOptions = {
  sort: {
    debitor: 1,
    birthday: -1,
    _id: -1,
  },
  skip: 50000,
  limit: 1000,
  projection: {
    _id: 1,
    email: 1,
    roles: { role: 1, contexts: 1 },
    address: { street: 1 },
    debitor: 1,
    birthday: 1,
    tags: 1,
  },
};
