import type { ObjectId } from "mongodb";
import type { GravelMongoWatchQueryFindOptions } from "../db/mongo.js";

export const PORT = 3000;
export const MONGO_URL = "mongodb://localhost:27017/gravel_db";
export const USE_BULK_OPERATIONS = true;

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
  $and: [
    {
      debitor: {
        $gt: 200000,
      },
    },
    {
      debitor: {
        $lt: 800000,
      },
    },
  ],
};

export const options: GravelMongoWatchQueryFindOptions = {
  sort: {
    debitor: -1,
    _id: -1,
  },
  skip: 70000,
  limit: 2000,
  projection: {
    _id: 1,
    email: 1,
    roles: { role: 1, contexts: 1 },
    address: { street: 1 },
    debitor: 1,
    tags: 1,
    sepa: {
      name: 1,
      startDate: 1,
      endDate: 1,
      price: 1,
      bankAccount: { iban: 1, bic: 1, openedAt: 1, active: 1 },
    },
  },
};
