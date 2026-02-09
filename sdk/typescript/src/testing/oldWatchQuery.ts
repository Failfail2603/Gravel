import mingo from "mingo";
import {
  type ChangeStream,
  type ChangeStreamDocument,
  type Document,
  type FindOptions,
} from "mongodb";
import { type Observable, Subject, concat, from, fromEvent } from "rxjs";
import {
  concatMap,
  filter,
  finalize,
  map,
  startWith,
  switchMap,
  throttleTime,
} from "rxjs/operators";
import { getMongoClient, mongoClient } from "./mongoClient.js";

/**
 * Symbol emitted when a change event is received but filtered out as irrelevant (noop).
 * This allows subscribers to distinguish between "no change" and "change was ignored".
 */
export const NOOP_CHANGE = Symbol("NOOP_CHANGE");
export type NoopChange = typeof NOOP_CHANGE;

let counter = 0;

export function getCurrentWatchQueryCount() {
  return counter;
}

let dbQueryCounter = 0;

export function getAndResetDbQueryCount(): number {
  const count = dbQueryCounter;
  dbQueryCounter = 0;
  return count;
}

export let mongoChangeStream: ChangeStream | null = null;

const changeStreamSubjects = new Map<
  string,
  Subject<ChangeStreamDocument<any>>
>();
// const changeStream = new Subject<ChangeStreamDocument<any>>();
const changeStreamSubscription = from(getMongoClient())
  .pipe(
    switchMap((client) =>
      fromEvent<ChangeStreamDocument<any>>(
        (mongoChangeStream = client.db().watch(
          [
            // make sure fullDocument exists
            {
              $match: {
                $and: [
                  // exclude any collection that starts with "tmp.". These are for example temporarly created by { $out: "" } stages
                  { "ns.coll": { $not: /^tmp\./ } },

                  // exclude sessions collection because runInContext potentially modifies it and causes an infinite loop.
                  // We should not care about session updates anyways
                  { "ns.coll": { $ne: "sessions" } },
                ],
                $or: [
                  {
                    fullDocument: {
                      $exists: true,
                      $not: { $type: "array" },
                      $type: "object",
                    },
                  },
                  { operationType: "delete" },
                ],
              },
            },
          ],
          { fullDocument: "updateLookup" },
        )),
        "change",
      ),
    ),
  )
  .subscribe({
    next(value) {
      for (const [sessionID, subject] of changeStreamSubjects.entries()) {
        subject.next(value);
        // need to push new values to subject in corresponding context in order to maintain initial context of called watchQuery
      }
    },
    complete() {
      for (const [sessionID, subject] of changeStreamSubjects.entries()) {
        subject.complete();
        // need to push new values to subject in corresponding context in order to maintain initial context of called watchQuery
      }
    },
    error(err) {
      for (const [sessionID, subject] of changeStreamSubjects.entries()) {
        subject.error(err);
        // need to push new values to subject in corresponding context in order to maintain initial context of called watchQuery
      }
    },
  });

function getChangeStreamSubject() {
  const sessionID = "hallo";
  let subject = changeStreamSubjects.get(sessionID);
  if (!subject) {
    subject = new Subject<ChangeStreamDocument<any>>();
    changeStreamSubjects.set(sessionID, subject);
  }

  return subject;
}

export function getCollectionChangeStream(
  collectionName: string,
): Observable<ChangeStreamDocument<any>> {
  const subject = getChangeStreamSubject();
  return subject.pipe(
    filter(
      (v) =>
        // @ts-expect-error if ns is in type does not affecht collection check
        v.ns?.coll === collectionName,
    ),
  );
}

export function watchEntireCollection(
  collectionName: string,
): Observable<void> {
  return getCollectionChangeStream(collectionName).pipe(
    startWith(null),
    map(() => {}),
    throttleTime(500, undefined, { leading: true, trailing: true }),
  );
}

export function watchQuery<T extends Document>(
  collectionName: string,
  query: Record<string, any>,
  options: FindOptions<any> = {},
): Observable<T[] | NoopChange> {
  let lastIDs: string[] = [];

  const subject = getChangeStreamSubject();
  counter++;

  return concat(
    Promise.resolve(mongoClient!.db().collection(collectionName))
      .then((collection) => collection.find<T>(query, options).toArray())
      .then((initialData) => {
        initialData.forEach((d) => {
          d._id = d._id.toString();
        });
        lastIDs = initialData.map((entry) => entry._id) as string[];
        if (lastIDs.length > 100)
          console.error(
            { collection: collectionName, lengthOfData: lastIDs.length },
            "Watched data in watchQuery is longer than 100. This could to heavy performance degradations.",
          );

        return initialData;
      }),
    subject.pipe(
      concatMap(async (doc) => {
        if (
          await checkIfChangeIsRelevant(
            doc,
            collectionName,
            lastIDs,
            query,
            options,
          )
        ) {
          dbQueryCounter++;
          const freshData = await mongoClient!
            .db()
            .collection(collectionName)
            .find(query, options)
            .toArray();
          freshData.forEach((d) => {
            d._id = d._id.toString();
          });
          lastIDs = freshData.map((r) => r._id.toString());

          return freshData as unknown as T[];
        } else {
          // Change was filtered out as irrelevant - return noop symbol
          return NOOP_CHANGE;
        }
      }),
      // throttleTime removed for testing - was causing delayed emissions and stale latency
    ),
  ).pipe(
    finalize(() => {
      counter--;
    }),
  );
}

const checkIfChangeIsRelevant = async <T extends { _id: string }>(
  doc: ChangeStreamDocument<T>,
  collectionName: string,
  lastIDs: string[],
  query: Record<string, any>,
  options: FindOptions<any>,
) => {
  if (
    doc.operationType !== "insert" &&
    doc.operationType !== "update" &&
    doc.operationType !== "replace" &&
    doc.operationType !== "delete"
  )
    return false;
  if (doc.ns.coll !== collectionName) return false;

  // want to always update subscription for "deletion" because we cannot determine if deleted doc may influence our current selection (because of sort or skip/limit,...)
  if (doc.operationType === "delete") return true;

  const result = match(query, doc, lastIDs);
  if (!result) return false;

  // this should always be the last check to avoid unnecessary db calls
  dbQueryCounter++;
  const ids = (
    await mongoClient!
      .db()
      .collection(collectionName)
      .find(query, { ...options, projection: { _id: 1 } })
      .toArray()
  ).map((x) => x._id);

  // convert ObjectId to string
  const idsMapped = ids.map((x) => x.toString());

  return (
    // check if changed document is included in our current query
    idsMapped.includes(doc.documentKey._id.toString()) ||
    // check if skip/limit window moved because of update/insert/replace before window
    new Set(lastIDs.concat(idsMapped)).size !== lastIDs.length ||
    // check if window shrinks or grows because of query (update/insert/replace)
    lastIDs.length !== idsMapped.length
  );
};

const match = (
  query: Record<string, any>,
  doc: ChangeStreamDocument<any>,
  lastIDs: string[],
) => {
  switch (doc.operationType) {
    case "insert":
    case "update":
    case "replace":
      return (
        lastIDs.includes((doc.documentKey as any)._id.toString()) ||
        mingo.find([doc.fullDocument], query).count()
      );
    case "delete":
      return lastIDs.includes((doc.documentKey as any)._id.toString());
    case "drop":
    case "dropDatabase":
    case "invalidate":
    case "rename":
    default:
      return false;
  }
};

/**
 * Wrapper class to provide reactivity to Subscriptions using MongoDBs `ChangeStream`s.
 */
export class CollectionHandle<T extends Document = any> {
  constructor(private readonly collectionName: string) {}

  /**
   * @deprecated use standalone watchQuery function exported from `@adornis/baseql/server/watchQuery`
   */
  public watchQuery(query: Record<string, any>, options: FindOptions<T> = {}) {
    return watchQuery<T>(this.collectionName, query, options);
  }
}

export function getMongoChangeStream() {
  if (!mongoChangeStream)
    throw new Error("Cannot return changeStream! It is not connected yet.");

  return mongoChangeStream;
}

/**
 * Stops the old watch query system by closing the change stream and cleaning up all resources.
 */
export async function stopOldWatchQuery() {
  console.log("Stopping old watch query system...");

  // Complete all active subjects
  for (const [sessionID, subject] of changeStreamSubjects.entries()) {
    subject.complete();
  }
  changeStreamSubjects.clear();

  // Unsubscribe from the change stream subscription
  if (changeStreamSubscription && !changeStreamSubscription.closed) {
    changeStreamSubscription.unsubscribe();
  }

  // Close the MongoDB change stream
  if (mongoChangeStream) {
    await mongoChangeStream.close();
    mongoChangeStream = null;
  }

  console.log("Old watch query system stopped successfully.");
}
