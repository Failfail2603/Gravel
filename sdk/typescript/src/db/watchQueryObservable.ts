import type { Operation } from "fast-json-patch";
import jsonPatch from "fast-json-patch";
import { Observable, Subscription } from "rxjs";

const { applyPatch } = jsonPatch;

export interface WatchQueryResult<T extends Record<string, any>> {
  initialQuery: { result: Array<T> };
  changes: Observable<unknown>;
  stop: () => Promise<void>;
}

interface WatchQueryChangeEnvelope {
  type: "full" | "patch";
  result: unknown;
}

function isJsonPatchArray(value: unknown): value is Operation[] {
  return (
    Array.isArray(value) &&
    value.every(
      (entry) =>
        entry !== null &&
        typeof entry === "object" &&
        "op" in entry &&
        "path" in entry,
    )
  );
}

function isWatchQueryChangeEnvelope(
  value: unknown,
): value is WatchQueryChangeEnvelope {
  return (
    value !== null &&
    typeof value === "object" &&
    "type" in value &&
    "result" in value
  );
}

function hasFromField(patch: Operation): patch is Operation & { from: string } {
  return "from" in patch && typeof patch.from === "string";
}

function normalizeGravelPatchPath(path: string): string {
  if (path === "/result") {
    return "";
  }

  if (path.startsWith("/result/")) {
    return path.slice("/result".length);
  }

  return path;
}

function normalizeGravelPatchOperations(patches: Operation[]): Operation[] {
  return patches.map((patch) => {
    const normalizedPatch = {
      ...patch,
      path: normalizeGravelPatchPath(patch.path),
      ...(hasFromField(patch)
        ? { from: normalizeGravelPatchPath(patch.from) }
        : {}),
    };

    return normalizedPatch as Operation;
  });
}

export function watchQueryToObservable<T extends Record<string, any>>(
  watchQueryPromise: Promise<WatchQueryResult<T>>,
): Observable<Array<T>> {
  return new Observable<Array<T>>((observer) => {
    let currentData: Array<T> = [];
    let stopped = false;

    let subscription: Subscription | undefined;
    let resolvedWatchQuery: WatchQueryResult<T> | undefined;

    void watchQueryPromise
      .then((watchQuery) => {
        resolvedWatchQuery = watchQuery;

        if (stopped) {
          void watchQuery.stop();
          return;
        }

        currentData = structuredClone(watchQuery.initialQuery.result);
        observer.next(currentData);

        subscription = watchQuery.changes.subscribe({
          next: (change) => {
            try {
              if (isWatchQueryChangeEnvelope(change)) {
                if (change.type === "full") {
                  currentData = structuredClone(change.result as Array<T>);
                  observer.next(currentData);
                  return;
                }

                if (isJsonPatchArray(change.result)) {
                  currentData = applyPatch(
                    structuredClone(currentData),
                    normalizeGravelPatchOperations(change.result),
                    false,
                    false,
                  ).newDocument as Array<T>;
                  observer.next(currentData);
                  return;
                }

                throw new Error("Unsupported watch query patch payload");
              }

              if (isJsonPatchArray(change)) {
                currentData = applyPatch(
                  structuredClone(currentData),
                  normalizeGravelPatchOperations(change),
                  false,
                  false,
                ).newDocument as Array<T>;
              } else if (Array.isArray(change)) {
                currentData = structuredClone(change as Array<T>);
              } else {
                throw new Error("Unsupported watch query change payload");
              }

              observer.next(currentData);
            } catch (error) {
              observer.error(error);
            }
          },
          error: (error) => {
            observer.error(error);
          },
          complete: () => {
            observer.complete();
          },
        });
      })
      .catch((error) => {
        observer.error(error);
      });

    return () => {
      subscription?.unsubscribe();
      if (!stopped) {
        stopped = true;
        if (resolvedWatchQuery) {
          void resolvedWatchQuery.stop();
        }
      }
    };
  });
}
