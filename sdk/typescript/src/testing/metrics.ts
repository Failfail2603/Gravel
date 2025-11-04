export let databaseUpdates = 0;
export let gravelUpdates = 0;
export let oldWatchQueryUpdates = 0;

export let gravelBytes = 0;
export let oldWatchQueryBytes = 0;

export function resetMetrics() {
  databaseUpdates = 0;
  gravelUpdates = 0;
  oldWatchQueryUpdates = 0;
  gravelBytes = 0;
  oldWatchQueryBytes = 0;
}

export function addDataBaseUpdates(count: number) {
  databaseUpdates += count;
}

export function addGravelUpdates(count: number, bytes: number) {
  gravelUpdates += count;
  gravelBytes += bytes;
}

export function addOldWatchQueryUpdates(count: number, bytes: number) {
  oldWatchQueryUpdates += count;
  oldWatchQueryBytes += bytes;
}
