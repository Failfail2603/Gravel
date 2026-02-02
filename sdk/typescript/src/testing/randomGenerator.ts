import { faker } from "@faker-js/faker";

const DEFAULT_SEED = 123456789;

let __seed = DEFAULT_SEED;
const __glibcMul = 1103515245;
const __glibcInc = 12345;
const __glibcRandMaxPlus1 = 0x80000000; // 2^31

function __glibcNext(seed: number): number {
  return (Math.imul(seed >>> 0, __glibcMul) + __glibcInc) >>> 0;
}

function __glibcRandR(): number {
  let next = __seed >>> 0;
  let result: number;

  next = __glibcNext(next);
  result = (((next / 65536) >>> 0) % 2048) | 0;

  next = __glibcNext(next);
  result = (result << 10) ^ ((((next / 65536) >>> 0) % 1024) | 0);

  next = __glibcNext(next);
  result = (result << 10) ^ ((((next / 65536) >>> 0) % 1024) | 0);

  __seed = next >>> 0;
  return result >>> 0;
}

/**
 * Deterministic Math.random replacement using glibc-style LCG
 */
export function seededRandom(): number {
  return __glibcRandR() / __glibcRandMaxPlus1;
}

/**
 * Reset both Math.random and faker to use the specified seed.
 * This ensures deterministic random generation across experiment runs.
 */
export function resetRandomGenerators(seed: number = DEFAULT_SEED): void {
  // Reset our custom random generator seed
  __seed = seed;

  // Reset faker's seed - this reseeds faker's internal Mersenne Twister
  faker.seed(seed);

  console.log(`Random generators reset with seed: ${seed}`);
}

/**
 * Install the seeded random generator as Math.random
 * Call this once at application startup
 */
export function installSeededMathRandom(): void {
  Math.random = seededRandom;
}

/**
 * Get the current seed value (useful for debugging)
 */
export function getCurrentSeed(): number {
  return __seed;
}

// Auto-install on module load
installSeededMathRandom();

// Also seed faker immediately with default seed for consistent initialization
faker.seed(DEFAULT_SEED);
