import type { NatsConnection } from "nats";

export interface GravelRedisOptions {
  redisUrl: string;
}
export interface GravelRedisClient {}

export async function generateRedisProvider(
  natsConnection: NatsConnection,
  clientID: string,
  options?: GravelRedisOptions,
): Promise<GravelRedisClient> {
  return {};
}
