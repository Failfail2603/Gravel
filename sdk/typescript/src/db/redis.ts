import type { NatsConnection } from "nats";
import { GravelClient } from "./gravelClient";

export interface GravelRedisOptions {
  redisUrl: string;
}
export interface GravelRedisClient extends GravelClient {}

export async function generateRedisProvider(
  natsConnection: NatsConnection,
  options?: GravelRedisOptions,
): Promise<GravelRedisClient> {
  return {
    clientID: "",
    dbProviderID: "",
  };
}
