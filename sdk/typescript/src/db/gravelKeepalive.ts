export interface GravelKeepAliveRequest {
  clientID: string;
  keepAliveIntervalMs: number;
}

export interface GravelKeepAliveResponse {
  status: string;
}

export const KEEPALIVE_CHECK_INTERVAL_MS = 1000 * 20;
export const KEEPALIVE_REQUEST_TIMEOUT_MS = 3000;
export const KEEPALIVE_MAX_FAILURES = 3;
export const STALE_RECONNECT_TIMEOUT_MS = 5000;

export function buildKeepAliveRequest(clientID: string): GravelKeepAliveRequest {
  return {
    clientID,
    keepAliveIntervalMs: KEEPALIVE_CHECK_INTERVAL_MS,
  };
}

export function isServerStaleKeepAliveResponse(
  response: GravelKeepAliveResponse,
): boolean {
  return response.status === "stale";
}
