import { type Msg, type NatsError } from "nats";

export interface GravelClient {
  // the unique client id for gravel. Consist of a uuid to differentiate a container instance of the client. This will be unique in the server environment to multiplex between different clients
  clientID: string;

  // this id is a unique key which should be unique to the project itself. This is used to multiplex between different database providers on the same client. Can be something like "mongodb + url" or "redis + url".
  // we can then hold a singleton to the gravel database withz the same url
  // ! this is not the same as the clientID as this can be the same for multiple containers hosting the same projects in a horizontal setup
  dbProviderID: string;

  // debug callback
  debugCallback?: (err: NatsError | null, msg: Msg) => void;
}
