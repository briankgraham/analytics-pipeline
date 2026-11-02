// What every service follows: the clients for outside systems, like Elasticsearch and Kafka, and the
// programs that start them, like the API server. There is one instance of each, from a static
// getInstance, and its constructor is private so nothing can make another.
export interface Service {
  // Safe to call any number of times: it never makes a second connection or repeats its work.
  init(): Promise<void>;
  // Safe to call any number of times. A closed instance can't start again; getInstance gives a new one.
  close(): Promise<void>;
}
