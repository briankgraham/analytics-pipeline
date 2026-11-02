// What every service follows, like Elasticsearch: a class that main.ts starts when the program boots
// and closes when it shuts down. There is one instance of each, from a static getInstance, and its
// constructor is private so nothing can make another.
export interface Service {
  // Safe to call any number of times: it never makes a second connection or repeats its work.
  init(): Promise<void>;
  // Safe to call any number of times. A closed instance can't start again; getInstance gives a new one.
  close(): Promise<void>;
}
