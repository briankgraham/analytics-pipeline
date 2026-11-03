// What a search can ask for, in terms that don't depend on any one database. Nothing in this folder
// imports from outside it or knows what is being searched, so it could become its own library.
//
// The caller names its own fields, which keeps each kind of criterion on the kind of field it works
// on. The wrong pairing raises no error in Elasticsearch: it just finds nothing or the wrong things.
// `WordField` is a field split into words, and `ExactField` is one kept as a single exact value.
export type Criterion<WordField extends string = string, ExactField extends string = string> =
  // The field holds every one of these words, in any order.
  | { type: 'words'; field: WordField; words: string }
  // The field holds exactly this value.
  | { type: 'exact'; field: ExactField; value: string };
