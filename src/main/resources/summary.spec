{
  "db": <String>,
  "coll": <String>,
  "chunk": {
      "min": <Bson>,
      "max": <Bson>
  },                        // bounds of shard chunk or partition
  "status": <String>,       // one of [SUCCEEDED, FAILED, RETRYING, RUNNING, UNSTARTED]
  "numRetries": <Int>,
  "matches": <Long>,
  "mismatches": [<Bson>],   // List of _ids
  "srcOnly": [<Bson>],      // List of _ids
  "destOnly": [<Bson>],     // List of _ids
  "bytesProcessed": <Long>,
  "timestamp": <Date>,
  "history": [{
       "timestamp": <Date>,
       "status": <String>,   // one of [SUCCESS/FAILURE]
       "numRetries": <Int>,
       "matches": <Long>,
       "mismatches": [<Bson>],
       "srcOnly": [<Bson>],
       "destOnly": [<Bson>]
    }, ... ]
}