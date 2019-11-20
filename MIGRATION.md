# Version Migration

## From 3.x to 4.x

### Node Requirements

This version of `priam` requires node 10 or above (but not version 11 which has a strange compatibility break).

### Config Setting Updates

Valid config options have now changed to be more [directly aligned with `cassandra-driver`](https://docs.datastax.com/en/developer/nodejs-driver/4.3/api/type.ClientOptions/). The following legacy config settings must be changed:

| Old Name(s) | New Name | Format Changes |
|----------|----------|----------------|
| `timeout`, `getAConnectionTimeout` | `socketOptions.connectTimeout` | None |
| `hostPoolSize`, `poolSize` | `pooling.coreConnectionsPerHost` | To replicate the legacy behavior, set `pooling.coreConnectionsPerHost` to `{ local: poolSize, remote: Math.ceil(poolSize / 2) }` |
| `cqlVersion`, `version` | `protocolOptions.maxVersion` | None |

### Stream Error Handling

If you're passing a stream to be written to for query methods or using the `.stream()` method of queries, you must add an `.on('error', ...)` handler; otherwise, your node process may be terminated due to an unhandled error.
