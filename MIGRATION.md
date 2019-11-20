# Version Migration

## From 3.x to 4.x

### Node Requirements

This version of `priam` requires node 10 or above (but not version 11 which has a strange compatibility break).

### Config Setting Updates

Valid config options have now changed to be more directly aligned with `cassandra-driver`. The following legacy config settings must be changed as follows:

| Old Name | New Name | Format Changes |
|----------|----------|----------------|
| `timeout` | `socketOptions.connectTimeout` | None |
| `getAConnectionTimeout` | `socketOptions.connectTimeout` | None |

### Stream Error Handling

If you're passing a stream to be written to for query methods or using the `.stream()` method of queries, you must add an `.on('error', ...)` handler; otherwise, your node process may be terminated due to an unhandled error.
