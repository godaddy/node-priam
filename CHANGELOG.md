# Changelog

## 4.1.0

Features:

* Expose more details in the emitted events, like the Cassandra clients (a.k.a. "pools") themselves

## 4.0.0

This major release is geared toward internal performance improvements and removing some legacy code cruft relating to the now-defunct dual driver support.

**BREAKING**:
* Priam now requires node 10.17 or node 12.3 or greater so it can take advantage of async iterator support. For some reason, node 11 is incompatible.
* In olden days, `priam` supported two underlying Cassandra drivers. This is no-longer the case, so some effort has been done to remove the cruft in the code for this dual support. One of these changes removes the translation of the old `helenus` driver config options to those compatible with `cassandra-driver`. Config options in `priam` are now aligned directly with the config options of `cassandra-driver`.
* Errors are now emitted correctly when writing to a stream. This means you may now need to attach `error` handlers to your streams to avoid unhandled error exceptions.
* Undocumented private methods have now been prefixed with underscores; if your code was calling these undocumented methods directly, those methods may now have been renamed or removed.
* Some cases where error would be emitted through streams or callbacks have been changed to throw immediately in the case of caller error. For example, passing a named query that does not exist will throw immediately to the caller.

Features:
* Query methods now all support callbacks, Promises, stream writing, and async iterators options.
* `connect` and `close` methods now support Promises
* All query result options use retry mechanisms; before the stream option did not.

Performance:
* Streaming functionality has been streamlined; native node streams are now used instead of third-party wrappers, and the number of intermediate streams wrappers have been reduced. You may now use the async iterable options to avoid even more node streaming overhead.

See the [migration from 3.x to 4.x](./MIGRATION.md#from-3.x-to-4.x) to assist your efforts into migrating to version 4.x.

## 3.1.0

* Error objects will now return query metadata, including parameters. In production
environments, it is recommended that the parameter values be excluded from logs.

## 3.0.0

Features:
* `localDataCenter` is now a required field in `cassandra-driver` version 4.

## 2.2.0

Features:
* Support a `localDataCenter` config setting to use in the load balancing policy
 
## 2.0.0

Features: 
* Updated `cassandra-driver` to latest version, handle new data type coercion. **Breaking Changes:** 
* Removed `helenus` dependency (deprecated in `1.2.0`).


## 1.2.1
* Added retry module from PR #47.

## 1.2.0 
* Resolved #44 (issue with `USING TIMESTAMP` on individual statementes within batch queries).
* Fixed an issue with subtypes being dropped from collection type hints.
* Downgrade `helenus` library to an optional dependency.

## 1.1.3
* Fix issue with BOM marks inside of named queries.

## 1.1.2
* Update `cassandra-driver` connection error logging.

## 1.1.1
* Add emulated streaming support for `helenus` driver.

## 1.1.0
* Add streaming support with `Query.stream()` or `db.cql()` for `cassandra-driver`.

## 1.0.0
* Fix `keyspace` option when using multiple instances. **This is a breaking change if you relied on the undocumented `instance.pools.default` property being available.**

## 0.9.7
* Attach cql to error objects generated by query execution.

## 0.9.6
* Add support for `routingIndexes` when using `TokenAwarePolicy`.

## 0.9.5
* Add better error handling for `cassandra-driver`.

## 0.9.4
* Strip schema metadata from result sets over binary protocol v1.

## 0.9.3
* Adjust stringify for numeric bigint values.

## 0.9.2
* Fix parameterized queries over binary protocol v1.

## 0.9.1
* Dependency updates.

## 0.9.0
* Removed `node-cassandra-cql` in favor of `cassandra-driver`.

## 0.8.17
* Batch.execute no longer yields an error when the batch is empty.

## 0.8.16
* Simplified result set transformation for `node-cassandra-cql` drivers.

## 0.8.15
* Add isBatch and isQuery methods to base driver.

## 0.8.14
* Fix `resultTransformer` bug when query generates an error.

## 0.8.13
* Fix `Batch.add()` when given empty `Batch` or `Query` objects.

## 0.8.12
* Remove github dependency via `priam-connection-cql` module. 
* Added versioning logic around `cqlVersion` to use the appropriate driver.

## 0.8.11
* Coerce `timestamp` hinted parameters for `node-cassandra-cql` to `Date` objects from `string` or `number`.

## 0.8.10
* `Batch.add()` can now take an `Array` argument.

## 0.8.9
* Fix usage of `Batch.addBatch()` in pre-2.0 Cassandra environments that do not support DML-level timestamps.

## 0.8.8
* Fixed bug where `Query.single()` and `Query.first()` would return empty array instead of null on empty result sets.

## 0.8.7
* Fixed bug which caused boolean values to not be returned when their value is false

## 0.8.6
* Fixed bug which caused resultTransformers to not execute

## 0.8.5
* Changed config to look up consistency level enum if given a string
* Added resultTransformers to drivers and queries-- synchronous functions that are mapped over query results
* Query consistency is set to driver's at instantiation, rather than being looked up at execution if not present
* Added `query` method to base driver, alias for `cql`

## 0.8.4
* Modified `Batch.execute()` to send timestamps as parameters instead of CQL strings.

## 0.8.3
* Added `Query.single()`, `Query.first()`, and `Query.all()` enhancements.

## 0.8.2
* Added generalized `Batch.add()` that can take a `Query` or `Batch` argument.

## 0.8.1
* Added `Batch.addBatch()` enhancements.

## 0.8.0
* Added `Batch.addBatch()`, `Query.params([Array])`, and `driver.connect([Function])`. 
* Updated internal file naming conventions.

## 0.7.6
* Updated to support `insert`/`update` statements on `map<,>` types.

## 0.7.5
* Updated consistency failover strategy. Added `EventEmitter` inheritance.

## 0.7.4
* Add support for `COUNTER` and `UNLOGGED` batch types.

## 0.7.3
* Dependency bump.

## 0.7.1
* Revert back to Promises v1.

## 0.7.0
* Update to latest version of Promises (q.js). 
* Potential breaking change - JSON is no longer auto-deserialized. See the [Executing CQL](#executing-cql) section for more information. Use `object` data types if auto-deserialization is required on specific fields, or use  `deserializeJsonStrings` option to detect JSON as 0.6.x and prior did.

## 0.6.9
* Dependency bump.

## 0.6.8
* Bugfixes.

## 0.6.7
* Added batching support.

## 0.6.6
* Added fluent syntax. 
* Updated example to include setup script.

## 0.6.4
* Added `#param()` helper method for hinted parameters.

## 0.6.3
* Dependency updates, test Travis CI hooks.

## 0.6.2
* Initial Public Release
