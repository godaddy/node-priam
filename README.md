[![Build Status](https://travis-ci.org/godaddy/node-priam.png?branch=master)](https://travis-ci.org/godaddy/node-priam)
[![Coverage Status](https://coveralls.io/repos/godaddy/node-priam/badge.png?branch=master)](https://coveralls.io/r/godaddy/node-priam)
[![Dependency Status](https://gemnasium.com/godaddy/node-priam.png?branch=master)](https://gemnasium.com/godaddy/node-priam)
[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/godaddy/node-priam/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

[![NPM](https://nodei.co/npm/priam.png?downloads=true&downloadRank=true&stars=true)](https://www.npmjs.org/package/priam)

priam
=====

A simple [Cassandra](http://cassandra.apache.org/) driver for [NodeJS](http://nodejs.org). It wraps the
[cassandra-driver](https://github.com/datastax/nodejs-driver) modules with additional error/retry handling, external
`.cql` file support, connection option resolution from an external source, and query composition, among other improvements.

The driver uses [cassandra-driver](https://github.com/datastax/nodejs-driver) over a binary-protocol connection.
If a Thrift connection is desired, please use [the latest 1.X release](https://github.com/godaddy/node-priam/releases/tag/1.2.1)
and simply specify the [helenus](https://github.com/simplereach/helenus) driver option in config.

[Priam](https://github.com/godaddy/node-priam) is designed to be used as a single instance in order to preserve the
connection pools. As an example, in an [Express](http://expressjs.com/) application,
[priam](https://github.com/godaddy/node-priam) should be initialized at server startup and attached to the `request`
object so that your controllers can access it.

[Priam](https://github.com/godaddy/node-priam) is actively developed and used by
[Go Daddy Website Builder](http://www.godaddy.com/hosting/website-builder.aspx?ci=87223) to provide a high-availability
and high-performance hosting platform based on the [Cassandra](http://cassandra.apache.org/) database.

Example Usage
-------------

Check the `example` folder for a more complete example. Start by running: `npm start` followed by `curl http://localhost:8080/` or `curl http://localhost:8080/stream=true`.

### Using Known Connection Information ###
```javascript
var path = require('path');
var db = require('priam')({
  config: {
    cqlVersion: '3.0.0', /* optional, defaults to '3.1.0' */
    timeout: 4000, /* optional, defaults to 4000 */
    poolSize: 2, /* optional, defaults to 1 */
    consistencyLevel: 'one', /* optional, defaults to one. Will throw if not a valid Cassandra consistency level*/
    numRetries: 3, /* optional, defaults to 0. Retries occur on connection failures. Deprecated, use retryOptions instead. */
    retryDelay: 100, /* optional, defaults to 100ms. Used on consistency fallback retry */
    retryOptions: { retries: 0 }, /* optional. See https://www.npmjs.com/package/retry for options */
    enableConsistencyFailover: true, /* optional, defaults to true */
    coerceDataStaxTypes: false, /* optional, defaults to true */
    queryDirectory: path.join(__dirname, 'path/to/your/cql/files'), /* optional, required to use #namedQuery() */
    user: '<your_username>',
    password: '<your_password>',
    keyspace: '<your_keyspace>', /* Default keyspace. Can be overwritten via options passed into #cql(), etc. */
    hosts: [ /* Ports are optional */
      '123.456.789.010:9042',
      '123.456.789.011:9042',
      '123.456.789.012:9042',
      '123.456.789.013:9042'
    ],
    localDataCenter: 'some_dc' /* optional; used for selecting preferred nodes during load balancing */ 
  }
});
```

### Executing CQL ###
The driver provides the `#cql()` method for executing CQL statements against [Cassandra](http://cassandra.apache.org/).
It provides the following arguments:

 - `cql`: The CQL statement to execute. Parameters should be replaced with `?` characters

 - `dataParams`: The parameters array. Should match the order of `?` characters in the `cql` parameter

 - `options`: Optional. Additional options for the CQL call. Supports `consistency`, `queryName`, `keyspace`,
   and `executeAsPrepared`.

 - `callback(err, data)`: Optional. The callback for the CQL call. Provides `err` and `data` arguments. `data` will be
   an `Array`.

 - `stream`: Optional. A [`Stream`](https://github.com/rvagg/through2) object to be written to when reading from
   Cassandra. If this is provided, `callback` should not be provided. *Note: Only available when using the `datastax`
   driver!*

`dataParams` will be normalized as necessary in order to be passed to Cassandra. In addition to primitives
(`Number`/`String`), the driver supports JSON objects, Array and Buffer types. `Object` and `Array` types will be
stringified prior to being sent to [Cassandra](http://cassandra.apache.org/), whereas `Buffer` types will be encoded.

The `executeAsPrepared` option informs the [node-cassandra-cql](https://github.com/jorgebay/node-cassandra-cql) driver
to execute the given CQL as a prepared statement, which will boost performance if the query is executed multiple times.

The `queryName` option allows metrics to be captured for the given query, assuming a `metrics` object was passed into
the constructor. See the [Monitoring / Instrumentation](#monitoring--instrumentation) section for more information.

The `consistency` option allows you to override any default consistency level that was specified in the constructor.

The `resultHint` option allows you to control how objects being returned by the underlying provider are treated. For
example, a data type of `objectAscii` will result in `JSON.parse()` being called on the resulting value. Special data types
of `objectAscii` and `objectText` are available for this purpose. If these data types are used in a parameter's `hint`
field, they will be automatically mapped to the corresponding data type (e.g. `ascii` or `text) prior to executing the
cql statement.

The `deserializeJsonStrings` option informs [Priam](https://github.com/godaddy/node-priam) to inspect any string results
coming back from the driver and call `JSON.parse()` before returning the value back to you. This works similar to providing
`resultHint` options for specific columns, but instead it applies to the entire set of columns.
*This was the default behavior prior to the 0.7.0 release.*

The `coerceDataStaxTypes` option informs [Priam](https://github.com/godaddy/node-priam) to convert any of the custom
[DataStax data types](http://docs.datastax.com/en/developer/nodejs-driver/2.1/nodejs-driver/reference/nodejs2Cql3Datatypes.html)
to standard JavaScript types (`string`, `number`). This is recommended if you are upgrading from a previous version of
[Priam](https://github.com/godaddy/node-priam) and need to keep backwards-compatibility in your codebase with the previous
versin of the [DataStax cassandra-driver module](https://github.com/datastax/nodejs-driver).

The `keyspace` option allows you to specify another keyspace to execute a query against. This will override the default
keyspace set in the connection information.

The `suppressDebugLog` option allows you to disable debug logging of CQL for an individual query. This is useful for
queries that may contain sensitive data that you do not wish to show up in debug logs.

[hinted parameters](http://www.datastax.com/drivers/nodejs/1.0/types.js.html#line28) are supported.
Instead of the driver inferring the data type, it can be explicitly specified by using a
[specially formatted object](http://www.datastax.com/documentation/developer/nodejs-driver/1.0/nodejs-driver/reference/nodejs2Cql3Datatypes.html).
Similar to consistencies, data types are exposed via the `<instance>.dataType` object.

There is also a `param(value [object], type [string])` helper method for creating hinted parameters, as shown below:

#### Example ####
```javascript
var db = require('priam')({
    config: { /* ... options ... */ }
});
db.cql(
  'SELECT "myCol1", "myCol2" FROM "myColumnFamily" WHERE "keyCol1" = ? AND "keyCol2" = ?',
  [db.param('value_of_keyCol1', 'ascii'), db.param('value_of_keyCol2', 'ascii')],
  { consistency: db.consistencyLevel.one, queryName: 'myQuery', executeAsPrepared: true },
  function (err, data) {
    if (err) {
      console.log('ERROR: ' + err);
      return;
    }
    console.log('Returned data: ' + data);
  }
);
```

### Named Queries ###
The driver supports using named queries. These queries should be `.cql` files residing in a single folder. The query
name corresponds to the file name preceding the `.cql` extension. For example, file `myObjectSelect.cql` would have a
query name of `myObjectSelect`.

In order to use named queries, the optional `queryDirectory` option should be passed into the driver constructor.

Queries are loaded *synchronously* and cached when the driver is constructed.

Named queries will automatically provide the `queryName` and `executeAsPrepared` options when executing the CQL,
though the caller can override these options by providing them in the `options` object.

#### Example ####
```javascript
var db = require('priam')({
    config: { queryDirectory: path.join(__dirName, 'cql') }
}); /* 'cql' folder will be scanned and all .cql files loaded into memory synchronously */
db.namedQuery(
  'myColumnFamilySelect', /* name of .cql file with contents: 'SELECT "myCol1", "myCol2" FROM "myColumnFamily" WHERE "keyCol1" = ? AND "keyCol2" = ?' */
  ['value_of_keyCol1', 'value_of_keyCol2'],
  { consistency: db.consistencyLevel.ONE },
  function (err, data) {
    if (err) {
      console.log('ERROR: ' + err);
      return;
    }
    console.log('Returned data: ' + data);
  }
);
```

### Fluent Syntax ###
The driver provides a fluent syntax that can be used to construct queries.

Calling `#beginQuery()` returns a `Query` object with the following chainable functions:

 - `#query(cql [string])`: Sets the cql for the query to execute.

 - `#namedQuery(queryName [string])`: Specifies the named query for the query to execute.

 - `#param(value [object], hint [optional, string], isRoutingKey [optional, bool])`: Adds a parameter to the query. *Note: They are applied in the order added*.

 If `isRoutingKey` is provided and true, the given parameter will be used to determine the coordinator node to execute a query against, when using the `datastax` driver and a prepared statement.

 - `#params(parameters [Array])`: Adds the array of parameters to the query. Parameters should be created using `db.param()`

 - `#options(optionsDictionary [object])`: Extends the query options. See
    [Executing CQL](https://github.com/godaddy/node-priam/blob/master/README.md#executing-cql) for valid options.

 - `#consistency(consistencyLevelName [string])`: Sets consistency level for the query. Alias for `#options({ consistency: db.consistencyLevel[consistencyLevelName] })`.

 - `#all()`: Default functionality. After calling execute will return array of any results found.

 - `#first()`: After calling execute will return first, if any of the results found.

 - `#single()`: Similar to first, will return first result however will yield an error if more than one record found.

 - `#execute(callback [optional, function])`: Executes the query. If a callback is not supplied, this will return a Promise.

 - `#stream()`: Executes the query and returns a read-only [`Stream`](https://github.com/rvagg/through2) object. *Note: Only available when using the `datastax` driver!*



#### Fluent Syntax Examples ####

```javascript
db
  .beginQuery()
  .query('SELECT "myCol1", "myCol2" FROM "myColumnFamily" WHERE "keyCol1" = ? AND "keyCol2" = ?')
  .param('value_of_keyCol1', 'ascii')
  .param('value_of_keyCol2', 'ascii')
  .consistency("one")
  .options({ queryName: 'myColumnFamilySelect' })
  .options({ executeAsPrepared: true })
  .execute(function (err, data) {
    if (err) {
      console.log('ERROR: ' + err);
      return;
    }
    console.log('Returned data: ' + data);
  });
```

Similarly, fluent syntax can be used for named queries.
```javascript
db
  .beginQuery()
  .namedQuery('myColumnFamilySelect') /* name of .cql file with contents: 'SELECT "myCol1", "myCol2" FROM "myColumnFamily" WHERE "keyCol1" = ? AND "keyCol2" = ?' */
  .param('value_of_keyCol1', 'ascii')
  .param('value_of_keyCol2', 'ascii')
  .consistency('one')
  .execute(function (err, data) {
    if (err) {
      console.log('ERROR: ' + err);
      return;
    }
    console.log('Returned data: ' + data);
  });
```

The fluent syntax also supports promises, if a callback is not supplied to the `#execute()` function.
```javascript
db
  .beginQuery()
  .namedQuery('myColumnFamilySelect') /* name of .cql file with contents: 'SELECT "myCol1", "myCol2" FROM "myColumnFamily" WHERE "keyCol1" = ? AND "keyCol2" = ?' */
  .param('value_of_keyCol1', 'ascii')
  .param('value_of_keyCol2', 'ascii')
  .consistency('one')
  .execute()
  .fail(function (err) {
    console.error('ERROR: ' + err);
  })
  .done(function (data) {
    console.log('Returned data: ' + JSON.stringify(data));
  });
```

For expected large data sets in a web application, it is a good idea to stream the data back.
```javascript
var through = require('through2');
/* ... */
function (req, res, next) {
  db
    .beginQuery()
    .query('SELECT "myCol1", "myCol2" FROM "myColumnFamily" limit 1000000')
    .consistency("one")
    .options({ queryName: 'myColumnFamilySelect' })
    .options({ executeAsPrepared: true })
    .stream()
    .on('error', function (err) {
      res.statusCode(500).end(err.message);
    })
    .pipe(through.obj(function(data, enc, next) {
      next(null, JSON.stringify(data)+'\n');
    }))
    .pipe(res);
}
```

### Batching Queries ###
Queries can be batched by using the fluent syntax to create a batch of queries. Standard CQL and named queries can be
combined. If a consistency level for the batch is not supplied, the strictest consistency from the batched queries will
be applied, if given. Similarly, if debug logs for one of the batched queries are suppressed, debug logs for the entire
batch will be suppressed. `queryName` and `executeAsPrepared` for individual queries will be ignored.

*Note: Batching prepared statements is currently not supported. If prepared statements are used in a batch, the entire
CQL query will be sent over the wire.*

**IMPORTANT:** Batching is only supported with `INSERT`, `UPDATE` and `DELETE` commands. If `SELECT` statements are
added, the query will yield a runtime error.

For more information on batching, see the
[CQL 3.0 reference](http://www.datastax.com/documentation/cql/3.0/cql/cql_reference/batch_r.html).

If using [CQL 3.1](http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/batch_r.html), batches can be nested
and timestamps will be applied at the query level instead of the batch level. If using
[CQL 3.0](http://www.datastax.com/documentation/cql/3.0/cql/cql_reference/batch_r.html), only timestamps at the outermost
batch level will be applied. Any others will be ignored.

Calling `#beginBatch()` returns a `Query` object with the following chainable functions:

 - `#addQuery(query [Query])`: Adds a query to the batch to execute. The query should be created by `db.beginQuery()`.

 - `#addBatch(batch [Batch])`: Adds the queries contained within the `batch` parameter to the current batch.. The batch
    should be created by `db.beginBatch()`.

 - `#add(batchOrQuery [Batch or Query])`: Allows `null`, `Query`, or `Batch` objects. See `#addQuery()` and `#addBatch()`
    above.

 - `#options(optionsDictionary [object])`: Extends the batch. See
    [Executing CQL](https://github.com/godaddy/node-priam/blob/master/README.md#executing-cql) for valid options.

 - `#timestamp(clientTimestamp [optional, long])`: Specifies that `USING TIMESTAMP <value>` will be sent as part of the
    batch CQL. If `clientTimestamp` is not specified, the current time will be used.

 - `#type(batchTypeName [string])`: Specifies the type of batch that will be used. Available types are `'standard'`,
    `'counter'` and `'unlogged'`. Defaults to `'standard'`. See
    [CQL 3.1 reference](http://www.datastax.com/documentation/cql/3.0/cql/cql_reference/batch_r.html) for more details
    on batch types.

 - `#consistency(consistencyLevelName [string])`: Sets consistency level for the batch. Alias for
   `#options({ consistency: db.consistencyLevel[consistencyLevelName] })`.

 - `#execute(callback [optional, function])`: Executes the query. If a callback is not supplied, this will return a Promise.

#### Batch Syntax Example ####

```javascript
db
  .beginBatch()
  .add(db.beginQuery(
    .query('UPDATE "myColumnFamily" SET "myCol1" = ?, "myCol2" = ? WHERE "keyCol1" = ? AND "keyCol2" = ?')
    .param('value_of_myCol1', 'ascii')
    .param('value_of_myCol2', 'ascii')
    .param('value_of_keyCol1', 'ascii')
    .param('value_of_keyCol2', 'ascii')
  )
  .add(db.beginQuery(
    .query('UPDATE "myOtherColumnFamily" SET "myCol" = ? WHERE "keyCol" = ?')
    .param('value_of_myCol', 'ascii')
    .param('value_of_keyCol', 'ascii')
  )
  .consistency('quorum')
  .type('counter')
  .timestamp()
  .execute()
  .fail(function (err) {
    console.log('ERROR: ' + err);
  })
  .done(function (data) {
    console.log('Returned data: ' + data);
  });
```

### Helper Functions ###
The driver also provides the following functions that wrap `#cql()`. They should be used in place of `#cql()` where
possible, when not using named queries, as it will allow you to both use default consistency levels for different types
of queries, and easily find references in your application to each query type.

- `select`: calls `#cql()` with `db.consistencyLevel.one`

- `insert`: calls `#cql()` with `db.consistencyLevel.localQuorum`

- `update`: calls `#cql()` with `db.consistencyLevel.localQuorum`

- `delete`: calls `#cql()` with `db.consistencyLevel.localQuorum`

### Connection Management ###
Connection pools are automatically instantiated when the first query is run and kept alive for the lifetime of the driver.
To manually initiate and/or close connections, you can use the following functions:

 - `#connect(keyspace [string, optional], callback [Function])`: Calls `callback` parameter after connection pool is initialized,
   or existing pool is retrieved. Can be used at application startup to immediately start the connection pool.

 - `#close(callback [Function])`: Calls `callback` after all connection pools are closed. Useful for testing purposes.

### Error Retries ###
The driver will automatically retry on network-related errors. In addition, other errors will be retried in the following
conditions:

- `db.consistencyLevel.all` will be retried at `db.consistencyLevel.eachQuorum`

- `db.consistencyLevel.quorum` and `db.consistencyLevel.eachQuorum` will be retried at `db.consistencyLevel.localQuorum`

The following retry options are supported in the driver constructor:

- `enableConsistencyFailover`: Optional. Defaults to `true`. If `false`, the failover described above will not take place.

- `numRetries`: (Deprecated) Optional. Defaults to 0 (no retry). The number of retries to execute on network failure.

- `retryOptions`: Optional. Defaults to `{ retries: 0 }` (no retries). Retry logic in the event that a network failure happens. See the [retry](https://www.npmjs.com/package/retry) package for options. *Note:
                this will also affect the number of retries executed during consistency level fallback. For example,
                if the number of retries is 2 and a CQL query with `db.consistencyLevel.all` is submitted, it will be executed
                3 times at `db.consistencyLevel.all`, 3 times at `db.constistencyLevel.eachQuorum` and 3 times at
                `db.consistencyLevel.localQuorum` before yielding an error back to the caller.

- `retryDelay`: Optional. Defaults to 100. The number of milliseconds used for consistency level fallback.

### Logging ###
The driver supports passing a [winston](https://github.com/flatiron/winston) logger inside of the options.

```javascript
require('priam')({
  config: { /* connection information */ },
  logger: new (require('winston')).Logger({ /* logger options */ })
});
```
Debug logging of CQL can be turned off for an individual query by passing the `suppressDebugLog = true` option in the
query options dictionary. This is useful for queries that may contain sensitive data that you do not wish to show up
in debug logs.

### Monitoring / Instrumentation ###
Instrumentation is supported via an optional `metrics` object passed into the driver constructor. The `metrics` object
should have a method `#measurement(queryName [string], duration [number], unit [string])`.

```javascript
var logger = new (require('winston')).Logger({ /* logger options */ })
var metrics = new MetricsClient();
require('priam')({
  config: { /* connection information */ },
  logger: logger,
  metrics: metrics
});
```

### Events ###
Each `priam` instance is an `EventEmitter`. The following events are emitted:

<table>
<thead>
<tr><th>Event</th><th>Params</th><th>Description</th></tr>
</thead>
<tbody>
<tr>
<td>connectionRequested</td>
<td>connectionRequestId</td>
<td>A pooled connection has been requested. Expect a `connectionAvailable` event when this request has been fulfilled.</td>
</tr>
<tr>
<td>connectionResolving</td>
<td>connectionResolutionId</td>
<td>If a connection resolver is being used, emitted when the connection resolver is about to be invoked</td>
</tr>
<tr>
<td>connectionResolved</td>
<td>connectionResolutionId</td>
<td>If a connection resolver is being used, emitted when the connection resolver succeeds.</td>
</tr>
<tr>
<td>connectionResolvedError</td>
<td>connectionResolutionId, err</td>
<td>If a connection resolver is being used, emitted when the connection resolver fails.</td>
</tr>
<tr>
<td>connectionOpening</td>
<td>connectionOpenRequestId</td>
<td>Emitted when opening a new connection.</td>
</tr>
<tr>
<td>connectionOpened</td>
<td>connectionOpenRequestId</td>
<td>Emitted when the opening of a new connection has succeeded.</td>
</tr>
<tr>
<td>connectionFailed</td>
<td>connectionOpenRequestId, err</td>
<td>Emitted when the opening of a new connection has failed.</td>
</tr>
<tr>
<td>connectionAvailable</td>
<td>connectionRequestId</td>
<td>Emitted if a new or existing connection is ready to be used to execute a query.</td>
</tr>
<tr>
<td>connectionLogged</td>
<td>logLevel, message, details</td>
<td>Emitted when the underlying driver outputs log events.</td>
</tr>
<tr>
<td>connectionClosed</td>
<td></td>
<td>Emitted when a connection closes.</td>
</tr>
<tr>
<td>queryStarted</td>
<td>requestId</td>
<td>Emitted when a query is about to be executed.</td>
</tr>
<tr>
<td>queryRetried</td>
<td>requestId</td>
<td>Emitted when a query is about to be retried.</td>
</tr>
<tr>
<td>queryCompleted</td>
<td>requestId</td>
<td>Emitted when a query has succeeded.</td>
</tr>
<tr>
<td>queryFailed</td>
<td>requestId, err</td>
<td>Emitted when a query has failed after exhausting any retries.</td>
</tr>
</tbody>
</table>


### Using a Connection Resolver ###

If you are required to pull database credentials from a credential store (e.g. to support database failover or expiring
old credentials), you should use the `connectionResolver` or `connectionResolverPath` options.

If used, the supplied connection resolver will be called before *every* CQL query is issued. It is up to the supplied
connection resolver to follow whatever caching strategies are required for the environment.

The `connectionResolver` option allows you to pass in a `connectionResolver` object that has already been constructed.

```javascript
var resolver = new MyConnectionResolver();
var db = require('priam')({
  config: {
    /* ... connection options ... */
  },
  connectionResolver: resolver
});
```

The `connectionResolverPath` will be loaded via a `#require()` call. *Note: it is required from `/lib/drivers/`, so
it is recommended to supply a resolver this way via a node module, as paths to source files should be relative to the
internal path.*

```javascript
var db = require('priam')({
  config: {
    /* ... other connection options ... */
    connectionResolverPath: 'myResolverModule'
  }
});
```

#### Sample Connection Resolver Implementation ####

The supplied connection resolver should have the following method: `#resolveConnection(config, callback)`.

`config` will be the [priam](https://github.com/godaddy/node-priam) configuration that was supplied to the constructor, so
connection resolvers can access any custom configuration information specified when the driver was initialized.

`callback(error, connectionInformation)` should be called with the results from the connection resolver. If `error` is
supplied, an error log message will be sent to the supplied logger. `connectionInformation` should always be supplied
if known. If it is not supplied when an error is thrown, the error will be supplied to the `#cql()` callback.

`connectionInformation` should contain the following properties: `user`, `password`, `hosts`.

See example application for a concrete example using a connection resolver.

#### Port Mapping Options ####
If your `connectionResolver` connection information includes port, and is returning the port for a protocol you do not
wish to use, (e.g. You want to use binary port 9042, but resolver is returning Thrift port 9160), you can use the
`connectionResolverPortMap` option to perform the mapping.

```javascript
var db = require('priam')({
  config: {
    /* ... other connection options, including Nimitz ... */
    connectionResolverPortMap = {
      from: '9160',
      to: '9042'
    }
  }
});
```

Release Notes
-------------
 - `2.0.0`: Updated `cassandra-driver` to latest version, handle new data type coercion. **Breaking Change:** Removed `helenus` dependency (deprecated in `1.2.0`).
 - `1.2.1`: Added retry module from PR #47.
 - `1.2.0`: Resolved #44 (issue with `USING TIMESTAMP` on individual statementes within batch queries).
            Fixed an issue with subtypes being dropped from collection type hints.
            Downgrade `helenus` library to an optional dependency.
 - `1.1.3`: Fix issue with BOM marks inside of named queries.
 - `1.1.2`: Update `cassandra-driver` connection error logging.
 - `1.1.1`: Add emulated streaming support for `helenus` driver.
 - `1.1.0`: Add streaming support with `Query.stream()` or `db.cql()` for `cassandra-driver`.
 - `1.0.0`: Fix `keyspace` option when using multiple instances. **This is a breaking change if you relied on the undocumented `instance.pools.default` property being available.**
 - `0.9.7`: Attach cql to error objects generated by query execution.
 - `0.9.6`: Add support for `routingIndexes` when using `TokenAwarePolicy`.
 - `0.9.5`: Add better error handling for `cassandra-driver`.
 - `0.9.4`: Strip schema metadata from result sets over binary protocol v1.
 - `0.9.3`: Adjust stringify for numeric bigint values.
 - `0.9.2`: Fix parameterized queries over binary protocol v1.
 - `0.9.1`: Dependency updates.
 - `0.9.0`: Removed `node-cassandra-cql` in favor of `cassandra-driver`.
 - `0.8.17`: Batch.execute no longer yields an error when the batch is empty.
 - `0.8.16`: Simplified result set transformation for `node-cassandra-cql` drivers.
 - `0.8.15`: Add isBatch and isQuery methods to base driver.
 - `0.8.14`: Fix `resultTransformer` bug when query generates an error.
 - `0.8.13`: Fix `Batch.add()` when given empty `Batch` or `Query` objects.
 - `0.8.12`: Remove github dependency via `priam-connection-cql` module. Added versioning logic around `cqlVersion` to use the appropriate driver.
 - `0.8.11`: Coerce `timestamp` hinted parameters for `node-cassandra-cql` to `Date` objects from `string` or `number`.
 - `0.8.10`: `Batch.add()` can now take an `Array` argument.
 - `0.8.9`: Fix usage of `Batch.addBatch()` in pre-2.0 Cassandra environments that do not support DML-level timestamps.
 - `0.8.8`: Fixed bug where `Query.single()` and `Query.first()` would return empty array instead of null on empty result sets.
 - `0.8.7`: Fixed bug which caused boolean values to not be returned when their value is false
 - `0.8.6`: Fixed bug which caused resultTransformers to not execute
 - `0.8.5`: Changed config to look up consistency level enum if given a string
            Added resultTransformers to drivers and queries-- synchronous functions that are mapped over query results
            Query consistency is set to driver's at instantiation, rather than being looked up at execution if not present
            Added `query` method to base driver, alias for `cql`
 - `0.8.4`: Modified `Batch.execute()` to send timestamps as parameters instead of CQL strings.
 - `0.8.3`: Added `Query.single()`, `Query.first()`, and `Query.all()` enhancements.
 - `0.8.2`: Added generalized `Batch.add()` that can take a `Query` or `Batch` argument.
 - `0.8.1`: Added `Batch.addBatch()` enhancements.
 - `0.8.0`: Added `Batch.addBatch()`, `Query.params([Array])`, and `driver.connect([Function])`. Updated internal file naming conventions.
 - `0.7.6`: Updated to support `insert`/`update` statements on `map<,>` types.
 - `0.7.5`: Updated consistency failover strategy. Added `EventEmitter` inheritance.
 - `0.7.4`: Add support for `COUNTER` and `UNLOGGED` batch types.
 - `0.7.3`: Dependency bump.
 - `0.7.1`: Revert back to Promises v1.
 - `0.7.0`: Update to latest version of Promises (q.js). Potential breaking change - JSON is no longer auto-deserialized.
            See the [Executing CQL](#executing-cql) section for more information. Use `object` data types if
            auto-deserialization is required on specific fields, or use `deserializeJsonStrings` option to detect JSON
            as 0.6.x and prior did.
 - `0.6.9`: Dependency bump.
 - `0.6.8`: Bugfixes.
 - `0.6.7`: Added batching support.
 - `0.6.6`: Added fluent syntax. Updated example to include setup script.
 - `0.6.4`: Added `#param()` helper method for hinted parameters.
 - `0.6.3`: Dependency updates, test Travis CI hooks.
 - `0.6.2`: Initial Public Release
