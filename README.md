priam
=====

A simple [Cassandra](http://cassandra.apache.org/) driver for [NodeJS](http://nodejs.org). It wraps the
[helenus](https://github.com/simplereach/helenus) and
[node-cassandra-cql](https://github.com/jorgebay/node-cassandra-cql) drivers with additional error/retry handling, external
`.cql` file support, and connection option resolution from an external source, among other improvements.

By default, the driver uses [node-cassandra-cql](https://github.com/jorgebay/node-cassandra-cql) over binary connection.
If a Thrift connection is desired, simply specify the [helenus](https://github.com/simplereach/helenus) driver option
in config. [Priam](https://github.com/godaddy/priam) uses internal aliases to map
[helenus](https://github.com/simplereach/helenus) and [node-cassandra-cql](https://github.com/jorgebay/node-cassandra-cql)
options to facilitate easily switching between the two drivers.

[Priam](https://github.com/godaddy/priam) is designed to be used as a single instance in order to preserve the
connection pools. As an example, in an [Express](#) application, [priam](https://github.com/godaddy/priam) should be
initialized at server startup and attached to the `request` object so that your controllers can access it.

[Priam](https://github.com/godaddy/priam) is actively developed and used by
[Go Daddy Website Builder](http://www.godaddy.com/hosting/website-builder.aspx?ci=87223) to provide a high-availability
and high-performance hosting platform based on the [Cassandra](http://cassandra.apache.org/) database.

Example Usage
-------------

Check the `example` folder for a more complete example.

### Using Known Connection Information ###
```javascript
var path = require("path");
var db = require("priam")({
        config: {
                cqlVersion: "3.0.0", /* optional, defaults to "3.0.0" */
                timeout: 4000, /* optional, defaults to 4000 */
                poolSize: 2, /* optional, defaults to 1 */
                consistencyLevel: 1, /* optional, defaults to 1. Should be a valid db.consistencyLevel value */
                driver: "helenus", /* optional, defaults to "node-cassandra-cql" */,
                numRetries: 3, /* optional, defaults to 0. Retries occur on connection failures. */
                retryDelay: 100, /* optional, defaults to 100ms. Used on error retry or consistency fallback retry */
                enableConsistencyFailover: true, /* optional, defaults to true */
                queryDirectory: path.join(__dirname, "path/to/your/cql/files"), /* optional, required to use #namedQuery() */
                user: "<your_username>",
                password: "<your_password>",
                keyspace: "<your_keyspace>", /* Default keyspace. Can be overwritten via options passed into #cql(), etc. */
                hosts: [ /* Ports are optional */
                    "123.456.789.010:9042",
                    "123.456.789.011:9042",
                    "123.456.789.012:9042",
                    "123.456.789.013:9042"
                ]
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

`dataParams` will be normalized as necessary in order to be passed to Cassandra. In addition to primitives
(`Number`/`String`), the driver supports JSON objects, Array and Buffer types. `Object` and `Array` types will be
stringified prior to being sent to [Cassandra](http://cassandra.apache.org/), whereas `Buffer` types will be encoded.

The `executeAsPrepared` option informs the [node-cassandra-cql](https://github.com/jorgebay/node-cassandra-cql) driver
to execute the given CQL as a prepared statement, which will boost performance if the query is executed multiple times.
*This option is currently ignored if using the [helenus](https://github.com/simplereach/helenus) driver.*

The `queryName` option allows metrics to be captured for the given query, assuming a `metrics` object was passed into
the constructor. See the [Monitoring / Instrumentation](#monitoring--instrumentation) section for more information.

The `consistency` option allows you to override any default consistency level that was specified in the constructor.

The `keyspace` option allows you to specify another keyspace to execute a query against. This will override the default
keyspace set in the connection information.

The `suppressDebugLog` option allows you to disable debug logging of CQL for an individual query. This is useful for
queries that may contain sensitive data that you do not wish to show up in debug logs.

When using the [node-cassandra-cql](https://github.com/jorgebay/node-cassandra-cql) driver,
[hinted parameters](https://github.com/jorgebay/node-cassandra-cql/wiki/Data-types#wiki-providing-hint) are supported.
Instead of the driver inferring the data type, it can be explicitly specified by using a
[specially formatted object](https://github.com/jorgebay/node-cassandra-cql/wiki/Data-types#wiki-providing-hint).
Similar to consistencies, data types are exposed via the `<instance>.dataType` object.
*Other than type `uuid`, parameter hints will be ignored when using the [helenus](https://github.com/simplereach/helenus)
driver.*

#### Example ####
```javascript
var db = require("priam")({
        config: { /* ... options ... */ }
});
db.cql(
        'SELECT "myCol1", "myCol2" FROM "myColumnFamily" WHERE "keyCol1" = ? AND "keyCol2" = ?',
        ["value_of_keyCol1", { value: "value_of_keyCol2", hint: db.dataType.ascii }],
        { consistency: db.consistencyLevel.one, queryName: "myQuery", executeAsPrepared: true },
        function (err, data) {
                if (err) {
                        console.log("ERROR: " + err);
                        return;
                }
                console.log("Returned data: " + data);
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
var db = require("priam")({
        config: { queryDirectory: path.join(__dirName, "cql") }
}); /* 'cql' folder will be scanned and all .cql files loaded into memory synchronously */
db.namedQuery(
        "myColumnFamilySelect", /* name of .cql file with contents: 'SELECT "myCol1", "myCol2" FROM "myColumnFamily" WHERE "keyCol1" = ? AND "keyCol2" = ?' */
        ["value_of_keyCol1", "value_of_keyCol2"],
        { consistency: db.consistencyLevel.ONE },
        function (err, data) {
                if (err) {
                        console.log("ERROR: " + err);
                        return;
                }
                console.log("Returned data: " + data);
        }
);
```

#### Helper Functions ####
The driver also provides the following functions that wrap `#cql()`. They should be used in place of `#cql()` where
possible, when not using named queries, as it will allow you to both use default consistency levels for different types
of queries, and easily find references in your application to each query type.
- `select`: calls `#cql()` with `db.consistencyLevel.one`
- `insert`: calls `#cql()` with `db.consistencyLevel.localQuorum`
- `update`: calls `#cql()` with `db.consistencyLevel.localQuorum`
- `delete`: calls `#cql()` with `db.consistencyLevel.localQuorum`

### Error Retries ###
The driver will automatically retry on network-related errors. In addition, other errors will be retried in the following
conditions:
- `db.consistencyLevel.all` will be retried at `db.consistencyLevel.quorum`
- `db.consistencyLevel.quorum` will be retried at `db.consistencyLevel.localQuorum`

The following retry options are supported in the driver constructor:
- `enableConsistencyFailover`: Optional. Defaults to `true`. If `false`, the failover described above will not take place.
- `numRetries`: Optional. Defaults to 0 (no retry). The number of retries to execute on network failure. *Note:
                this will also affect the number of retries executed during consistency level fallback. For example,
                if `numRetries` is 2 and a CQL query with `db.consistencyLevel.all` is submitted, it will be executed
                3 times at `db.consistencyLevel.all`, 3 times at `db.constistencyLevel.quorum` and 3 times at
                `db.consistencyLevel.localQuorum` before yielding an error back to the caller.
- `retryDelay`: Optional. Defaults to 100.

### Logging ###
The driver supports passing a [winston](https://github.com/flatiron/winston) logger inside of the options.

```javascript
require("priam")({
        config: { /* connection information */ },
        logger: new (require("winston")).Logger({ /* logger options */ })
});
```
Debug logging of CQL can be turned off for an individual query by passing the `suppressDebugLog = true` option in the
query options dictionary. This is useful for queries that may contain sensitive data that you do not wish to show up
in debug logs.

### Monitoring / Instrumentation ###
Instrumentation is supported via an optional `metrics` object passed into the driver constructor. The `metrics` object
should have a method `#measurement(queryName [string], duration [number], unit [string])`.

```javascript
var logger = new (require("winston")).Logger({ /* logger options */ })
var metrics = new MetricsClient();
require("priam")({
        config: { /* connection information */ },
        logger: logger,
        metrics: metrics
});
```

### Using a Connection Resolver ###

If you are required to pull database credentials from a credential store (e.g. to support database failover or expiring
old credentials), you should use the `connectionResolver` or `connectionResolverPath` options.

If used, the supplied connection resolver will be called before *every* CQL query is issued. It is up to the supplied
connection resolver to follow whatever caching strategies are required for the environment.

The `connectionResolver` option allows you to pass in a `connectionResolver` object that has already been constructed.

```javascript
var resolver = new MyConnectionResolver();
var db = require("priam")({
        config: {
                /* ... other connection options ... */
                connectionResolver: resolver
        }
});
```

The `connectionResolverPath` will be loaded via a `#require()` call. *Note: it is required from `/lib/drivers/`, so
it is recommended to supply a resolver this way via a node module, as paths to source files should be relative to the
internal path.*

```javascript
var db = require("priam")({
        config: {
                /* ... other connection options ... */
                connectionResolverPath: "myResolverModule"
        }
});
```

#### Sample Connection Resolver Implementation ####

The supplied connection resolver should have the following method: `#resolveConnection(config, callback)`.

`config` will be the [priam](https://github.com/godaddy/priam) configuration that was supplied to the constructor, so
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
var db = require("priam")({
        config: {
                /* ... other connection options, including Nimitz ... */
                connectionResolverPortMap = {
                    from: "9160",
                    to: "9042"
                }
        }
});
```

Release Notes
-------------
 - `0.6.2`: Initial Public Release
