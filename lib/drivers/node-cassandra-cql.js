"use strict";

var
    cql = require("node-cassandra-cql"),
    util = require("util"),
    BaseDriver = require("./basedriver"),
    retryErrors = ["DriverError", "PoolConnectionError", "ECONNRESET", "ENOTFOUND", "ECONNREFUSED"];

function NodeCassandraDriver() {
    BaseDriver.call(this);
    var consistencies = cql.types.consistencies;
    this.consistencyLevel = {
        ONE: consistencies.one,
        one: consistencies.one,
        TWO: consistencies.two,
        two: consistencies.two,
        THREE: consistencies.three,
        three: consistencies.three,
        QUORUM: consistencies.quorum,
        quorum: consistencies.quorum,
        LOCAL_QUORUM: consistencies.localQuorum,
        localQuorum: consistencies.localQuorum,
        EACH_QUORUM: consistencies.eachQuorum,
        eachQuorum: consistencies.eachQuorum,
        ALL: consistencies.all,
        all: consistencies.all,
        ANY: consistencies.any,
        any: consistencies.any
    };
    this.dataType = cql.types.dataTypes;
}
util.inherits(NodeCassandraDriver, BaseDriver);

exports = module.exports = function (context) {
    var driver = new NodeCassandraDriver();
    driver.init(context);
    return driver;
};
module.exports.NodeCassandraDriver = NodeCassandraDriver;

NodeCassandraDriver.prototype.initProviderOptions = function init(config) {
    setHelenusOptions(config);
    config.supportsPreparedStatements = true;
    config.version = config.version || "3.0.0";
};

NodeCassandraDriver.prototype.createConnectionPool = function createConnectionPool(poolConfig, callback) {
    var self = this;

    self.logger.debug("priam.Driver: Creating new pool", {
        poolConfig: {
            keyspace: poolConfig.keyspace,
            hosts: poolConfig.hosts
        }
    });

    var pool = new cql.Client(poolConfig);
    pool.storeConfig = poolConfig;
    pool.waiters = [];
    pool.isReady = false;
    pool.on('log', function (level, message, data)  {
        if (level === "debug" || level === "info") { return; } // these are trace logs that are too verbose for even our debug logging
        if (level === "error") { level = "warn"; } // true errors will yield error on execution
        var metaData = null;

        /* istanbul ignore else: no benefit of testing when there is no metadata */
        if (data) {
            metaData = { data: data };
        }

        self.logger[level]("priam.Driver: " + message, metaData);
    });
    pool.connect(function (err) {
        if (err) {
            self.logger.error("priam.Driver: Pool Connect Error",
                { name: err.name, code: err.code, error: err.message, stack: err.stack });
            self.callWaiters(pool, err);
            return void self.closePool(pool);
        }
        pool.isReady = true;
        self.callWaiters(pool, null);
    });
    callback(pool);
};

NodeCassandraDriver.prototype.remapConnectionOptions = function remapConnectionOptions(connectionData) {
    remapOption(connectionData, "user", "username");
};

NodeCassandraDriver.prototype.closePool = function closePool(pool, callback) {
    pool.isClosed = true;
    pool.shutdown(callback);
};

NodeCassandraDriver.prototype.executeCqlOnDriver = function executeCqlOnDriver(pool, cqlStatement, params, consistency, options, callback) {
    var method = options.executeAsPrepared ? "executeAsPrepared" : "execute";
    pool[method](cqlStatement, params, consistency, function (err, data) {
        if (err) { return void callback(err); }
        var result = (data && data.rows) ? data.rows : [];
        return void callback(null, result);
    });
};

NodeCassandraDriver.prototype.canRetryError = function canRetryError(err) {
    return err && (retryErrors.indexOf(err.name) >= 0 || retryErrors.indexOf(err.code) >= 0);
};

NodeCassandraDriver.prototype.getNormalizedResults = function getNormalizedResults(original) {
    var i, k, v, col, row, result, results;

    results = new Array(original.length);
    for (i = 0; i < original.length; i++) {
        row = original[i];
        result = {};
        for (k = 0; k < row.columns.length; k++) {
            col = row.columns[k];
            /* istanbul ignore if: guard statement for something that should never happen */
            if (!col.name) { continue; }
            v = row[col.name];

            /* istanbul ignore else: not easily tested and no real benefit to doing so */
            if (v) {
                if (typeof v === "string" && v.length > 0 && (v[0] === "{" || v[0] === "[" || v === "null")) {
                    // attempt to auto-deserialize
                    try {
                        v = JSON.parse(v);
                    } catch (ex) {
                        // ignore
                    }
                }
                result[col.name] = v;
            }
        }
        results[i] = result;
    }

    return results;
};

NodeCassandraDriver.prototype.dataToCql = function dataToCql(val) {
    if (val && val.hasOwnProperty("value") && val.hasOwnProperty("hint")) {
        return val; // {value,hint} style parameter -- node-cassandra-cql will handle this internally, do not stringify
    }

    if (!Buffer.isBuffer(val) && (util.isArray(val) || typeof val === "object")) {
        // arrays and objects should be JSON'ized
        return JSON.stringify(val);
    }

    return val; // use as-is
};

function remapOption(config, from, to) {
    if (config.hasOwnProperty(from)) {
        config[to] = config[from];
        delete config[from];
    }
}

function setHelenusOptions(config) {
    remapOption(config, "timeout", "getAConnectionTimeout");
    remapOption(config, "hostPoolSize", "poolSize");
    remapOption(config, "cqlVersion", "version");
    remapOption(config, "user", "username");
}
