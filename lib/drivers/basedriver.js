"use strict";

var
    helenus = require("helenus"),
    util = require("util"),
    async = require("async"),
    Query = require("./../util/query"),
    QueryCache = require("./../util/queryCache"),
    fakeLogger = require("./../util/fakeLogger"),
    _ = require("lodash");

function BaseDriver() {
    this.consistencyLevel = {};
    this.dataType = {};
}

module.exports = BaseDriver;

BaseDriver.prototype.initProviderOptions = function initProviderOptions(config) {
};

BaseDriver.prototype.init = function init(context) {
    if (!context) {
        throw new Error("missing context object");
    }
    if (!context.config) {
        throw new Error("missing context.config object");
    }

    /* istanbul ignore next: no benefit of testing coalesce */
    this.config = context.config || {};
    this.logger = context.logger || fakeLogger;
    this.metrics = context.metrics;
    if (context.config.queryDirectory) {
        this.queryCache = new QueryCache({ queryDirectory: context.config.queryDirectory });
    }

    this.initProviderOptions(this.config);
    this.poolConfig = {
        consistencyLevel: this.consistencyLevel.one
    };
    this.poolConfig = _.extend(this.poolConfig, this.config);

    this.connectionResolver = null;
    if (context.connectionResolver) {
        this.connectionResolver = context.connectionResolver;
    }
    else if (this.config.connectionResolverPath) {
        this.connectionResolver = require(this.config.connectionResolverPath);
    }
    if (this.connectionResolver) {
        if (typeof this.connectionResolver === "function") {
            this.connectionResolver = this.connectionResolver();
        }
        /* istanbul ignore else: no benefit of testing event handlers not being wired */
        if (typeof this.connectionResolver.on === "function") {
            this.connectionResolver.on("fetch", this.connectionResolverFetchErrorHandler.bind(this));
            this.connectionResolver.on("lazyfetch", this.connectionResolverFetchErrorHandler.bind(this));
        }
    }

    this.pools = {};

    this.ConnectionPool = helenus.ConnectionPool;
};

BaseDriver.prototype.remapConnectionOptions = function remapConnectionOptions(connectionData) {
};

BaseDriver.prototype.closePool = function closePool(pool, callback) {
    process.nextTick(callback);
};

BaseDriver.prototype.createConnectionPool = function createConnectionPool(poolConfig, callback) {
    callback(null);
};

BaseDriver.prototype.getConnectionPool = function getConnectionPool(keyspace, callback) {
    var self = this,
        keyspaceKey = keyspace || "default";

    function poolCreated(pool) {
        self.pools[keyspaceKey] = pool;
        callback(null, pool);
    }

    function getExistingPool(config, cb) {
        if (keyspace) {
            config = _.extend(config, { keyspace: keyspace });
        }
        var pool = self.pools[keyspaceKey];
        if (!pool && self.pools.default && self.pools.default.storeConfig.keyspace === keyspace) {
            pool = self.pools.default;
        }

        // If config has changed since the last call, close the pool and open a new connection
        if (!pool || pool.isClosed || !_.isEqual(pool.storeConfig, config)) {

            var createConnection = self.createConnectionPool.bind(self, config, poolCreated);

            if (pool && !pool.isClosed) {

                // TODO: Add monitoring on this to see how many times this happens.

                return void self.closePool(pool, createConnection);
            }

            return void createConnection();
        }

        cb(null, pool);
    }

    var poolConfig = _.extend({hosts: []}, self.poolConfig);
    if (self.connectionResolver) {
        self.connectionResolver.resolveConnection(self.config,
            function (err, connectionData) {
                if (err) {
                    self.logger.error("priam.ConnectionResolver: Error resolving connection options",
                        { name: err.name, code: err.code, error: err.message, stack: err.stack });
                    if (!connectionData) {
                        // only exit if no connection data was returned. Else, just log the error and move on
                        return void callback(err);
                    }
                }

                self.remapConnectionOptions(connectionData);
                var portMap = poolConfig.connectionResolverPortMap;
                if (portMap && portMap.from && portMap.to) {
                    connectionData.hosts = changePorts(connectionData.hosts, portMap.from, portMap.to);
                }

                poolConfig = _.extend(poolConfig, connectionData);
                getExistingPool(poolConfig, callback);
            });
    }
    else {
        getExistingPool(poolConfig, callback);
    }
};

BaseDriver.prototype.callWaiters = function callWaiters(pool, err) {
    pool.waiters.forEach(function (waiter) {
        process.nextTick(function () {
            waiter(err); // call waiters with error
        });
    });
    pool.waiters = []; // reset
};

BaseDriver.prototype.executeCqlOnDriver = function executeCqlOnDriver(pool, cqlStatement, params, consistency, options, callback) {
    process.nextTick(callback.bind(null, null, []));
};

BaseDriver.prototype.execCql = function execCql(cql, dataParams, options, callback) {
    /* istanbul ignore next: no benefit of testing coalesce for keyspace retrieval */
    var self = this,
        consistency = options.consistency || self.poolConfig.consistencyLevel || self.consistencyLevel.one,
        keyspace = (options || {}).keyspace;

    self.getConnectionPool(keyspace, function (err, pool) {
        if (err) {
            callback(err);
            return;
        }

        if (pool.isReady === false) {
            pool.waiters.push(function (e) {
                if (e) {
                    return void callback(e);
                }
                self.execCql(cql, dataParams, options, callback);
            });
            return;
        }

        // client removes used data entries, so we need to create a copy of the data in case it's needed for retry...
        var dataCopy = dataParams.slice();
        self.executeCqlOnDriver(pool, cql, dataCopy, consistency, options, function (err, results) {

            function execCqlCallback(e, res) {
                if (Array.isArray(res)) {
                    res = self.getNormalizedResults(res);
                }
                callback(e, res);
            }

            // Check if retry is allowed
            var retryCount = Math.max(options.retryCount || 0, 0),
                allowedRetries = Math.max(self.config.numRetries || 0, 0),
                retryDelay = Math.max(self.config.retryDelay || 100, 0),
                enableConsistencyFailover = (self.config.enableConsistencyFailover !== false),
                canRetryErrorType = !!err && self.canRetryError(err),
                shouldRetry = !!err && canRetryErrorType && (retryCount < allowedRetries);

            if (shouldRetry) {
                options.retryCount = (retryCount + 1);
                self.logger.warn("priam.Cql: Retryable error condition encountered. Executing retry #" + options.retryCount + " in " + retryDelay + "ms...",
                    { name: err.name, code: err.code, error: err.message, stack: err.stack });
                return void setTimeout(self.execCql.bind(self, cql, dataParams, options, execCqlCallback), retryDelay);
            }
            else if (enableConsistencyFailover) {
                // Fallback from all to localQuorum via additional retries
                if (err && consistency === self.consistencyLevel.all) {
                    options.consistency = self.consistencyLevel.quorum;
                    options.retryCount = 0;
                    return void setTimeout(self.execCql.bind(self, cql, dataParams, options, execCqlCallback), retryDelay);
                }
                else if (err && consistency === self.consistencyLevel.quorum) {
                    options.consistency = self.consistencyLevel.localQuorum;
                    options.retryCount = 0;
                    return void setTimeout(self.execCql.bind(self, cql, dataParams, options, execCqlCallback), retryDelay);
                }
            }

            execCqlCallback(err, results);
        });
    });

};

BaseDriver.prototype.canRetryError = function canRetryError(err) {
    return false;
};

/*
 namedQuery(queryName, dataParams, [options]: { consistency }, callback)
 */
BaseDriver.prototype.namedQuery = function namedQuery(name, dataParams, options, callback){
    var self = this,
        p = checkOptionalParameters(options, callback);
    options = p.options;
    callback = p.callback;
    if (typeof callback !== "function") {
        callback = function () {};
    }

    if (!self.queryCache) {
        throw new Error("'queryDirectory' option must be set in order to use #namedQuery()");
    }

    self.queryCache.readQuery(name, function(err, queryText){
        if (err) { return void callback(err); }
        options.queryName = options.queryName || name;
        options.executeAsPrepared = self.config.supportsPreparedStatements &&
            (typeof options.executeAsPrepared === "undefined") ? true : !!options.executeAsPrepared;
        self.cql(queryText, dataParams, options, callback);
    });
};

/*
 cql(cqlQuery, dataParams, [options]: { consistency }, [callback])
 */
BaseDriver.prototype.cql = function executeCqlQuery(cqlQuery, dataParams, options, callback) {
    var self = this,
        p = checkOptionalParameters(options, callback),
        metrics = this.metrics,
        captureMetrics,
        start;
    options = p.options;
    callback = p.callback;

    captureMetrics = !!(metrics && options.queryName);

    if (!options.suppressDebugLog) {
        var debugParams = [];
        /* istanbul ignore else: not easily tested and no real benefit to doing so */
        if (util.isArray(dataParams)) {
            dataParams.forEach(function (p) {
                if (p.hasOwnProperty("value") && p.hasOwnProperty("hint")) {
                    p = p.value;
                }
                debugParams.push(Buffer.isBuffer(p) ? "<buffer>" : JSON.stringify(p));
            });
        }
        self.logger.debug("priam.cql: executing cql statement", { consistencyLevel: options.consistency, cql: cqlQuery, params: debugParams });
    }
    dataParams = self.normalizeParameters(dataParams);

    if (captureMetrics) {
        start = process.hrtime();
    }
    self.execCql(cqlQuery, dataParams, options, function(err, result){
        if(captureMetrics){
            var duration = process.hrtime(start);
            duration = (duration[0] * 1e3) + (duration[1] / 1e6);
            metrics.measurement('query.' + options.queryName, duration, 'ms');
        }
        if (typeof callback === "function") {
            callback(err, result);
        }
    });
};

BaseDriver.prototype.select = function select(cql, dataParams, options, callback) {
    var params = setDefaultConsistency(this.consistencyLevel.ONE, options, callback);
    this.cql(cql, dataParams, params.options, params.callback);
};
BaseDriver.prototype.insert = function insert(cql, dataParams, options, callback) {
    var params = setDefaultConsistency(this.consistencyLevel.LOCAL_QUORUM, options, callback);
    this.cql(cql, dataParams, params.options, params.callback);
};
BaseDriver.prototype.update = function update(cql, dataParams, options, callback) {
    var params = setDefaultConsistency(this.consistencyLevel.LOCAL_QUORUM, options, callback);
    this.cql(cql, dataParams, params.options, params.callback);
};
BaseDriver.prototype.delete = function deleteQuery(cql, dataParams, options, callback) {
    var params = setDefaultConsistency(this.consistencyLevel.LOCAL_QUORUM, options, callback);
    this.cql(cql, dataParams, params.options, params.callback);
};

BaseDriver.prototype.beginQuery = function beginQuery() {
    return new Query(this);
};

BaseDriver.prototype.param = function(value, hint) {
  if (!hint) {
    return value;
  }

  hint = (hint in this.dataType) ? this.dataType[hint] : hint;
  return { value: value, hint: hint };
};

BaseDriver.prototype.close = function close(cb) {
    function closed() {
        /* istanbul ignore else: not easily tested and no real benefit to doing so */
        if (typeof cb === 'function') {
            cb();
        }
    }

    var pools = [],
        prop;
    for (prop in this.pools) {
        /* istanbul ignore else: not easily tested and no real benefit to doing so */
        if (this.pools.hasOwnProperty(prop)) {
            pools.push(this.pools[prop]);
        }
    }
    async.each(pools, this.closePool, closed);
};

BaseDriver.prototype.normalizeParameters = function normalizeParameters(original) {
    if (!original) {
        return [];
    }
    if (util.isArray(original)) {
        for (var i = 0; i < original.length; i++) {
            original[i] = this.dataToCql(original[i]);
        }
    }
    return original;
};

BaseDriver.prototype.getNormalizedResults = function getNormalizedResults(original) {
    return original;
};

BaseDriver.prototype.dataToCql = function dataToCql(val) {
    return val;
};

BaseDriver.prototype.connectionResolverFetchErrorHandler = function connectionResolverFetchErrorHandler(err, data) {
    if (err) {
        this.logger.warn("priam.ConnectionResolver: Error fetching connection options", { error: err.stack });
    }
    else {
        this.logger.debug("priam.ConnectionResolver: fetched connection options");
    }
};

function setDefaultConsistency(defaultConsistencyLevel, options, callback) {
    var p = checkOptionalParameters(options, callback);
    if (!p.options.consistency) {
        p.options.consistency = defaultConsistencyLevel;
    }
    return p;
}

function checkOptionalParameters(options, callback) {
    if (typeof options === "function") {
        callback = options;
        options = {};
    }
    else if (!options) {
        options = {};
    }
    return {
        options: options,
        callback: callback
    };
}

function changePorts(hostsArray, from, to) {
    return hostsArray.map(function (host) {
        var ix = host.indexOf(":");
        if (ix === -1) { return host; }

        var hostName = host.substring(0, ix),
            port = host.substring(ix + 1);

        if (port === from) {
            port = to;
        }

        return hostName + ":" + port;
    });
}