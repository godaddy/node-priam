'use strict';

var EventEmitter = require('events').EventEmitter
  , util = require('util')
  , async = require('async')
  , QueryCache = require('../util/query-cache')
  , Query = require('../util/query')
  , Batch = require('../util/batch')
  , typeCheck = require('../util/type-check')
  , fakeLogger = require('../util/fake-logger')
  , _ = require('lodash')
  , through = require('through2')
  , isStream = require('isstream')
  , uuid = require('uuid')
  , retry = require('retry');

module.exports = BaseDriver;

util.inherits(BaseDriver, EventEmitter);

function BaseDriver() {
  EventEmitter.call(this);
  this.consistencyLevel = {};
  this.dataType = {
    objectAscii: -1,
    objectText: -2
  };
  this.hostConfigKey = 'hosts';
}

BaseDriver.prototype.initProviderOptions = function initProviderOptions(config) {
};

BaseDriver.prototype.init = function init(context) {
  if (!context) {
    throw new Error('missing context object');
  }
  if (!context.config) {
    throw new Error('missing context.config object');
  }

  /* istanbul ignore next: no benefit of testing coalesce */
  this.config = context.config || {};
  this.logger = context.logger || fakeLogger;
  this.metrics = context.metrics;
  if (context.config.queryDirectory) {
    this.queryCache = new QueryCache({ queryDirectory: context.config.queryDirectory });
  }

  this.initProviderOptions(this.config);
  this.keyspace = context.config.keyspace;
  this.poolConfig = {
    consistencyLevel: this.consistencyLevel.one
  };
  this.poolConfig = _.extend(this.poolConfig, this.config);
  if( typeof this.config.consistency === 'string' ) {
    var level = this.config.consistency;

    if (typeof this.consistencyLevel[level] !== 'undefined') {
      this.poolConfig.consistencyLevel = this.consistencyLevel[level];
    } else {
      throw new Error('Error: "' + level + '" is not a valid consistency level');
    }
  }

  this.connectionResolver = null;
  if (context.connectionResolver) {
    this.connectionResolver = context.connectionResolver;
  }
  else if (this.config.connectionResolverPath) {
    this.connectionResolver = require(this.config.connectionResolverPath);
  }
  if (this.connectionResolver) {
    if (typeof this.connectionResolver === 'function') {
      this.connectionResolver = this.connectionResolver();
    }
    /* istanbul ignore else: no benefit of testing event handlers not being wired */
    if (typeof this.connectionResolver.on === 'function') {
      this.connectionResolver.on('fetch', this.connectionResolverFetchHandler.bind(this));
      this.connectionResolver.on('lazyfetch', this.connectionResolverFetchHandler.bind(this));
    }
  }

  this.pools = {};
  this.resultTransformers = context.resultTransformers || [];
};

BaseDriver.prototype.remapConnectionOptions = function remapConnectionOptions(connectionData) {
};

BaseDriver.prototype.closePool = function closePool(pool, callback) {
  process.nextTick(callback);
};

BaseDriver.prototype.createConnectionPool = function createConnectionPool(poolConfig, waitForConnect, callback) {
  callback(null, null);
};

BaseDriver.prototype.getConnectionPool = function getConnectionPool(keyspace, waitForConnect, callback) {
  var self = this;

  function poolCreated(err, pool) {
    if (err) { return void callback(err); }
    self.pools[keyspace] = pool;
    callback(null, pool);
  }

  function getExistingPool(config, cb) {
    if (keyspace) {
      config = _.extend(config, { keyspace: keyspace });
    }

    var pool = self.pools[keyspace];

    // If config has changed since the last call, close the pool and open a new connection
    if (!pool || pool.isClosed || !_.isEqual(pool.storeConfig, config)) {

      var createConnection = self.createConnectionPool.bind(self, config, waitForConnect, poolCreated);

      if (pool && !pool.isClosed) {
        return void self.closePool(pool, createConnection);
      }

      return void createConnection();
    }

    cb(null, pool);
  }

  var emptyConfig = {hosts: []};
  self.remapConnectionOptions(emptyConfig);
  var poolConfig = _.extend(emptyConfig, self.poolConfig);
  if (self.connectionResolver) {
    var resolutionRequestId = uuid.v4();
    self.emit('connectionResolving', resolutionRequestId);
    self.connectionResolver.resolveConnection(self.config,
      function (err, connectionData) {
        if (err) {
          self.emit('connectionResolvedError', resolutionRequestId, err);
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
          connectionData[self.hostConfigKey] = changePorts(connectionData[self.hostConfigKey], portMap.from, portMap.to);
        }

        poolConfig = _.extend(poolConfig, connectionData);
        self.emit('connectionResolved', resolutionRequestId);

        getExistingPool(poolConfig, callback);
      });
  }
  else {
    getExistingPool(poolConfig, callback);
  }
};

BaseDriver.prototype.callWaiters = function callWaiters(err, pool) {
  pool.waiters.forEach(function (waiter) {
    process.nextTick(function () {
      waiter(err); // pass possible error to waiting queries
    });
  });
  pool.waiters = []; // reset
};

BaseDriver.prototype.getResultTransformStream = function getResultTransformStream(options) {
  var self = this;
  return through.obj(function transformRecord(record, encoding, next) {
    if (record) {
      var normalized = self.getNormalizedResults([record]);
      record = normalized[0];
      if (Array.isArray(options.resultTransformers)) {
        for (var i = 0; i < options.resultTransformers.length; i++) {
          record = options.resultTransformers[i](record);
        }
      }
    }
    next(null, record);
  });
};

BaseDriver.prototype.executeCqlOnDriver = function executeCqlOnDriver(pool, cqlStatement, params, consistency, options, callback) {
  process.nextTick(callback.bind(null, null, []));
};

BaseDriver.prototype.connect = function connect(keyspace, callback) {
  if (typeof keyspace === 'function') {
    callback = keyspace;
    keyspace = null;
  }

  performConnect.call(this, keyspace, true, callback);
};

function performConnect(keyspace, waitForConnect, callback) {
  /* jshint validthis: true */
  var self = this
    , connectionRequestId = uuid.v4();

  self.emit('connectionRequested', connectionRequestId);
  self.getConnectionPool(keyspace, waitForConnect, function (err, pool) {
    if (err) {
      callback(err);
      return;
    }

    self.emit('connectionAvailable', connectionRequestId);
    callback(null, pool);
  });
}

// TODO: we need to have some sort of incremental retry logic but we will leave
// this up to the consumer in this case as its a very special cased type thing.
// We can potentially make it generic in some way later on
BaseDriver.prototype.execCqlStream = function execCqlStream(cql, dataParams, options, stream) {
   /* istanbul ignore next: no benefit of testing coalesce for keyspace retrieval */
  var self = this
    , keyspace = (options || {}).keyspace || this.keyspace
    , consistency = options.consistency || self.poolConfig.consistencyLevel || self.consistencyLevel.one;

  performConnect.call(self, keyspace, false, function (err, pool) {
    if (err) {
      return stream.emit(err);
    }

    if (pool.isReady === false) {
      pool.waiters.push(function (e) {
        if (e) {
          return stream.emit(e);
        }
        self.execCqlStream(cql, dataParams, options, stream);
      });
      return;
    }

    self.streamCqlOnDriver(pool, cql, dataParams, consistency, options, stream);

  });
};

BaseDriver.prototype.execCql = function execCql(cql, dataParams, options, queryRequestId, callback) {
  /* istanbul ignore next: no benefit of testing coalesce for keyspace retrieval */
  var self = this
    , keyspace = (options || {}).keyspace || this.keyspace
    , consistency = options.consistency || self.poolConfig.consistencyLevel || self.consistencyLevel.one;

  performConnect.call(self, keyspace, false, function (err, pool) {
    if (err) {
      return void callback(err);
    }

    if (pool.isReady === false) {
      pool.waiters.push(function (e) {
        if (e) {
          return void callback(e);
        }
        self.execCql(cql, dataParams, options, queryRequestId, callback);
      });
      return;
    }

    // client removes used data entries, so we need to create a copy of the data in case it's needed for retry...
    var dataCopy = dataParams.slice()
      , queryRequestId = uuid.v4();
    self.emit('queryStarted', queryRequestId, cql, dataCopy, consistency, options);
    self.executeCqlOnDriver(pool, cql, dataCopy, consistency, options, function (err, results) {

      function execCqlCallback(e, res) {
        if (Array.isArray(res)) {
          res = self.getNormalizedResults(res, options);
        }
        if (e) {
          e.cql = cql;
        }
        callback(e, res);
      }

      execCqlCallback(err, results);
    });
  });

};

BaseDriver.prototype.canRetryError = function canRetryError() {
  return false;
};

/*
 namedQuery(queryName, dataParams, [options]: { consistency }, callback)
 */
BaseDriver.prototype.namedQuery = function namedQuery(name, dataParams, options, callback) {
  var self = this,
    p = checkOptionalParameters(options, callback);
  options = p.options;
  callback = p.callback;
  if (typeof callback !== "function") {
    callback = function () {
    };
  }

  if (!self.queryCache) {
    throw new Error('"queryDirectory" option must be set in order to use #namedQuery()');
  }

  self.queryCache.readQuery(name, function (err, queryText) {
    if (err) {
      return void callback(err);
    }
    options.queryName = options.queryName || name;
    options.executeAsPrepared = self.config.supportsPreparedStatements &&
      (typeof options.executeAsPrepared === 'undefined') ? true : !!options.executeAsPrepared;
    self.cql(queryText, dataParams, options, callback);
  });
};

/*
 cql(cqlQuery, dataParams, [options]: { consistency }, [callback])
 */
BaseDriver.prototype.cql = function executeCqlQuery(cqlQuery, dataParams, options, callback) {
  var self = this
    , p = checkOptionalParameters(options, callback)
    , metrics = this.metrics
    , captureMetrics
    , start;
  options = p.options;
  callback = p.callback;

  captureMetrics = !!(metrics && options.queryName);

  if (!options.suppressDebugLog) {
    var debugParams = [];
    /* istanbul ignore else: not easily tested and no real benefit to doing so */
    if (util.isArray(dataParams)) {
      dataParams.forEach(function (p) {
        if (p && p.hasOwnProperty('value') && p.hasOwnProperty('hint')) {
          p = p.value;
        }
        debugParams.push(Buffer.isBuffer(p) ? '<buffer>' : JSON.stringify(p));
      });
    }
    self.logger.debug('priam.cql: executing cql statement', { consistencyLevel: options.consistency, cql: cqlQuery, params: debugParams });
  }
  dataParams = self.normalizeParameters(dataParams);

  if (captureMetrics) {
    start = process.hrtime();
  }

  var retryOptions = {
    retries: 0
  };

  self.config.retryOptions = self.config.retryOptions || retryOptions;
  var retryDelay = Math.max(self.config.retryDelay || 100, 0);

  if (self.config.numRetries){
    self.config.retryOptions = {
      retries: Math.max(self.config.numRetries, 0),
      factor: 1,
      minTimeout: retryDelay,
      maxTimeout: retryDelay,
      randomize: false
    };
  } 

  function executeRetryableCql(){

    var consistency = options.consistency || self.poolConfig.consistencyLevel || self.consistencyLevel.one;

    var operation = retry.operation(self.config.retryOptions);

    operation.attempt(function(currentAttempt) {
      var queryRequestId = uuid.v4();
      self.execCql(cqlQuery, dataParams, options, queryRequestId, function (err, result) {

        var canRetryErrorType = !!err && self.canRetryError(err);
        var enableConsistencyFailover = (self.config.enableConsistencyFailover !== false);

        if (canRetryErrorType && operation.retry(err)) {
          self.logger.warn('priam.Cql: Retryable error condition encountered. Executing retry #' + currentAttempt + '...',
          { name: err.name, code: err.code, error: err.message, stack: err.stack });
          self.emit('queryRetried', queryRequestId, cqlQuery, dataParams, options);
          return;
        }

        // retries are complete. lets fallback on quorum if needed
        if (canRetryErrorType && enableConsistencyFailover) {
          // Fallback from all to localQuorum via additional retries
          if (err && consistency === self.consistencyLevel.all) {
            options.consistency = self.consistencyLevel.eachQuorum;
            return void setTimeout(executeRetryableCql, retryDelay);
          }
          else if (err && (
            consistency === self.consistencyLevel.eachQuorum ||
            consistency === self.consistencyLevel.quorum)) {
            options.consistency = self.consistencyLevel.localQuorum;
            return void setTimeout(executeRetryableCql, retryDelay);
          }
        }

        self.emit(err ? 'queryFailed' : 'queryCompleted', queryRequestId);

        // all retries are finally complete
        if (captureMetrics) {
          var duration = process.hrtime(start);
          duration = (duration[0] * 1e3) + (duration[1] / 1e6);
          metrics.measurement('query.' + options.queryName, duration, 'ms');
        }
        if (typeof callback === 'function') {
          if (result && result.length && options && options.resultTransformers && options.resultTransformers.length) {
            for (var i = 0; i < options.resultTransformers.length; i++) {
              result = result.map(options.resultTransformers[i]);
            }
          }
          callback(err, result);
        }
      });
    });
  }

  // Keep passing the stream down the chain so we are still reusing logic
  if (isStream(callback)){
    return self.execCqlStream(cqlQuery, dataParams, options, callback);
  } else {
    return executeRetryableCql();
  }
};

BaseDriver.prototype.query = BaseDriver.prototype.cql;

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
  var self = this;
  var querySettings = {
    options: {consistency: self.poolConfig.consistencyLevel},
    resultTransformers: self.resultTransformers.slice(0)
  };
  return new Query(this, querySettings);
};

BaseDriver.prototype.beginBatch = function beginBatch() {
  return new Batch(this);
};

BaseDriver.prototype.param = function (value, hint, isRoutingKey) {
  if (!hint) {
    return value;
  }

  hint = (hint in this.dataType) ? this.dataType[hint] : hint;
  return { value: value, hint: hint, isRoutingKey: isRoutingKey === true };
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
  async.each(pools, this.closePool.bind(this), closed);
};

BaseDriver.prototype.normalizeParameters = function normalizeParameters(original) {
  if (!original) {
    return [];
  }
  if (util.isArray(original)) {
    for (var i = 0; i < original.length; i++) {
      if (original[i] && original[i].hint) {
        original[i].hint = this.getDriverDataType(original[i].hint);
      }
      original[i] = this.dataToCql(original[i]);
    }
  }
  return original;
};

BaseDriver.prototype.getDriverDataType = function getDriverDataType(type) {
  // convert 'objectAscii' type to 'ascii', etc.
  var prop;
  for (prop in this.dataType) {
    if (this.dataType.hasOwnProperty(prop) && this.dataType[prop] === type) {
      // check if property starts with Object
      if (prop.length > 7 && prop.substring(0, 6) === 'object') {
        prop = prop.substring(6, 7).toLowerCase() + prop.substring(7);
        return this.dataType[prop];
      }
      break;
    }
  }
  return type;
};

BaseDriver.prototype.checkObjectResult = function checkObjectResult(result, fieldName, options) {
  // attempt to deserialize
  var isObjectType = false;
  if (options && options.resultHint && options.resultHint[fieldName]) {
    var type = options.resultHint[fieldName];
    isObjectType = (type === this.dataType.objectAscii || type === this.dataType.objectText);
  }
  if (options && options.deserializeJsonStrings === true) {
    isObjectType = (result.length > 0 && (result[0] === "{" || result[0] === "[" || result === "null"));
  }

  if (isObjectType) {
    try {
      result = JSON.parse(result);
    } catch (e) {
      // ignore
    }
  }

  return result;
};

BaseDriver.prototype.getNormalizedResults = function getNormalizedResults(original, options) {
  return original;
};

BaseDriver.prototype.dataToCql = function dataToCql(val) {
  return val;
};

BaseDriver.prototype.connectionResolverFetchHandler = function connectionResolverFetchHandler(err, data) {
  if (err) {
    this.emit('connectionOptionsError', err);
    this.logger.warn('priam.ConnectionResolver: Error fetching connection options', { error: err.stack });
  } else {
    this.emit('connectionOptionsFetched', data);
    this.logger.debug('priam.ConnectionResolver: fetched connection options');
  }
};

BaseDriver.prototype.isBatch = typeCheck.isBatch;

BaseDriver.prototype.isQuery = typeCheck.isQuery;

function setDefaultConsistency(defaultConsistencyLevel, options, callback) {
  var p = checkOptionalParameters(options, callback);
  if (!p.options.consistency) {
    p.options.consistency = defaultConsistencyLevel;
  }
  return p;
}

function checkOptionalParameters(options, callback) {
  if (typeof options === 'function') {
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
    var ix = host.indexOf(':');
    if (ix === -1) {
      return host;
    }

    var hostName = host.substring(0, ix),
      port = host.substring(ix + 1);

    if (port === from) {
      port = to;
    }

    return hostName + ':' + port;
  });
}
