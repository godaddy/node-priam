'use strict';

var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');
var async = require('async');
var util = require('util');
var uuid = require('uuid');
var through = require('through2');
var isStream  = require('isstream');
var retry = require('retry');
var cqlDriver = require('cassandra-driver');
var fakeLogger = require('./util/fake-logger');
var typeCheck = require('./util/type-check');
var QueryCache = require('./util/query-cache');
var Query = require('./util/query');
var Batch = require('./util/batch');

function DatastaxDriver() {
  EventEmitter.call(this);
  this.name = 'datastax';

  this.consistencyLevel = {};
  this.hostConfigKey = 'contactPoints';

  this.dataType = _.extend(
    {
      objectAscii: -1,
      objectText: -2
    },
    cqlDriver.types.dataTypes);

  var consistencies = cqlDriver.types.consistencies;
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
    LOCAL_ONE: consistencies.localOne,
    localOne: consistencies.localOne,
    EACH_QUORUM: consistencies.eachQuorum,
    eachQuorum: consistencies.eachQuorum,
    ALL: consistencies.all,
    all: consistencies.all,
    ANY: consistencies.any,
    any: consistencies.any
  };
}
util.inherits(DatastaxDriver, EventEmitter);

module.exports = function dataStaxDriverFactory(context) {
  var driver = new DatastaxDriver();
  driver.init(context);
  return driver;
};
module.exports.DatastaxDriver = DatastaxDriver;

DatastaxDriver.prototype.init = function init(context) {
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
  if (typeof this.config.consistency === 'string') {
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
  } else {
    if (this.config.connectionResolverPath) {
      this.connectionResolver = require(this.config.connectionResolverPath);
    }
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

DatastaxDriver.prototype.initProviderOptions = function init(config) {
  setHelenusOptions(config);
  config.supportsPreparedStatements = true;
};

DatastaxDriver.prototype.getConnectionPool = function getConnectionPool(keyspace, waitForConnect, callback) {
  var self = this;
  keyspace = keyspace || self.keyspace;

  function poolCreated(err, pool) {
    if (err) {
      return void callback(err);
    }
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

  var emptyConfig = { hosts: [] };
  self.remapConnectionOptions(emptyConfig);
  var poolConfig = _.extend(emptyConfig, self.poolConfig);
  if (self.connectionResolver) {
    var resolutionRequestId = uuid.v4();
    self.emit('connectionResolving', resolutionRequestId);
    self.connectionResolver.resolveConnection(self.config,
      function (err, connectionData) {
        if (err) {
          self.emit('connectionResolvedError', resolutionRequestId, err);
          self.logger.error('priam.ConnectionResolver: Error resolving connection options',
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
  } else {
    getExistingPool(poolConfig, callback);
  }
};

DatastaxDriver.prototype.createConnectionPool = function createConnectionPool(poolConfig, waitForConnect, callback) {
  var self          = this,
      openRequestId = uuid.v4(),
      pool;

  self.logger.debug('priam.Driver: Creating new pool', {
    poolConfig: {
      keyspace: poolConfig.keyspace,
      contactPoints: poolConfig.contactPoints
    }
  });

  var dsPoolConfig = _.cloneDeep(poolConfig);
  if (dsPoolConfig.username && dsPoolConfig.password) {
    dsPoolConfig.authProvider = new cqlDriver.auth.PlainTextAuthProvider(dsPoolConfig.username, dsPoolConfig.password);
  }

  dsPoolConfig.queryOptions = dsPoolConfig.queryOptions || {};
  dsPoolConfig.queryOptions.fetchSize = dsPoolConfig.limit;
  dsPoolConfig.queryOptions.prepare = false;
  if (dsPoolConfig.consistencyLevel) {
    dsPoolConfig.queryOptions.consistency = dsPoolConfig.consistencyLevel;
  }
  var port = null;
  if (Array.isArray(dsPoolConfig.contactPoints)) {
    for (var i = 0; i < dsPoolConfig.contactPoints.length; i++) {
      var split = dsPoolConfig.contactPoints[i].split(':');
      dsPoolConfig.contactPoints[i] = split[0].trim();
      if (split.length > 1) {
        port = parseInt(split[1].trim(), 10);
      }
    }
    if (port !== null) {
      dsPoolConfig.protocolOptions = dsPoolConfig.protocolOptions || {};
      dsPoolConfig.protocolOptions.port = port;
    }
  }

  if (poolConfig.getAConnectionTimeout) {
    dsPoolConfig.socketOptions = dsPoolConfig.socketOptions || {};
    dsPoolConfig.socketOptions.connectTimeout = poolConfig.getAConnectionTimeout;
  }

  if (poolConfig.poolSize) {
    dsPoolConfig.pooling = dsPoolConfig.pooling || {};
    dsPoolConfig.pooling.coreConnectionsPerHost = dsPoolConfig.pooling.coreConnectionsPerHost || {};
    dsPoolConfig.pooling.coreConnectionsPerHost[cqlDriver.types.distance.local.toString()] = poolConfig.poolSize;
    dsPoolConfig.pooling.coreConnectionsPerHost[cqlDriver.types.distance.remote.toString()] = Math.ceil(poolConfig.poolSize / 2);
  }

  pool = new cqlDriver.Client(dsPoolConfig);
  pool.storeConfig = poolConfig;
  pool.waiters = [];
  pool.isReady = false;
  pool.on('log', function (level, message, data) {
    self.emit('connectionLogged', level, message, data);
    // unrecoverable errors will yield error on execution, so treat these as warnings since they'll be retried
    // treat everything else as debug information.
    var logMethod = (level === 'error' || level === 'warning') ? 'warn' : 'debug';

    var metaData = {
      datastaxLogLevel: level
    };
    if (typeof data === 'string') {
      message += ': ' + data;
    } else {
      metaData.data = data;
    }
    self.logger[logMethod]('priam.Driver.' + message, metaData);
  });

  this.emit('connectionOpening', openRequestId);
  pool.connect(function (err) {
    if (err) {
      self.emit('connectionFailed', openRequestId, err);
      self.logger.error('priam.Driver: Pool Connect Error',
        { name: err.name, error: err.message, inner: err.innerErrors });
      if (waitForConnect) {
        callback(err, pool);
      }
      self.callWaiters(err, pool);
      return void self.closePool(pool);
    }
    pool.isReady = true;
    self.emit('connectionOpened', openRequestId);
    if (waitForConnect) {
      callback(null, pool);
    }
    self.callWaiters(null, pool);
  });
  if (!waitForConnect) {
    callback(null, pool);
  }
};

DatastaxDriver.prototype.remapConnectionOptions = function remapConnectionOptions(connectionData) {
  remapOption(connectionData, 'user', 'username');
  remapOption(connectionData, 'hosts', 'contactPoints');
};

DatastaxDriver.prototype.connect = function connect(keyspace, callback) {
  if (typeof keyspace === 'function') {
    callback = keyspace;
    keyspace = null;
  }

  performConnect.call(this, keyspace, true, callback);
};

function performConnect(keyspace, waitForConnect, callback) {
  /* jshint validthis: true */
  var self                = this,
      connectionRequestId = uuid.v4();

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

DatastaxDriver.prototype.connectionResolverFetchHandler = function connectionResolverFetchHandler(err, data) {
  if (err) {
    this.emit('connectionOptionsError', err);
    this.logger.warn('priam.ConnectionResolver: Error fetching connection options', { error: err.stack });
  } else {
    this.emit('connectionOptionsFetched', data);
    this.logger.debug('priam.ConnectionResolver: fetched connection options');
  }
};

DatastaxDriver.prototype.closePool = function closePool(pool, callback) {
  if (pool.isReady && !pool.isClosed) {
    pool.isClosed = true;
    pool.isReady = false;
    pool.shutdown(callback);
  } else {
    if (_.isFunction(callback)) {
      pool.isClosed = true;
      pool.isReady = false;
      process.nextTick(callback);
    }
  }
  this.emit('connectionClosed');
};

DatastaxDriver.prototype.close = function close(cb) {
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

DatastaxDriver.prototype.callWaiters = function callWaiters(err, pool) {
  pool.waiters.forEach(function (waiter) {
    process.nextTick(function () {
      waiter(err); // pass possible error to waiting queries
    });
  });
  pool.waiters = []; // reset
};

DatastaxDriver.prototype.getResultTransformStream = function getResultTransformStream(options) {
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

DatastaxDriver.prototype.executeCqlOnDriver = function executeCqlOnDriver(pool, cqlStatement, params, consistency, options, callback) {
  var self = this;
  var opts = this.prepareQueryArgs(pool, cqlStatement, params, consistency, options);
  var execOptions = opts.execOptions;
  cqlStatement = opts.cqlStatement;
  params = opts.params;
  pool.execute(cqlStatement, params, execOptions, function (err, data) {
    if (err) {
      if (err instanceof cqlDriver.errors.ResponseError) {
        err.type = findResponseErrorType(err.code);
      }
      return void callback(err);
    }
    var result = (data && data.rows) ? data.rows : [];
    return void callback(null, result);
  });
};

DatastaxDriver.prototype.streamCqlOnDriver = function streamCqlOnDriver(pool, cqlStatement, params, consistency, options, stream) {
  var opts = this.prepareQueryArgs(pool, cqlStatement, params, consistency, options);
  var execOptions = opts.execOptions;
  cqlStatement = opts.cqlStatement;
  params = opts.params;
  pool.stream(cqlStatement, params, execOptions)
    .on('error', stream.emit.bind(stream, 'error'))
    .pipe(this.getResultTransformStream(execOptions))
    .on('error', stream.emit.bind(stream, 'error'))
    .pipe(stream);
};

// TODO: we need to have some sort of incremental retry logic but we will leave
// this up to the consumer in this case as its a very special cased type thing.
// We can potentially make it generic in some way later on
DatastaxDriver.prototype.execCqlStream = function execCqlStream(cql, dataParams, options, stream) {
  /* istanbul ignore next: no benefit of testing coalesce for keyspace retrieval */
  var self        = this,
      keyspace    = (options || {}).keyspace || this.keyspace,
      consistency = options.consistency || self.poolConfig.consistencyLevel || self.consistencyLevel.one;

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

DatastaxDriver.prototype.execCql = function execCql(cql, dataParams, options, queryRequestId, callback) {
  /* istanbul ignore next: no benefit of testing coalesce for keyspace retrieval */
  var self        = this,
      keyspace    = (options || {}).keyspace || this.keyspace,
      consistency = options.consistency || self.poolConfig.consistencyLevel || self.consistencyLevel.one;

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
    var dataCopy       = dataParams.slice(),
        queryRequestId = uuid.v4();
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

/*
 namedQuery(queryName, dataParams, [options]: { consistency }, callback)
 */
DatastaxDriver.prototype.namedQuery = function namedQuery(name, dataParams, options, callback) {
  var self = this,
      p    = checkOptionalParameters(options, callback);
  options = p.options;
  callback = p.callback;
  if (typeof callback !== 'function') {
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
DatastaxDriver.prototype.cql = function executeCqlQuery(cqlQuery, dataParams, options, callback) {
  var self    = this,
      p       = checkOptionalParameters(options, callback),
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
        if (p && p.hasOwnProperty('value') && p.hasOwnProperty('hint')) {
          p = p.value;
        }
        debugParams.push(Buffer.isBuffer(p) ? '<buffer>' : JSON.stringify(p));
      });
    }
    self.logger.debug('priam.cql: executing cql statement', {
      consistencyLevel: options.consistency,
      cql: cqlQuery,
      params: debugParams
    });
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

  if (self.config.numRetries) {
    self.config.retryOptions = {
      retries: Math.max(self.config.numRetries, 0),
      factor: 1,
      minTimeout: retryDelay,
      maxTimeout: retryDelay,
      randomize: false
    };
  }

  function executeRetryableCql() {
    /* istanbul ignore next: ignroe coalesce */
    var consistency = options.consistency || self.poolConfig.consistencyLevel || self.consistencyLevel.one;

    var operation = retry.operation(self.config.retryOptions);

    operation.attempt(function (currentAttempt) {
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
          } else {
            if (err && (
              consistency === self.consistencyLevel.eachQuorum ||
              consistency === self.consistencyLevel.quorum)) {
              options.consistency = self.consistencyLevel.localQuorum;
              return void setTimeout(executeRetryableCql, retryDelay);
            }
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
          if (result && result.length) {
            if (options.resultTransformers && options.resultTransformers.length) {
              for (var i = 0; i < options.resultTransformers.length; i++) {
                result = result.map(options.resultTransformers[i]);
              }
            }
          }
          callback(err, result);
        }
      });
    });
  }

  // Keep passing the stream down the chain so we are still reusing logic
  if (isStream(callback)) {
    return self.execCqlStream(cqlQuery, dataParams, options, callback);
  } else {
    return executeRetryableCql();
  }
};

DatastaxDriver.prototype.prepareQueryArgs = function prepareQueryArgs(pool, cqlStatement, params, consistency, options) {
  var self = this;
  var execOptions = _.assign({
    prepare: !!options.executeAsPrepared,
    consistency: consistency
  }, options);

  var hints = [];
  var routingIndexes = [];
  _.forEach(params, function (param, index) {
    if (param && param.hasOwnProperty('value') && param.hasOwnProperty('hint')) {
      params[index] = param.value;
      if (param.hint) {
        if (_.isString(param.hint)) {
          param.hint = self.dataType.getByName(param.hint);
        }
        hints[index] = param.hint;
      }
      if (param.isRoutingKey) {
        routingIndexes.push(index);
      }
    }
  });
  if (hints.length) {
    execOptions.hints = hints;
  }
  if (routingIndexes.length) {
    execOptions.routingIndexes = routingIndexes;
  }
  return { cqlStatement: cqlStatement, params: params, execOptions: execOptions };
};

DatastaxDriver.prototype.getDriverDataType = function getDriverDataType(type) {
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

DatastaxDriver.prototype.normalizeParameters = function normalizeParameters(original) {
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

DatastaxDriver.prototype.checkObjectResult = function checkObjectResult(result, fieldName, options) {
  // attempt to deserialize
  var isObjectType = false;
  if (options && options.resultHint && options.resultHint[fieldName]) {
    var type = options.resultHint[fieldName];
    isObjectType = (type === this.dataType.objectAscii || type === this.dataType.objectText);
  }
  if (options && options.deserializeJsonStrings === true) {
    isObjectType = (result.length > 0 && (result[0] === '{' || result[0] === '[' || result === 'null'));
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

DatastaxDriver.prototype.getNormalizedResults = function getNormalizedResults(original, options) {
  var self = this;
  var shouldCoerce = shouldCoerceDataStaxTypes(options, self.config);
  var results = _.map(original, function (row) {
    var result = {};
    _.forOwn(row, function (value, name) {
      if ((name === 'columns' || name === '__columns') && _.isObject(value)) {
        return;
      } // skip metadata
      if (typeof value === 'string') {
        value = self.checkObjectResult(value, name, options);
      }
      result[name] = value;
    });
    if (shouldCoerce) {
      coerceDataStaxTypes(result);
    }
    return result;
  });
  return results;
};

var numberRegex = /^[0-9]+$/;
DatastaxDriver.prototype.dataToCql = function dataToCql(val) {
  if (val && val.hasOwnProperty('value') && val.hasOwnProperty('hint')) {

    // Transform timestamp values into Date objects if number or string
    if (val.hint === this.dataType.timestamp) {
      if (typeof val.value === 'number') {
        val.value = new Date(val.value);
      } else {
        if (typeof val.value === 'string') {
          if (numberRegex.test(val.value)) {
            val.value = new Date(parseInt(val.value, 10)); // string of numbers
          } else {
            val.value = new Date(val.value); // assume ISO string
          }
        }
      }
    }

    return val; // {value,hint} style parameter - hint will be extracted out on the execute step
  }

  if (!Buffer.isBuffer(val) && (util.isArray(val) || typeof val === 'object')) {
    // arrays and objects should be JSON'ized
    return JSON.stringify(val);
  }

  return val; // use as-is
};


var retryableResponseErrors = [
  cqlDriver.types.responseErrorCodes.serverError,
  cqlDriver.types.responseErrorCodes.unavailableException,
  cqlDriver.types.responseErrorCodes.overloaded,
  cqlDriver.types.responseErrorCodes.isBootstrapping,
  cqlDriver.types.responseErrorCodes.writeTimeout,
  cqlDriver.types.responseErrorCodes.readTimeout
];

DatastaxDriver.prototype.canRetryError = function canRetryError(err) {
  if (err instanceof cqlDriver.errors.ResponseError) {
    return retryableResponseErrors.indexOf(err.code) !== -1;
  }

  return (err instanceof cqlDriver.errors.NoHostAvailableError ||
  err instanceof cqlDriver.errors.DriverInternalError);
};

DatastaxDriver.prototype.query = DatastaxDriver.prototype.cql;

DatastaxDriver.prototype.select = function select(cql, dataParams, options, callback) {
  var params = setDefaultConsistency(this.consistencyLevel.ONE, options, callback);
  this.cql(cql, dataParams, params.options, params.callback);
};
DatastaxDriver.prototype.insert = function insert(cql, dataParams, options, callback) {
  var params = setDefaultConsistency(this.consistencyLevel.LOCAL_QUORUM, options, callback);
  this.cql(cql, dataParams, params.options, params.callback);
};
DatastaxDriver.prototype.update = function update(cql, dataParams, options, callback) {
  var params = setDefaultConsistency(this.consistencyLevel.LOCAL_QUORUM, options, callback);
  this.cql(cql, dataParams, params.options, params.callback);
};
DatastaxDriver.prototype.delete = function deleteQuery(cql, dataParams, options, callback) {
  var params = setDefaultConsistency(this.consistencyLevel.LOCAL_QUORUM, options, callback);
  this.cql(cql, dataParams, params.options, params.callback);
};

DatastaxDriver.prototype.isBatch = typeCheck.isBatch;

DatastaxDriver.prototype.isQuery = typeCheck.isQuery;

DatastaxDriver.prototype.beginQuery = function beginQuery() {
  var self = this;
  var querySettings = {
    options: { consistency: self.poolConfig.consistencyLevel },
    resultTransformers: self.resultTransformers.slice(0)
  };
  return new Query(this, querySettings);
};

DatastaxDriver.prototype.beginBatch = function beginBatch() {
  return new Batch(this);
};

DatastaxDriver.prototype.param = function (value, hint, isRoutingKey) {
  if (!hint) {
    return value;
  }

  hint = (hint in this.dataType) ? this.dataType[hint] : hint;
  return { value: value, hint: hint, isRoutingKey: isRoutingKey === true };
};

function findResponseErrorType(code) {
  return _.findKey(cqlDriver.types.responseErrorCodes,
      function (val) {
        return val === code;
      }) || 'unknown';
}

function remapOption(config, from, to) {
  if (config.hasOwnProperty(from)) {
    config[to] = config[from];
    delete config[from];
  }
}

function setHelenusOptions(config) {
  remapOption(config, 'timeout', 'getAConnectionTimeout');
  remapOption(config, 'hostPoolSize', 'poolSize');
  remapOption(config, 'cqlVersion', 'version');
  remapOption(config, 'user', 'username');
  remapOption(config, 'hosts', 'contactPoints');
}

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
  } else {
    if (!options) {
      options = {};
    }
  }
  return {
    options: options,
    callback: callback
  };
}

var cqlDriverNumericTypeNames = ['BigDecimal', 'Integer', 'Long'];
var cqlDriverNumericTypes = _.pick(cqlDriver.types, cqlDriverNumericTypeNames);
var cqlDriverStringTypeNames = ['Uuid', 'TimeUuid'];
var cqlDriverStringTypes = _.pick(cqlDriver.types, cqlDriverStringTypeNames);

function shouldCoerceDataStaxTypes(options, config) {
  /* istanbul ignore next: no benefit of testing coalesce */
  var coerce = (options || {}).coerceDataStaxTypes;
  if (isNullOrUndefined(coerce)) {
    coerce = config.coerceDataStaxTypes;
  }
  return coerce !== false;
}

function coerceDataStaxTypes(record) {
  _.forOwn(record, function (value, key) {
    record[key] = coerceDataStaxType(value);
  });
}

function coerceDataStaxType(value) {
  if (isNullOrUndefined(value) || typeof value !== 'object' || Buffer.isBuffer(value)) {
    return value; // already a primitive, buffer, or null
  }

  var testValue, type, i;
  if (Array.isArray(value)) {
    // Set/List types
    testValue = null;
    for (i = 0; i < value.length; i++) {
      if (!isNullOrUndefined(value[i])) {
        testValue = value[i];
        break;
      }
    }
    if (testValue !== null) {
      if ((type = getNumericDataStaxTypeName(testValue))) {
        // Array of numerics
        value = value.map(function (v) {
          if (isNullOrUndefined(v)) { return v; }
          return convertDataStaxNumericType(type, v);
        });
      } else if ((type = getStringDataStaxTypeName(testValue))) {
        // Array of strings
        value = value.map(function (v) {
          if (isNullOrUndefined(v)) { return v; }
          return v.toString();
        });
      }
    }
  } else if ((type = getNumericDataStaxTypeName(value))) {
    // Numeric Types
    return convertDataStaxNumericType(type, value);
  } else if ((type = getStringDataStaxTypeName(value))) {
    // String Types
    return value.toString();
  } else {
    // Map Types
    var keys = Object.keys(value);
    if (!keys.length) { return value; } // empty object

    // Get value type
    var valueType = null;
    var valueTypeIsString = false;
    testValue = null;
    for (i = 0; i < keys.length; i++) {
      if (!isNullOrUndefined(value[keys[i]])) {
        testValue = value[keys[i]];
        break;
      }
    }
    if (testValue !== null) {
      valueType = getNumericDataStaxTypeName(testValue);
      if (!valueType && (valueType = getStringDataStaxTypeName(testValue))) {
        valueTypeIsString = true;
      }
    }

    // Perform mapping
    if (valueType) {
      var transformed = value;
      var key, mappedValue;
      for (i = 0; i < keys.length; i++) {
        key = keys[i];
        mappedValue = value[key];

        if (!isNullOrUndefined(mappedValue)) {
          /* istanbul ignore next: else if condition is there for clarity, evne though it should always be populated */
          if (valueTypeIsString) {
            mappedValue = mappedValue.toString();
          } else if (valueType) {
            mappedValue = convertDataStaxNumericType(valueType, mappedValue);
          }
        }
        transformed[key] = mappedValue;
      }
      return transformed;
    }
  }

  return value;
}

function convertDataStaxNumericType(type, value) {
  if (type === 'BigDecimal') {
    return parseFloat(value.toString());
  }
  return parseInt(value.toString(), 10);
}

function getStringDataStaxTypeName(value) {
  return getDataStaxType(value, cqlDriverStringTypeNames, cqlDriverStringTypes);
}

function getNumericDataStaxTypeName(value) {
  return getDataStaxType(value, cqlDriverNumericTypeNames, cqlDriverNumericTypes);
}

function getDataStaxType(value, typeNames, types) {
  var name, type, i;
  for (i = 0; i < typeNames.length; i++) {
    name = typeNames[i];
    type = types[name];
    if (value instanceof type) {
      return name;
    }
  }
  return null;
}

function changePorts(hostsArray, from, to) {
  return hostsArray.map(function (host) {
    var ix = host.indexOf(':');
    if (ix === -1) {
      return host;
    }

    var hostName = host.substring(0, ix),
        port     = host.substring(ix + 1);

    if (port === from) {
      port = to;
    }

    return hostName + ':' + port;
  });
}

function isNullOrUndefined(value) {
  return value === undefined || value === null;
}
