'use strict';

var _ = require('lodash')
  , util = require('util')
  , uuid = require('uuid')
  , through = require('through2')
  , BaseDriver = require('../base-driver')
  , cqlDriver = require('cassandra-driver')
  , queryParser = require('./query-parser');

function DatastaxDriver() {
  BaseDriver.call(this);
  var consistencies = cqlDriver.types.consistencies;
  this.hostConfigKey = 'contactPoints';
  this.dataType = _.extend(this.dataType, cqlDriver.types.dataTypes);
  this.name = 'datastax';
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
util.inherits(DatastaxDriver, BaseDriver);

module.exports = function dataStaxDriverFactory(context) {
  var driver = new DatastaxDriver();
  driver.init(context);
  return driver;
};
module.exports.DatastaxDriver = DatastaxDriver;

DatastaxDriver.prototype.initProviderOptions = function init(config) {
  setHelenusOptions(config);
  config.supportsPreparedStatements = true;
};

DatastaxDriver.prototype.createConnectionPool = function createConnectionPool(poolConfig, waitForConnect, callback) {
  var self = this
    , openRequestId = uuid.v4()
    , pool;

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
      datastaxLogLevel: level,
      data: data
    };
    self.logger[logMethod]('priam.Driver: ' + message, metaData);
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

DatastaxDriver.prototype.closePool = function closePool(pool, callback) {
  if (pool.isReady && !pool.isClosed) {
    pool.isClosed = true;
    pool.isReady = false;
    pool.shutdown(callback);
  }
  else if (_.isFunction(callback)) {
    pool.isClosed = true;
    pool.isReady = false;
    process.nextTick(callback);
  }
  this.emit('connectionClosed');
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
  var self = this;
  var opts = this.prepareQueryArgs(pool, cqlStatement, params, consistency, options);
  var execOptions = opts.execOptions;
  cqlStatement = opts.cqlStatement;
  params = opts.params;

  var readable = pool.stream(cqlStatement, params, execOptions);
  readable
    .on('error', stream.emit.bind(stream, 'error'))
    .pipe(through.obj(function (record, enc, cb) {
      if (record) {
        var normalized = self.getNormalizedResults([record]);
        record = normalized[0];
        if (Array.isArray(execOptions.resultTransformers)) {
          for (var i = 0; i < execOptions.resultTransformers.length; i++) {
            record = execOptions.resultTransformers[i](record);
          }
        }
      }
      cb(null, record);
    }))
    .on('error', stream.emit.bind(stream, 'error'))
    .pipe(stream);
};

DatastaxDriver.prototype.prepareQueryArgs = function prepareQueryArgs(pool, cqlStatement, params, consistency, options) {
  var self = this;
  var execOptions = _.assign({
    prepare: !!options.executeAsPrepared,
    consistency: consistency
  }, options);

  if (!execOptions.prepare && (pool.controlConnection.protocolVersion < 2)) {
    // Stringify parameters for unprepared statements over binary v1
    cqlStatement = queryParser.parse(cqlStatement, params);
    params = [];
  }

  var hints = [];
  var routingIndexes = [];
  _.forEach(params, function (param, index) {
    if (param && param.hasOwnProperty('value') && param.hasOwnProperty('hint')) {
      params[index] = param.value;
      if (param.hint) {
        if (_.isString(param.hint)) {
          param.hint = self.dataType.getByName(param.hint).type;
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

DatastaxDriver.prototype.getNormalizedResults = function getNormalizedResults(original, options) {
  var self = this;
  var results = _.map(original, function (row) {
    var result = {};
    _.forOwn(row, function (value, name) {
      if ((name === 'columns' || name === '__columns') && _.isObject(value)) { return; } // skip metadata
      if (typeof value === 'string') {
        value = self.checkObjectResult(value, name, options);
      }
      result[name] = value;
    });
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
      }
      else if (typeof val.value === 'string') {
        if (numberRegex.test(val.value)) {
          val.value = new Date(parseInt(val.value, 10)); // string of numbers
        }
        else {
          val.value = new Date(val.value); // assume ISO string
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
    err instanceof cqlDriver.errors.DriverInternalError) ;
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
