'use strict';

var _ = require('lodash')
  , util = require('util')
  , uuid = require('uuid')
  , BaseDriver = require('./base-driver')
  , retryErrors = ['DriverError', 'PoolConnectionError', 'ECONNRESET', 'ENOTFOUND', 'ECONNREFUSED'];

function NodeCassandraDriver(cqlDriver) {
  BaseDriver.call(this);
  this.cqlDriver = cqlDriver;
  this.dataType = _.extend(this.dataType, cqlDriver.types.dataTypes);
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
    EACH_QUORUM: consistencies.eachQuorum,
    eachQuorum: consistencies.eachQuorum,
    ALL: consistencies.all,
    all: consistencies.all,
    ANY: consistencies.any,
    any: consistencies.any
  };
}
util.inherits(NodeCassandraDriver, BaseDriver);

module.exports = function (context) {
  var cqlDriver = require((context && context.config && context.config.driver) || 'node-cassandra-cql');
  var driver = new NodeCassandraDriver(cqlDriver);
  driver.init(context);
  return driver;
};
module.exports.NodeCassandraDriver = NodeCassandraDriver;

NodeCassandraDriver.prototype.initProviderOptions = function init(config) {
  setHelenusOptions(config);
  config.supportsPreparedStatements = true;
};

NodeCassandraDriver.prototype.createConnectionPool = function createConnectionPool(poolConfig, waitForConnect, callback) {
  var self = this
    , openRequestId = uuid.v4()
    , pool;

  self.logger.debug('priam.Driver: Creating new pool', {
    poolConfig: {
      keyspace: poolConfig.keyspace,
      hosts: poolConfig.hosts
    }
  });

  pool = new self.cqlDriver.Client(poolConfig);
  pool.storeConfig = poolConfig;
  pool.waiters = [];
  pool.isReady = false;
  pool.on('log', function (level, message, data) {
    self.emit('connectionLogged', level, message, data);
    if (level === 'debug' || level === 'info') {
      return;
    } // these are trace logs that are too verbose for even our debug logging
    if (level === 'error') {
      level = 'warn';
    } // true errors will yield error on execution
    var metaData = null;

    /* istanbul ignore else: no benefit of testing when there is no metadata */
    if (data) {
      metaData = { data: data };
    }

    self.logger[level]('priam.Driver: ' + message, metaData);
  });
  this.emit('connectionOpening', openRequestId);
  pool.connect(function (err) {
    if (err) {
      self.emit('connectionFailed', openRequestId, err);
      self.logger.error('priam.Driver: Pool Connect Error',
        { name: err.name, code: err.code, error: err.message, stack: err.stack });
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

NodeCassandraDriver.prototype.remapConnectionOptions = function remapConnectionOptions(connectionData) {
  remapOption(connectionData, 'user', 'username');
};

NodeCassandraDriver.prototype.closePool = function closePool(pool, callback) {
  pool.isClosed = true;
  pool.shutdown(callback);
  this.emit('connectionClosed');
};

NodeCassandraDriver.prototype.executeCqlOnDriver = function executeCqlOnDriver(pool, cqlStatement, params, consistency, options, callback) {
  var method = options.executeAsPrepared ? 'executeAsPrepared' : 'execute';
  pool[method](cqlStatement, params, consistency, function (err, data) {
    if (err) {
      return void callback(err);
    }
    var result = (data && data.rows) ? data.rows : [];
    return void callback(null, result);
  });
};

NodeCassandraDriver.prototype.canRetryError = function canRetryError(err) {
  return err && (retryErrors.indexOf(err.name) >= 0 || retryErrors.indexOf(err.code) >= 0);
};

NodeCassandraDriver.prototype.getNormalizedResults = function getNormalizedResults(original, options) {
  var self = this,
    i, j, v, col, row, result, results;

  results = new Array(original.length);
  for (i = 0; i < original.length; i++) {
    row = original[i];
    result = {};
    for (j = 0; j < row.columns.length; j++) {
      col = row.columns[j];
      /* istanbul ignore if: guard statement for something that should never happen */
      if (!col.name) {
        continue;
      }
      v = row[col.name];

      /* istanbul ignore else: not easily tested and no real benefit to doing so */
      if (typeof v !== 'undefined' && v !== null) {
        if (typeof v === 'string') {
          v = self.checkObjectResult(v, col.name, options);
        }
        result[col.name] = v;
      }
    }
    results[i] = result;
  }

  return results;
};

var numberRegex = /^[0-9]+$/;
NodeCassandraDriver.prototype.dataToCql = function dataToCql(val) {
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

    return val; // {value,hint} style parameter -- node-cassandra-cql will handle this internally, do not stringify
  }

  if (!Buffer.isBuffer(val) && (util.isArray(val) || typeof val === 'object')) {
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
  remapOption(config, 'timeout', 'getAConnectionTimeout');
  remapOption(config, 'hostPoolSize', 'poolSize');
  remapOption(config, 'cqlVersion', 'version');
  remapOption(config, 'user', 'username');
}
