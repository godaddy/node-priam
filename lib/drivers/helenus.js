"use strict";

var
  helenus = require("helenus"),
  _ = require("lodash"),
  util = require("util"),
  uuid = require('node-uuid'),
  BaseDriver = require("./basedriver"),
  nonRetryErrors = ["HelenusInvalidNameError", "HelenusInvalidRequestException"],
  consistencies = helenus.ConsistencyLevel,
  dataTypes = {
    auto: 0x0000,   // Infers from JavaScript type
    uuid: 0x000c    // Wraps in a UUID object
  };

function HelenusDriver() {
  BaseDriver.call(this);
  this.consistencyLevel = {
    ONE: consistencies.ONE,
    one: consistencies.ONE,
    TWO: consistencies.TWO,
    two: consistencies.TWO,
    THREE: consistencies.THREE,
    three: consistencies.THREE,
    QUORUM: consistencies.QUORUM,
    quorum: consistencies.QUORUM,
    LOCAL_QUORUM: consistencies.LOCAL_QUORUM,
    localQuorum: consistencies.LOCAL_QUORUM,
    EACH_QUORUM: consistencies.EACH_QUORUM,
    eachQuorum: consistencies.EACH_QUORUM,
    ALL: consistencies.ALL,
    all: consistencies.ALL,
    ANY: consistencies.ANY,
    any: consistencies.ANY
  };
  this.dataType = _.extend(this.dataType, dataTypes);
}
util.inherits(HelenusDriver, BaseDriver);

exports = module.exports = function (context) {
  var driver = new HelenusDriver();
  driver.init(context);
  return driver;
};
module.exports.HelenusDriver = HelenusDriver;

HelenusDriver.prototype.initProviderOptions = function init(config) {
  this.ConnectionPool = helenus.ConnectionPool;
  config.supportsPreparedStatements = false;
  config.cqlVersion = config.cqlVersion || "3.0.0";
};

HelenusDriver.prototype.createConnectionPool = function createConnectionPool(poolConfig, callback) {
  var self = this;

  // Watch out, poolConfig contains database credentials!
  self.logger.debug("cassandra.creating new pool", { poolConfig: { keyspace: poolConfig.keyspace, hosts: poolConfig.hosts } });
  var pool = new helenus.ConnectionPool(poolConfig);
  pool.storeConfig = poolConfig;
  pool.waiters = [];
  pool.isReady = false;
  pool.on('error', function (err) {
    // error occurred on existing connection
    // close the connection
    self.logger.error("priam.Driver: Connection Error",
      { name: err.name, code: err.code, error: err.message, stack: err.stack });
    self.closePool(pool);
  });
  var openRequestId = uuid.v4();
  this.emit('connectionOpening', openRequestId);
  pool.connect(function (err, keyspace) {
    if (err) {
      self.emit('connectionFailed', openRequestId, err);
      self.logger.error("priam.Driver: Pool Connect Error",
        { name: err.name, code: err.code, error: err.message, stack: err.stack });
      self.callWaiters(pool, err);
      return void self.closePool(pool);
    }
    pool.isReady = true;
    self.emit('connectionOpened', openRequestId);
    self.callWaiters(pool, null);
  });
  pool.monitorConnections();
  callback(pool);
};

HelenusDriver.prototype.closePool = function closePool(pool, callback) {
  // Helenus has a callback for close, but might not call it if there's no active queries. Funny.
  if (typeof callback === "function") {
    pool.once('close', callback);
  }
  pool.isClosed = true;
  pool.close();
  this.emit('connectionClosed');
};

HelenusDriver.prototype.executeCqlOnDriver = function executeCqlOnDriver(pool, cqlStatement, params, consistency, options, callback) {
  pool.cql(cqlStatement, params, callback);
};

HelenusDriver.prototype.canRetryError = function canRetryError(err) {
  return err && (nonRetryErrors.indexOf(err.name) < 0);
};

HelenusDriver.prototype.getNormalizedResults = function getNormalizedResults(original, options) {
  var self = this,
    i, j, v, row, result, results;

  results = new Array(original.length);
  for (i = 0; i < original.length; i++) {
    row = original[i];
    result = {};
    for (j in row._map) {
      /* istanbul ignore else: not easily tested and no real benefit to doing so */
      if (row._map.hasOwnProperty(j)) {
        v = row.get(j);
        /* istanbul ignore else: not easily tested and no real benefit to doing so */
        if (v && v.value) {
          v = v.value;
          if (typeof v === "string") {
            v = self.checkObjectResult(v, j, options);
          }
          result[j] = v;
        }
      }
    }
    results[i] = result;
  }

  return results;
};

HelenusDriver.prototype.dataToCql = function dataToCql(val) {
  if (val && val.hasOwnProperty("value") && val.hasOwnProperty("hint")) {
    // {value,hint} style parameter for node-cassandra-cql driver. Ignore, except for UUIDs.
    switch (val.hint) {
      case dataTypes.uuid:
        return new helenus.UUID(val.value);
      default:
        val = val.value;
        break;
    }
  }

  // TODO: support CQL 3 collection types

  if (Buffer.isBuffer(val)) {
    // this is currently required for blob fields.. effectively a bug/limitation in the Helenus client
    return val.toString('hex');
  } else if (util.isDate(val)) {
    // convert dates to their unix time for storing
    return val.getTime();
  } else if (util.isArray(val) || typeof val === "object") {
    // arrays and objects should be JSON'ized
    return JSON.stringify(val);
  }

  return val; // use as-is
};
