const { EventEmitter } = require('events');
const util = require('util');

const _ = require('lodash');
const uuid = require('uuid');
const isStream  = require('isstream');
const retry = require('retry');
const cqlDriver = require('cassandra-driver');

const cqlTypes = require('./util/cql-types');
const fakeLogger = require('./util/fake-logger');
const typeCheck = require('./util/type-check');
const QueryCache = require('./util/query-cache');
const Query = require('./util/query');
const Batch = require('./util/batch');
const peek = require('./util/peek');
const iterateIntoStream = require('./util/iterate-into-stream');

const numberRegex = /^[0-9]+$/;

const retryableResponseErrors = [
  cqlDriver.types.responseErrorCodes.serverError,
  cqlDriver.types.responseErrorCodes.unavailableException,
  cqlDriver.types.responseErrorCodes.overloaded,
  cqlDriver.types.responseErrorCodes.isBootstrapping,
  cqlDriver.types.responseErrorCodes.writeTimeout,
  cqlDriver.types.responseErrorCodes.readTimeout
];

const consistencies = cqlTypes.consistencies;
const retryConsistencyDownlevels = {
  [consistencies.all]: consistencies.eachQuorum,
  [consistencies.eachQuorum]: consistencies.localQuorum,
  [consistencies.quorum]: consistencies.localQuorum
};

class DatastaxDriver extends EventEmitter {
  constructor() {
    super();
    this.name = 'datastax';

    this.consistencyLevel = {};
    this.hostConfigKey = 'contactPoints';

    this.types = cqlTypes.valueTypes;
    this.dataType = _.extend(
      {
        objectAscii: -1,
        objectText: -2
      },
      cqlTypes.dataTypes);

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

  _init(context) {
    if (!context) {
      throw new Error('missing context object');
    }
    if (!context.config) {
      throw new Error('missing context.config object');
    }

    this.logger = context.logger || fakeLogger;
    this.metrics = context.metrics;
    this._initDriverConfig(context.config);

    if (this.config.queryDirectory) {
      this.queryCache = new QueryCache({ queryDirectory: this.config.queryDirectory });
    }

    this._initProviderOptions();
    this.keyspace = this.config.keyspace;
    this._initPoolConfig();
    this._initConnectionResolver(context);

    this.pools = {};
    this.resultTransformers = context.resultTransformers || [];
  }

  _initDriverConfig(config) {
    config = config || {};
    config.retryOptions = config.retryOptions || { retries: 0 };
    const retryDelay = Math.max(config.retryDelay || 100, 0);
    if (config.numRetries) {
      config.retryOptions = {
        retries: Math.max(config.numRetries, 0),
        factor: 1,
        minTimeout: retryDelay,
        maxTimeout: retryDelay,
        randomize: false
      };
    }
    config.supportsPreparedStatements = true;
    this.config = config;
  }

  _initProviderOptions() {
    setHelenusOptions(this.config);
  }

  _initPoolConfig() {
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

    if (typeof this.config.localDataCenter === 'string') {
      this.poolConfig.policies = this.poolConfig.policies || {};
      if (!this.poolConfig.policies.loadBalancing) {
        this.poolConfig.policies.loadBalancing = new cqlDriver.policies.loadBalancing.TokenAwarePolicy(
          new cqlDriver.policies.loadBalancing.DCAwareRoundRobinPolicy(this.config.localDataCenter)
        );
      }
    }
  }

  _initConnectionResolver(context) {
    this.connectionResolver = null;
    if (context.connectionResolver) {
      this.connectionResolver = context.connectionResolver;
    } else if (this.config.connectionResolverPath) {
      this.connectionResolver = require(this.config.connectionResolverPath);
    }
    if (this.connectionResolver) {
      if (typeof this.connectionResolver === 'function') {
        this.connectionResolver = this.connectionResolver();
      }
      /* istanbul ignore else: no benefit of testing event handlers not being wired */
      if (typeof this.connectionResolver.on === 'function') {
        this.connectionResolver.on('fetch', this._connectionResolverFetchHandler.bind(this));
        this.connectionResolver.on('lazyfetch', this._connectionResolverFetchHandler.bind(this));
      }
    }
  }

  async _getConnectionPool(keyspace, waitForConnect) {
    keyspace = keyspace || this.keyspace;
    const poolConfig = await this._getPoolConfig();
    return this._getExistingPool(keyspace, poolConfig, waitForConnect);
  }

  async _getPoolConfig() {
    const emptyConfig = { hosts: [] };
    this._remapConnectionOptions(emptyConfig);
    let poolConfig = _.extend(emptyConfig, this.poolConfig);
    if (this.connectionResolver) {
      const connectionData = await this._resolveConnectionData(poolConfig);
      poolConfig = _.extend(poolConfig, connectionData);
    }
    return poolConfig;
  }

  async _resolveConnectionData(poolConfig) {
    const resolutionRequestId = uuid.v4();
    this.emit('connectionResolving', resolutionRequestId);
    const resolveConnection = util.promisify(this.connectionResolver.resolveConnection.bind(this.connectionResolver));
    try {
      const connectionData = await resolveConnection(this.config);
      this._remapConnectionOptions(connectionData);
      const portMap = poolConfig.connectionResolverPortMap;
      if (portMap && portMap.from && portMap.to) {
        connectionData[this.hostConfigKey] = changePorts(connectionData[this.hostConfigKey], portMap.from, portMap.to);
      }
      this.emit('connectionResolved', resolutionRequestId);
      return connectionData;
    } catch (err) {
      this.emit('connectionResolvedError', resolutionRequestId, err);
      this.logger.error('priam.ConnectionResolver: Error resolving connection options', {
        name: err.name,
        code: err.code,
        error: err.message,
        stack: err.stack
      });
      throw err;
    }
  }

  async _getExistingPool(keyspace, config, waitForConnect) {
    if (keyspace) {
      config = _.extend(config, { keyspace: keyspace });
    }
    let pool = this.pools[keyspace];
    // If config has changed since the last call, close the pool and open a new connection
    if (!pool || pool.isClosed || !_.isEqual(pool.storeConfig, config)) {
      if (pool && !pool.isClosed) {
        await this._closePool(pool);
      }

      pool = await this._createConnectionPool(config, waitForConnect);
      this.pools[keyspace] = pool;
    }

    return pool;
  }

  async _createConnectionPool(poolConfig, waitForConnect) {
    const openRequestId = uuid.v4();

    this.logger.debug('priam.Driver: Creating new pool', {
      poolConfig: {
        keyspace: poolConfig.keyspace,
        contactPoints: poolConfig.contactPoints
      }
    });

    const pool = new cqlDriver.Client(this._getDataStaxPoolConfig(poolConfig));
    pool.storeConfig = poolConfig;
    pool.waiters = [];
    pool.isReady = false;
    pool.on('log', (level, message, data) => {
      this.emit('connectionLogged', level, message, data);
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
      this.logger[logMethod]('priam.Driver.' + message, metaData);
    });

    this.emit('connectionOpening', openRequestId);
    const connect = async () => {
      try {
        await pool.connect();
        pool.isReady = true;
        this.emit('connectionOpened', openRequestId);
        this._callWaiters(null, pool);
      } catch (err) {
        this.emit('connectionFailed', openRequestId, err);
        this.logger.error('priam.Driver: Pool Connect Error',
          { name: err.name, error: err.message, inner: err.innerErrors });
        this._callWaiters(err, pool);
        this._closePool(pool);
        if (waitForConnect) {
          throw err;
        }
      }
    };

    if (waitForConnect) {
      await connect();
    } else {
      setImmediate(connect);
    }

    return pool;
  }

  _getDataStaxPoolConfig(poolConfig) {
    const dsPoolConfig = _.cloneDeep(poolConfig);
    if (dsPoolConfig.username && dsPoolConfig.password) {
      dsPoolConfig.authProvider = new cqlDriver.auth.PlainTextAuthProvider(dsPoolConfig.username, dsPoolConfig.password);
    }
    dsPoolConfig.queryOptions = dsPoolConfig.queryOptions || {};
    dsPoolConfig.queryOptions.fetchSize = dsPoolConfig.limit;
    dsPoolConfig.queryOptions.prepare = false;
    if (dsPoolConfig.consistencyLevel) {
      dsPoolConfig.queryOptions.consistency = dsPoolConfig.consistencyLevel;
    }
    let port = null;
    if (Array.isArray(dsPoolConfig.contactPoints)) {
      for (let i = 0; i < dsPoolConfig.contactPoints.length; i++) {
        const split = dsPoolConfig.contactPoints[i].split(':');
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
    return dsPoolConfig;
  }

  _remapConnectionOptions(connectionData) {
    remapOption(connectionData, 'user', 'username');
    remapOption(connectionData, 'hosts', 'contactPoints');
  }

  async _performConnect(keyspace, waitForConnect) {
    const connectionRequestId = uuid.v4();
    this.emit('connectionRequested', connectionRequestId);

    const pool = await this._getConnectionPool(keyspace, waitForConnect);

    this.emit('connectionAvailable', connectionRequestId);
    return pool;
  }

  _connectionResolverFetchHandler(err, data) {
    if (err) {
      this.emit('connectionOptionsError', err);
      this.logger.warn('priam.ConnectionResolver: Error fetching connection options', { error: err.stack });
    } else {
      this.emit('connectionOptionsFetched', data);
      this.logger.debug('priam.ConnectionResolver: fetched connection options');
    }
  }

  async _closePool(pool) {
    pool.isReady = false;
    if (!pool.isClosed) {
      pool.isClosed = true;
      await pool.shutdown();
    }

    this.emit('connectionClosed');
  }

  _callWaiters(err, pool) {
    pool.waiters.forEach(function (waiter) {
      process.nextTick(function () {
        waiter(err, pool); // pass possible error to waiting queries
      });
    });
    pool.waiters = []; // reset
  }

  _transformRecord(record, options) {
    if (record) {
      record = this._getNormalizedRecord(record, options);
      if (Array.isArray(options.resultTransformers)) {
        for (var i = 0; i < options.resultTransformers.length; i++) {
          record = options.resultTransformers[i](record);
        }
      }
    }
    return record;
  }

  async _executeCqlOnDriver(pool, cqlStatement, params, consistency, options) {
    const result = [];
    for await (const record of this._iterateCqlOnDriver(pool, cqlStatement, params, consistency, options)) {
      result.push(record);
    }
    return result;
  }

  async _streamCqlOnDriver(pool, cqlStatement, params, consistency, options, stream) {
    const iterable = this._iterateCqlOnDriver(pool, cqlStatement, params, consistency, options);
    await iterateIntoStream(iterable, stream);
  }

  async *_iterateCqlOnDriver(pool, cqlStatement, params, consistency, options) {
    const opts = this._prepareQueryArgs(pool, cqlStatement, params, consistency, options);
    const execOptions = opts.execOptions;
    cqlStatement = opts.cqlStatement;
    params = opts.params;
    try {
      for await (const row of pool.stream(cqlStatement, params, execOptions)) {
        yield this._transformRecord(row, execOptions);
      }
    } catch (err) {
      if (err instanceof cqlDriver.errors.ResponseError) {
        err.type = findResponseErrorType(err.code);
      }
      err.query = {
        cql: cqlStatement,
        params: params,
        options: execOptions
      };
      throw err;
    }
  }

  async *_execIterable(cql, dataParams, options) {
    const iterable = await this._execWithRetries(cql, dataParams, options, async (pool, consistency) => {
      // client removes used data entries, so we need to create a copy of the data in case it's needed for retry...
      const innerIterable = this._iterateCqlOnDriver(pool, cql, dataParams.slice(), consistency, options);

      // Fast forward/rewind one item to ensure that query failures are thrown to trigger a retry
      const peeked = await peek(innerIterable, 1);

      return peeked;
    });

    yield* iterable;
  }

  async _execCqlStream(cql, dataParams, options, stream) {
    try {
      const iterable = this._execIterable(cql, dataParams, options);
      await iterateIntoStream(iterable, stream);
    } catch (err) {
      stream.emit('error', err);
    }
  }

  async _execCql(cql, dataParams, options) {
    const result = [];
    const iterable = this._execIterable(cql, dataParams, options);
    for await (const row of iterable) {
      result.push(row);
    }
    return result;
  }

  async _preparePool(options) {
    /* istanbul ignore next: no benefit of testing coalesce for keyspace retrieval */
    const keyspace = (options || {}).keyspace || this.keyspace;
    const pool = await this._performConnect(keyspace, false);
    if (!pool.isReady) {
      await new Promise((resolve, reject) => {
        pool.waiters.push(e => e ? reject(e) : resolve());
      });
    }
    return pool;
  }

  _execWithRetries(cqlQuery, dataParams, options, task) {
    const retryDelay = Math.max(this.config.retryDelay || 100, 0);
    const operation = retry.operation(this.config.retryOptions);
    let consistency = options.consistency
      || this.poolConfig.consistencyLevel
      || this.consistencyLevel.one;
    return new Promise((resolve, reject) => {
      const executeRetryableCql = () => {
        operation.attempt(async currentAttempt => {
          options.consistency = consistency;
          const queryRequestId = uuid.v4();
          try {
            const result = await this._timeQuery(queryRequestId, cqlQuery, dataParams, options, async options => {
              const pool = await this._preparePool(options);
              return task(pool, consistency);
            });
            return void resolve(result);
          } catch (err) {
            if (this._canRetryError(err)) {
              if (operation.retry(err)) {
                this.logger.warn('priam.Cql: Retryable error condition encountered. Executing retry #' + currentAttempt + '...', {
                  name: err.name,
                  code: err.code,
                  error: err.message,
                  stack: err.stack
                });
                return void this.emit('queryRetried', queryRequestId, cqlQuery, dataParams, options);
              }
              const enableConsistencyFailover = this.config.enableConsistencyFailover !== false;
              if (enableConsistencyFailover && (consistency in retryConsistencyDownlevels)) {
                consistency = retryConsistencyDownlevels[consistency];
                return void setTimeout(executeRetryableCql, retryDelay);
              }
            }
            return void reject(err);
          }
        });
      };
      executeRetryableCql();
    });
  }

  _logQuery(cqlQuery, dataParams, options) {
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
      this.logger.debug('priam.cql: executing cql statement', {
        consistencyLevel: options.consistency,
        cql: cqlQuery,
        params: debugParams
      });
    }
  }

  async _timeQuery(queryRequestId, cqlQuery, dataParams, options, act) {
    try {
      const metrics = this.metrics;
      const captureMetrics = !!(metrics && options.queryName);
      const start = captureMetrics && process.hrtime();
      this.emit('queryStarted', queryRequestId, cqlQuery, dataParams, options.consistency, options);
      const result = await act(options);
      this.emit('queryCompleted', queryRequestId);
      if (captureMetrics) {
        var duration = process.hrtime(start);
        duration = (duration[0] * 1e3) + (duration[1] / 1e6);
        metrics.measurement('query.' + options.queryName, duration, 'ms');
      }
      return result;
    } catch (err) {
      this.emit('queryFailed', queryRequestId);
      throw err;
    }
  }

  _prepareQueryArgs(pool, cqlStatement, params, consistency, options) {
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
  }

  _getDriverDataType(type) {
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
  }

  _normalizeParameters(original) {
    if (!original) {
      return [];
    }
    if (util.isArray(original)) {
      for (var i = 0; i < original.length; i++) {
        if (original[i] && original[i].hint) {
          original[i].hint = this._getDriverDataType(original[i].hint);
        }
        original[i] = this._dataToCql(original[i]);
      }
    }
    return original;
  }

  _checkObjectResult(result, fieldName, options) {
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
  }

  _getNormalizedRecord(row, options) {
    var self = this;
    var shouldCoerce = shouldCoerceDataStaxTypes(options, self.config);
    var result = {};
    _.forOwn(row, function (value, name) {
      if ((name === 'columns' || name === '__columns') && _.isObject(value)) {
        return;
      } // skip metadata
      if (typeof value === 'string') {
        value = self._checkObjectResult(value, name, options);
      }
      result[name] = value;
    });
    if (shouldCoerce) {
      coerceDataStaxTypes(result);
    }
    return result;
  }

  _dataToCql(val) {
    if (val && val.hasOwnProperty('value') && val.hasOwnProperty('hint')) {

      // Transform timestamp values into Date objects if number or string
      if (val.hint === this.dataType.timestamp) {
        if (typeof val.value === 'number') {
          val.value = new Date(val.value);
        } else if (typeof val.value === 'string') {
          if (numberRegex.test(val.value)) {
            val.value = new Date(parseInt(val.value, 10)); // string of numbers
          } else {
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
  }

  _canRetryError(err) {
    if (err instanceof cqlDriver.errors.ResponseError) {
      return retryableResponseErrors.indexOf(err.code) !== -1;
    }

    return (err instanceof cqlDriver.errors.NoHostAvailableError ||
    err instanceof cqlDriver.errors.DriverInternalError);
  }

  /*
  cql(cqlQuery, dataParams, [options]: { consistency }, [callback])
  */
  cql(cqlQuery, dataParams, options, callback) {
    ({ options, callback } = checkOptionalParameters(options, callback));
    this._logQuery(cqlQuery, dataParams, options);
    dataParams = this._normalizeParameters(dataParams);

    if (isStream(callback)) {
      // Keep passing the stream down the chain so we are still reusing logic
      return this._execCqlStream(cqlQuery, dataParams, options, callback);
    }

    if (options.iterable) {
      return this._execIterable(cqlQuery, dataParams, options);
    }

    try {
      const promise = this._execCql(cqlQuery, dataParams, options);
      if (!callback) {
        return promise;
      }
      return void promise.then(result => callback(null, result), callback);
    } catch (err) {
      if (callback) {
        return void callback(err);
      }
    }
  }

  /*
  namedQuery(queryName, dataParams, [options]: { consistency }, callback)
  */
  namedQuery(name, dataParams, options, callback) {
    ({ options, callback } = checkOptionalParameters(options, callback));

    if (!this.queryCache) {
      throw new Error('"queryDirectory" option must be set in order to use #namedQuery()');
    }

    const queryText = this.queryCache.readQuery(name);
    options.queryName = options.queryName || name;
    options.executeAsPrepared =
      (this.config.supportsPreparedStatements && (typeof options.executeAsPrepared === 'undefined'))
      || !!options.executeAsPrepared;
    return this.cql(queryText, dataParams, options, callback);
  }

  select(cql, dataParams, options, callback) {
    var params = setDefaultConsistency(this.consistencyLevel.ONE, options, callback);
    this.cql(cql, dataParams, params.options, params.callback);
  }

  insert(cql, dataParams, options, callback) {
    var params = setDefaultConsistency(this.consistencyLevel.LOCAL_QUORUM, options, callback);
    this.cql(cql, dataParams, params.options, params.callback);
  }

  update(cql, dataParams, options, callback) {
    var params = setDefaultConsistency(this.consistencyLevel.LOCAL_QUORUM, options, callback);
    this.cql(cql, dataParams, params.options, params.callback);
  }

  delete(cql, dataParams, options, callback) {
    var params = setDefaultConsistency(this.consistencyLevel.LOCAL_QUORUM, options, callback);
    this.cql(cql, dataParams, params.options, params.callback);
  }

  beginQuery() {
    var self = this;
    var querySettings = {
      options: { consistency: self.poolConfig.consistencyLevel },
      resultTransformers: self.resultTransformers.slice(0)
    };
    return new Query(this, querySettings);
  }

  beginBatch() {
    return new Batch(this);
  }

  param(value, hint, isRoutingKey) {
    if (!hint) {
      return value;
    }

    hint = (hint in this.dataType) ? this.dataType[hint] : hint;
    return { value: value, hint: hint, isRoutingKey: isRoutingKey === true };
  }

  async connect(keyspace, callback) {
    if (typeof keyspace === 'function') {
      callback = keyspace;
      keyspace = null;
    }

    try {
      const pool = await this._performConnect(keyspace, true);
      return void callback(null, pool);
    } catch (err) {
      return void callback(err);
    }
  }

  async close(cb) {
    const pools = [];
    for (const prop in this.pools) {
      /* istanbul ignore else: not easily tested and no real benefit to doing so */
      if (this.pools.hasOwnProperty(prop)) {
        pools.push(this.pools[prop]);
      }
    }

    try {
      await Promise.all(pools.map(pool => this._closePool(pool)));
      return cb && cb();
    } catch (err) {
      if (cb) {
        return void cb(err);
      }

      throw err;
    }
  }
}

DatastaxDriver.prototype.query = DatastaxDriver.prototype.cql;
DatastaxDriver.prototype.isBatch = typeCheck.isBatch;
DatastaxDriver.prototype.isQuery = typeCheck.isQuery;

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
  } else if (!options) {
    options = {};
  }
  return {
    options: options,
    callback: callback
  };
}

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

  var testValue, i;
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
      if (getNumericDataStaxTypeName(testValue)) {
        // Array of numerics
        value = value.map(function (v) {
          if (isNullOrUndefined(v)) { return v; }
          return v.toNumber();
        });
      } else if (getStringDataStaxTypeName(testValue)) {
        // Array of strings
        value = value.map(function (v) {
          if (isNullOrUndefined(v)) { return v; }
          return v.toString();
        });
      }
    }
  } else if (getNumericDataStaxTypeName(value)) {
    // Numeric Types
    return value.toNumber();
  } else if (getStringDataStaxTypeName(value)) {
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
            mappedValue = mappedValue.toNumber();
          }
        }
        transformed[key] = mappedValue;
      }
      return transformed;
    }
  }

  return value;
}

function getStringDataStaxTypeName(value) {
  return getDataStaxType(value, cqlTypes.stringValueTypeNames, cqlTypes.stringValueTypes);
}

function getNumericDataStaxTypeName(value) {
  return getDataStaxType(value, cqlTypes.numericValueTypeNames, cqlTypes.numericValueTypes);
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

module.exports = function dataStaxDriverFactory(context) {
  var driver = new DatastaxDriver();
  driver._init(context);
  return driver;
};
module.exports.DatastaxDriver = DatastaxDriver;
