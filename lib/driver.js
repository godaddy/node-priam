const { EventEmitter, once } = require('events');
const util = require('util');
const { finished } = require('stream');

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

const numberRegex = /^[0-9]+$/;

const retryableResponseErrors = [
  cqlDriver.types.responseErrorCodes.serverError,
  cqlDriver.types.responseErrorCodes.unavailableException,
  cqlDriver.types.responseErrorCodes.overloaded,
  cqlDriver.types.responseErrorCodes.isBootstrapping,
  cqlDriver.types.responseErrorCodes.writeTimeout,
  cqlDriver.types.responseErrorCodes.readTimeout
];

const streamFinished = util.promisify(finished);

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

    const consistencies = cqlTypes.consistencies;
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

    /* istanbul ignore next: no benefit of testing coalesce */
    this.config = context.config || {};
    this.logger = context.logger || fakeLogger;
    this.metrics = context.metrics;
    if (context.config.queryDirectory) {
      this.queryCache = new QueryCache({ queryDirectory: context.config.queryDirectory });
    }

    this._initProviderOptions(this.config);
    this.keyspace = context.config.keyspace;
    this._initPoolConfig();
    this._initConnectionResolver(context);

    this.pools = {};
    this.resultTransformers = context.resultTransformers || [];
  }

  _initProviderOptions(config) {
    setHelenusOptions(config);
    config.supportsPreparedStatements = true;
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

    const emptyConfig = { hosts: [] };
    this._remapConnectionOptions(emptyConfig);
    let poolConfig = _.extend(emptyConfig, this.poolConfig);
    if (this.connectionResolver) {
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

        poolConfig = _.extend(poolConfig, connectionData);
        this.emit('connectionResolved', resolutionRequestId);
      } catch (err) {
        this.emit('connectionResolvedError', resolutionRequestId, err);
        this.logger.error('priam.ConnectionResolver: Error resolving connection options',
          { name: err.name, code: err.code, error: err.message, stack: err.stack });
        throw err;
      }
    }

    return this._getExistingPool(keyspace, poolConfig, waitForConnect);
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
    if (pool.isReady && !pool.isClosed) {
      pool.isClosed = true;
      pool.isReady = false;
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
      var normalized = this._getNormalizedResults([record]);
      record = normalized[0];
      if (Array.isArray(options.resultTransformers)) {
        for (var i = 0; i < options.resultTransformers.length; i++) {
          record = options.resultTransformers[i](record);
        }
      }
    }
    return record;
  }

  _executeCqlOnDriver(pool, cqlStatement, params, consistency, options, callback) {
    var opts = this._prepareQueryArgs(pool, cqlStatement, params, consistency, options);
    var execOptions = opts.execOptions;
    cqlStatement = opts.cqlStatement;
    params = opts.params;
    pool.execute(cqlStatement, params, execOptions, function (err, data) {
      if (err) {
        if (err instanceof cqlDriver.errors.ResponseError) {
          err.type = findResponseErrorType(err.code);
        }
        err.query = {
          cql: cqlStatement,
          params: params,
          options: execOptions
        };
        return void callback(err);
      }
      var result = (data && data.rows) ? data.rows : [];
      return void callback(null, result);
    });
  }

  async _streamCqlOnDriver(pool, cqlStatement, params, consistency, options, stream) {
    for await (const chunk of this._iterateCqlOnDriver(pool, cqlStatement, params, consistency, options)) {
      if (!stream.write(chunk))
        await once(stream, 'drain');
    }

    stream.end();
    await streamFinished(stream);
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
      err.query = {
        cql: cqlStatement,
        params: params,
        options: execOptions
      };
      throw err;
    }
  }

  // TODO: we need to have some sort of incremental retry logic but we will leave
  // this up to the consumer in this case as its a very special cased type thing.
  // We can potentially make it generic in some way later on
  async _execCqlStream(cql, dataParams, options, stream) {
    const todo = (e, pool) => {
      if (e) {
        return void stream.emit(e);
      }

      const consistency = options.consistency || this.poolConfig.consistencyLevel || this.consistencyLevel.one;
      this._streamCqlOnDriver(pool, cql, dataParams, consistency, options, stream);
    };

    /* istanbul ignore next: no benefit of testing coalesce for keyspace retrieval */
    const keyspace = (options || {}).keyspace || this.keyspace;
    try {
      const pool = await this._performConnect(keyspace, false);
      if (pool.isReady) {
        return void todo(null, pool);
      }
      return void pool.waiters.push(todo);
    } catch (err) {
      return void todo(err);
    }
  }

  async _execCql(cql, dataParams, options, queryRequestId, callback) {
    const todo = (e, pool) => {
      if (e) {
        return void callback(e);
      }

      // client removes used data entries, so we need to create a copy of the data in case it's needed for retry...
      const dataCopy = dataParams.slice();
      queryRequestId = uuid.v4();
      const consistency = options.consistency || this.poolConfig.consistencyLevel || this.consistencyLevel.one;
      this.emit('queryStarted', queryRequestId, cql, dataCopy, consistency, options);
      this._executeCqlOnDriver(pool, cql, dataCopy, consistency, options, (err, results) => {
        if (Array.isArray(results)) {
          results = this._getNormalizedResults(results, options);
        }
        if (err) {
          err.cql = cql;
        }
        callback(err, results);
      });
    };

    /* istanbul ignore next: no benefit of testing coalesce for keyspace retrieval */
    const keyspace = (options || {}).keyspace || this.keyspace;
    try {
      const pool = await this._performConnect(keyspace, false);
      if (pool.isReady) {
        return void todo(null, pool);
      }
      return void pool.waiters.push(todo);
    } catch (err) {
      return void todo(err);
    }
  }

  _execCqlWithRetries(cqlQuery, dataParams, options, callback) {
    const retryOptions = { retries: 0 };
    this.config.retryOptions = this.config.retryOptions || retryOptions;
    const retryDelay = Math.max(this.config.retryDelay || 100, 0);
    if (this.config.numRetries) {
      this.config.retryOptions = {
        retries: Math.max(this.config.numRetries, 0),
        factor: 1,
        minTimeout: retryDelay,
        maxTimeout: retryDelay,
        randomize: false
      };
    }

    const consistency = options.consistency || this.poolConfig.consistencyLevel || this.consistencyLevel.one;
    const operation = retry.operation(this.config.retryOptions);

    const metrics = this.metrics;
    const captureMetrics = !!(metrics && options.queryName);
    const start = captureMetrics && process.hrtime();

    const executeRetryableCql = () => {
      operation.attempt(currentAttempt => {
        const queryRequestId = uuid.v4();
        this._execCql(cqlQuery, dataParams, options, queryRequestId, (err, result) => {
          const canRetryErrorType = !!err && this._canRetryError(err);
          const enableConsistencyFailover = (this.config.enableConsistencyFailover !== false);
          if (canRetryErrorType) {
            if (operation.retry(err)) {
              this.logger.warn('priam.Cql: Retryable error condition encountered. Executing retry #' + currentAttempt + '...', { name: err.name, code: err.code, error: err.message, stack: err.stack });
              this.emit('queryRetried', queryRequestId, cqlQuery, dataParams, options);
              return;
            }

            if (enableConsistencyFailover) {
              // Fallback from all to localQuorum via additional retries
              if (err && consistency === this.consistencyLevel.all) {
                options.consistency = this.consistencyLevel.eachQuorum;
                return void setTimeout(executeRetryableCql, retryDelay);
              }
              if (err && (consistency === this.consistencyLevel.eachQuorum ||
                consistency === this.consistencyLevel.quorum)) {
                options.consistency = this.consistencyLevel.localQuorum;
                return void setTimeout(executeRetryableCql, retryDelay);
              }
            }
          }

          // retries are complete. lets fallback on quorum if needed
          this.emit(err ? 'queryFailed' : 'queryCompleted', queryRequestId);

          // all retries are finally complete
          if (captureMetrics) {
            var duration = process.hrtime(start);
            duration = (duration[0] * 1e3) + (duration[1] / 1e6);
            metrics.measurement('query.' + options.queryName, duration, 'ms');
          }

          if (typeof callback === 'function') {
            if (result && result.length && options.resultTransformers) {
              for (var i = 0; i < options.resultTransformers.length; i++) {
                result = result.map(options.resultTransformers[i]);
              }
            }

            callback(err, result);
          }
        });
      });
    };
    executeRetryableCql();
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

  _getNormalizedResults(original, options) {
    var self = this;
    var shouldCoerce = shouldCoerceDataStaxTypes(options, self.config);
    var results = _.map(original, function (row) {
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
    });
    return results;
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
    dataParams = this._normalizeParameters(dataParams);

    // Keep passing the stream down the chain so we are still reusing logic
    if (isStream(callback)) {
      return this._execCqlStream(cqlQuery, dataParams, options, callback);
    }
    return this._execCqlWithRetries(cqlQuery, dataParams, options, callback);
  }

  /*
  namedQuery(queryName, dataParams, [options]: { consistency }, callback)
  */
  namedQuery(name, dataParams, options, callback) {
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
        err.query = {
          queryName: name,
          params: dataParams,
          options: options
        };
        return void callback(err);
      }
      options.queryName = options.queryName || name;
      options.executeAsPrepared = self.config.supportsPreparedStatements &&
      (typeof options.executeAsPrepared === 'undefined') ? true : !!options.executeAsPrepared;
      self.cql(queryText, dataParams, options, callback);
    });
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
