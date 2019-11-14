const { EventEmitter, once } = require('events');
const util = require('util');
const { finished } = require('stream');

const _ = require('lodash');
const async = require('async');
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

  _getConnectionPool(keyspace, waitForConnect, callback) {
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

        var createConnection = self._createConnectionPool.bind(self, config, waitForConnect, poolCreated);

        if (pool && !pool.isClosed) {
          return void self._closePool(pool, createConnection);
        }

        return void createConnection();
      }

      cb(null, pool);
    }

    var emptyConfig = { hosts: [] };
    self._remapConnectionOptions(emptyConfig);
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

          self._remapConnectionOptions(connectionData);
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
  }

  _createConnectionPool(poolConfig, waitForConnect, callback) {
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
        self._callWaiters(err, pool);
        return void self._closePool(pool);
      }
      pool.isReady = true;
      self.emit('connectionOpened', openRequestId);
      if (waitForConnect) {
        callback(null, pool);
      }
      self._callWaiters(null, pool);
    });
    if (!waitForConnect) {
      callback(null, pool);
    }
  }

  _remapConnectionOptions(connectionData) {
    remapOption(connectionData, 'user', 'username');
    remapOption(connectionData, 'hosts', 'contactPoints');
  }

  connect(keyspace, callback) {
    if (typeof keyspace === 'function') {
      callback = keyspace;
      keyspace = null;
    }

    this._performConnect(keyspace, true, callback);
  }

  _performConnect(keyspace, waitForConnect, callback) {
    /* jshint validthis: true */
    var self                = this,
      connectionRequestId = uuid.v4();

    self.emit('connectionRequested', connectionRequestId);
    self._getConnectionPool(keyspace, waitForConnect, function (err, pool) {
      if (err) {
        callback(err);
        return;
      }

      self.emit('connectionAvailable', connectionRequestId);
      callback(null, pool);
    });
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

  _closePool(pool, callback) {
    if (pool.isReady && !pool.isClosed) {
      pool.isClosed = true;
      pool.isReady = false;
      pool.shutdown(callback);
    } else if (_.isFunction(callback)) {
      pool.isClosed = true;
      pool.isReady = false;
      process.nextTick(callback);
    }
    this.emit('connectionClosed');
  }

  close(cb) {
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
    async.each(pools, this._closePool.bind(this), closed);
  }

  _callWaiters(err, pool) {
    pool.waiters.forEach(function (waiter) {
      process.nextTick(function () {
        waiter(err); // pass possible error to waiting queries
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
  _execCqlStream(cql, dataParams, options, stream) {
    /* istanbul ignore next: no benefit of testing coalesce for keyspace retrieval */
    const keyspace    = (options || {}).keyspace || this.keyspace;

    this._performConnect(keyspace, false, (err, pool) => {
      if (err) {
        return stream.emit(err);
      }

      if (pool.isReady === false) {
        pool.waiters.push(e => {
          if (e) {
            return stream.emit(e);
          }
          this._execCqlStream(cql, dataParams, options, stream);
        });
        return;
      }

      const consistency = options.consistency || this.poolConfig.consistencyLevel || this.consistencyLevel.one;
      this._streamCqlOnDriver(pool, cql, dataParams, consistency, options, stream);
    });
  }

  _execCql(cql, dataParams, options, queryRequestId, callback) {
    /* istanbul ignore next: no benefit of testing coalesce for keyspace retrieval */
    var self        = this,
      keyspace    = (options || {}).keyspace || this.keyspace,
      consistency = options.consistency || self.poolConfig.consistencyLevel || self.consistencyLevel.one;

    this._performConnect(keyspace, false, function (err, pool) {
      if (err) {
        return void callback(err);
      }

      if (pool.isReady === false) {
        pool.waiters.push(function (e) {
          if (e) {
            return void callback(e);
          }
          self._execCql(cql, dataParams, options, queryRequestId, callback);
        });
        return;
      }

      // client removes used data entries, so we need to create a copy of the data in case it's needed for retry...
      var dataCopy       = dataParams.slice(),
        queryRequestId = uuid.v4();
      self.emit('queryStarted', queryRequestId, cql, dataCopy, consistency, options);
      self._executeCqlOnDriver(pool, cql, dataCopy, consistency, options, function (err, results) {

        function execCqlCallback(e, res) {
          if (Array.isArray(res)) {
            res = self._getNormalizedResults(res, options);
          }
          if (e) {
            e.cql = cql;
          }
          callback(e, res);
        }

        execCqlCallback(err, results);
      });
    });
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
