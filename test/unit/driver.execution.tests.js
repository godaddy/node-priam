const path             = require('path');
const { EventEmitter } = require('events');
const sinon            = require('sinon');
const chai             = require('chai');
const _                = require('lodash');
const cql              = require('cassandra-driver');
const FakeResolver     = require('../stubs/fake-resolver');
const Driver           = require('../../lib/driver');

chai.use(require('sinon-chai'));
const assert = chai.assert;
const expect = chai.expect;

describe('lib/driver.js', function () {

  function getDefaultConfig() {
    return {
      protocolOptions: {
        maxVersion: '3.1.0'
      },
      contactPoints: ['123.456.789.012:9042'],
      keyspace: 'myKeySpace',
      limit: 5000
    };
  }

  function getDefaultLogger() {
    return {
      debug: sinon.stub(),
      info: sinon.stub(),
      warn: sinon.stub(),
      error: sinon.stub(),
      critical: sinon.stub()
    };
  }

  function getDefaultInstance() {
    return new Driver({
      config: getDefaultConfig(),
      logger: getDefaultLogger()
    });
  }

  function getPoolStub(config, isReady, err, data) {
    const rows = (data || {}).rows || [];
    var storeConfig = {
      consistencyLevel: 1,
      protocolOptions: { maxVersion: '3.1.0' },
      ...config
    };
    return Object.assign(new EventEmitter(), {
      storeConfig: storeConfig,
      isReady: isReady,
      execute: sinon.stub().returns(err ? Promise.reject(err) : Promise.resolve(data)),
      stream: sinon.spy(function *() {
        if (err) {
          throw err;
        } else {
          yield* rows;
        }
      }),
      batch: sinon.stub().yields(err, data),
      connect: sinon.stub().resolves({}),
      shutdown: sinon.stub().resolves(),
      controlConnection: {
        protocolVersion: 2
      }
    });
  }

  describe('DatastaxDriver#connect()', function () {

    var instance;

    beforeEach(function () {
      instance = getDefaultInstance();
    });

    afterEach(function () {
      if (cql.Client.restore) {
        cql.Client.restore();
      }
    });

    it('returns the connection pool on successful connection', function (done) {
      // arrange
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().resolves({});
      sinon.stub(cql, 'Client').returns(pool);

      // act
      instance.connect(function (err, newPool) {
        // assert
        assert.notOk(err, 'error should not be passed');
        assert.equal(newPool, pool, 'pool should be passed');
        assert.strictEqual(pool.isReady, true, 'pool should be ready');

        done();
      });
    });

    it('returns error if pool fails to connect', function (done) {
      // arrange
      var pool = getPoolStub(instance.config, true, null, {});
      var error = new Error('connection failed');
      pool.on = sinon.stub();
      pool.connect = sinon.stub().rejects(error);
      sinon.stub(cql, 'Client').returns(pool);

      // act
      instance.connect(function (err, pool) {
        // assert
        assert.notOk(pool, 'pool should not be populated');
        assert.equal(err, error, 'error should be populated');

        done();
      });
    });

    it('supports returning a pool via Promise', async () => {
      // arrange
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().resolves({});
      sinon.stub(cql, 'Client').returns(pool);

      // act
      const newPool = await instance.connect();

      // assert
      assert.equal(newPool, pool, 'pool should be passed');
      assert.strictEqual(pool.isReady, true, 'pool should be ready');
    });

    it('handles errors via Promises', async () => {
      // arrange
      var pool = getPoolStub(instance.config, true, null, {});
      var error = new Error('connection failed');
      pool.on = sinon.stub();
      pool.connect = sinon.stub().rejects(error);
      sinon.stub(cql, 'Client').returns(pool);

      // act
      let caughtError;
      try {
        await instance.connect();
      } catch (err) {
        caughtError = err;
      }

      // assert
      assert.equal(caughtError, error, 'error should be populated');
    });

    it('allows the load balancing policy to be overridden', done => {
      const mockPolicy = {
        loadBalancing: Symbol()
      };
      const driver = new Driver({
        config: {
          localDataCenter: 'fakeDC',
          policies: mockPolicy
        }
      });
      var pool = getPoolStub(instance.config, true, null, {});
      sinon.stub(cql, 'Client').returns(pool);

      driver.connect(err => {
        expect(err).to.not.exist;
        const poolOpts = cql.Client.lastCall.args[0];
        expect(poolOpts.policies).to.deep.equal(mockPolicy);
        done();
      });
    });
  });

  describe('DatastaxDriver#createConnectionPool()', function () {

    var instance;

    beforeEach(function () {
      instance = getDefaultInstance();
    });

    afterEach(function () {
      if (cql.Client.restore) {
        cql.Client.restore();
      }
    });

    it('returns the connection pool on successful connection if "waitForConnect" is true', async () => {
      // arrange
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().resolves({});
      sinon.stub(cql, 'Client').returns(pool);

      // act
      const newPool = await instance._createConnectionPool({}, true);

      // assert
      assert.equal(newPool, pool, 'pool should be passed');
      assert.strictEqual(pool.isReady, true, 'pool should be ready');
    });

    it('generates appropriate configuration structure for socket options', async () => {
      // arrange
      instance.config.socketOptions = { connectTimeout: 15000 };
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().resolves({});
      sinon.stub(cql, 'Client').returns(pool);

      // act
      const newPool = await instance._createConnectionPool(instance.config, true);

      // assert
      assert.equal(newPool, pool, 'pool should be passed');
      assert.ok(cql.Client.calledWithMatch({
        socketOptions: {
          connectTimeout: 15000
        }
      }), 'socketOptions should be set');
    });

    it('uses configured pooling options', async () => {
      // arrange
      const pooling = {
        coreConnectionsPerHost: {
          0: 5,
          1: 3
        }
      };
      instance.config.pooling = pooling;
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().resolves({});
      sinon.stub(cql, 'Client').returns(pool);

      // act
      const newPool = await instance._createConnectionPool(instance.config, true);

      // assert
      assert.equal(newPool, pool, 'pool should be passed');
      assert.ok(cql.Client.calledWithMatch({ pooling }), 'pooling should be set');
    });

    it('returns the connection pool immediately if "waitForConnect" is false', async () => {
      // arrange
      var pool = getPoolStub(instance.config, false, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub()
        .resolves(new Promise(resolve => setTimeout(resolve, 10)));
      sinon.stub(cql, 'Client').returns(pool);

      // act
      const newPool = await instance._createConnectionPool({}, false);

      // assert
      assert.equal(newPool, pool, 'pool should be passed');
      assert.strictEqual(pool.isReady, false, 'pool should NOT be ready');
    });

  });

  describe('DatastaxDriver#close()', function () {

    var pool, instance;
    beforeEach(function () {
      instance = getDefaultInstance();
      pool = getPoolStub(instance.config, true, null, {});
      instance.pools = { myKeySpace: pool };
      instance.getConnectionPool = sinon.stub().yields(null, {});
    });

    it('closes the connection pool if it exists', function (done) {
      // arrange
      instance.on('connectionClosed', setTimeout.bind(null, done, 4));
      pool.isReady = true;
      pool.isClosed = false;

      // act
      instance.close(function () {
        // assert
        assert.strictEqual(pool.isClosed, true, 'pool should be set to closed');
        assert.strictEqual(pool.isReady, false, 'pool should not be marked ready');
        assert.ok(pool.shutdown.called, 'pool shutdown should be called');
      });
    });

    it('skips closing the pool if it is already closed', function (done) {
      // arrange
      instance.on('connectionClosed', setTimeout.bind(null, done, 4));
      pool.isReady = true;
      pool.isClosed = true;

      // act
      instance.close(function () {
        // assert
        assert.strictEqual(pool.isClosed, true, 'pool should be set to closed');
        assert.strictEqual(pool.isReady, false, 'pool should not be marked ready');
        assert.notOk(pool.shutdown.called, 'pool shutdown should not be called');
      });
    });

    it('just calls callback if pool does not yet exist', function (done) {
      // arrange
      instance.pools = {};

      // act
      instance.close(done);
    });

    it('propagates pool closing errors', done => {
      // arrange
      const error = new Error('Please, just 5 more minutes?');
      pool.isReady = true;
      pool.isClosed = false;
      pool.shutdown = sinon.stub().rejects(error);

      // act
      instance.close(function (err) {
        // assert
        expect(err).to.equal(error);
        done();
      });
    });

    it('supports Promises', async () => {
      // arrange
      const error = new Error('Please, just 5 more minutes?');
      pool.isReady = true;
      pool.isClosed = false;
      pool.shutdown = sinon.stub().rejects(error);

      // act
      let caughtError;
      try {
        await instance.close();
      } catch (err) {
        caughtError = err;
      }

      // assert
      expect(caughtError).to.equal(error);
    });

  });

  describe('DatastaxDriver#cql()', function () {

    let instance;
    let fakeResolver;

    beforeEach(function () {
      fakeResolver = new FakeResolver();
      instance = getDefaultInstance();
    });

    afterEach(function () {
      if (cql.Client.restore) {
        cql.Client.restore();
      }
    });

    it('emits a connectionRequested event at the beginning of a query', function () {
      var eventHandler = sinon.stub();
      instance.on('connectionRequested', eventHandler);
      instance.cql('select foo from bar');

      expect(eventHandler).to.have.been.called;
    });

    it('creates a new connection pool if one does not exist', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().resolves({});

      sinon.stub(cql, 'Client').returns(pool);
      instance.pools = {};
      var connectionOpeningHandler = sinon.stub(),
        connectionOpenedHandler  = sinon.stub(),
        queryStartedHandler      = sinon.stub();
      instance
        .on('connectionOpening', connectionOpeningHandler)
        .on('connectionOpened', connectionOpenedHandler)
        .on('queryStarted', queryStartedHandler);

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function () {
        // assert
        expect(connectionOpeningHandler).to.have.been.called;
        expect(connectionOpenedHandler).to.have.been.called;
        expect(queryStartedHandler).to.have.been.called;
        assert.equal(instance.pools.myKeySpace, pool, 'pool should be cached');
        assert.strictEqual(pool.waiters.length, 0, 'waiters should be executed after connection completes');
        assert.strictEqual(pool.stream.called, true, 'cql statements should execute after connection completes');

        done();
      });

      // before callback asserts
      assert.strictEqual(pool.stream.called, false, 'cql statements should wait to execute until after connection completes');
    });

    it('creates a new connection pool if pool is closed', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().resolves({});
      sinon.stub(cql, 'Client').returns(pool);
      var existingPool = getPoolStub(instance.config, true, null, {});
      existingPool.isClosed = true;
      instance.pools = { myKeySpace: existingPool };

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function () {
        // assert
        assert.notOk(existingPool.execute.called, 'existing pool should not be called');
        assert.ok(pool.stream.called, 'new pool should be called');

        done();
      });
    });

    it('adds a new connection pool if keyspace is different', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(_.extend(_.extend({}, instance.config), { keyspace: 'myNewKeyspace' }), true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().resolves({});
      sinon.stub(cql, 'Client').returns(pool);

      var existingPool = getPoolStub(_.extend({}, instance.config), true, null, {});
      instance.pools = { myKeySpace: existingPool };

      // act
      instance.cql(cqlQuery, params, { consistency: consistency, keyspace: 'myNewKeyspace' }, function () {
        // assert
        assert.notOk(existingPool.execute.called, 'existing pool should not be called');
        assert.ok(pool.stream.called, 'new pool should be called');

        done();
      });
    });

    it('uses default connection pool if supplied keyspace matches default', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(_.extend(_.extend({}, instance.config), { keyspace: instance.config.keyspace }), true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().yieldsAsync(null, {});
      sinon.stub(cql, 'Client').returns(pool);
      var existingPool = getPoolStub(_.extend({}, instance.config), true, null, {});
      instance.pools = { myKeySpace: existingPool };
      var openingHandler = sinon.stub(), openedHandler = sinon.stub(), availableHandler = sinon.stub();
      instance
        .on('connectionOpening', openingHandler)
        .on('connectionOpened', openedHandler)
        .on('connectionAvailable', availableHandler);

      // act
      instance.cql(cqlQuery, params, { consistency: consistency, keyspace: instance.config.keyspace }, function () {
        // assert
        expect(openingHandler).to.have.not.been.called;
        expect(openedHandler).to.have.not.been.called;
        expect(availableHandler).to.have.been.called;
        assert.notOk(pool.stream.called, 'new pool should not be called');
        assert.ok(existingPool.stream.called, 'existing pool should be called');

        done();
      });
    });

    it('creates a new connection pool if config no longer matches', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub({ different: 'config', keyspace: instance.config.keyspace }, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().resolves({});
      sinon.stub(cql, 'Client').returns(pool);
      var existingPool = getPoolStub({ keyspace: instance.config.keyspace }, true, null, {});
      instance.pools = { myKeySpace: existingPool };

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function () {
        // assert
        assert.notEqual(instance.pools.myKeySpace, existingPool, 'existing pool should be replaced');
        assert.equal(instance.pools.myKeySpace, pool, 'pool should be cached');
        assert.strictEqual(pool.waiters.length, 0, 'waiters should be executed after connection completes');
        assert.strictEqual(pool.stream.called, true, 'cql statements should execute after connection completes');

        done();
      });

      // before callback asserts
      assert.strictEqual(pool.stream.called, false, 'cql statements should wait to execute until after connection completes');
    });

    function testLogEvent(logLevel, expectedLevel, errorData, done) {
      // setup arrange
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().yieldsAsync(null, {});
      sinon.stub(cql, 'Client').returns(pool);
      instance.pools = {};
      var logEventHandler = sinon.stub();
      instance.on('connectionLogged', logEventHandler);

      // setup act
      instance.cql('cql', [], { consistency: consistency }, function () {
        // setup assert
        assert.ok(pool.on.calledWith('log', sinon.match.func), 'log handler should be wired up');

        // handler arrange
        var errorCb = pool.on.getCall(0).args[1];
        instance.logger = {
          debug: sinon.stub(),
          info: sinon.stub(),
          warn: sinon.stub(),
          error: sinon.stub(),
          critical: sinon.stub()
        };

        // handler act
        var message  = 'Error',
          metaData = errorData;
        errorCb(logLevel, message, metaData);

        // handler assert
        var expectedMessage = message;
        var expectedData = {
          datastaxLogLevel: logLevel,
          data: errorData
        };
        if (typeof errorData === 'string') {
          expectedMessage = `${message}: ${errorData}`;
          delete expectedData.data;
        }
        expect(logEventHandler).to.have.been.calledWith(logLevel, message, metaData);
        if (expectedLevel) {
          assert.strictEqual(instance.logger[expectedLevel].args[0][0], `priam.Driver.${expectedMessage}`);
          assert.deepEqual(instance.logger[expectedLevel].args[0][1], expectedData);
        } else {
          assert.notOk(instance.logger.info.called);
          assert.notOk(instance.logger.warn.called);
          assert.notOk(instance.logger.error.called);
        }
      });

      done();
    }

    it('sets up a global trace handler for the connection pool - logs debug level as debug', function (done) {
      testLogEvent('verbose', 'debug', 'myMessage', done);
    });

    it('sets up a global trace handler for the connection pool - logs info level as debug', function (done) {
      testLogEvent('info', 'debug', 'myMessage', done);
    });

    it('sets up a global error handler for the connection pool - logs warning as warn', function (done) {
      testLogEvent('warning', 'warn', 'myMessage', done);
    });

    it('sets up a global error handler for the connection pool - logs warning object as warn', function (done) {
      testLogEvent('warning', 'warn', { some: 'metaData' }, done);
    });

    it('sets up a global error handler for the connection pool - logs error as warn', function (done) {
      testLogEvent('error', 'warn', 'myMessage', done);
    });

    it('sets up an error handler for pool.connect', function (done) {
      // arrange
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().rejects(new Error('Connection pool failed to connect'));
      sinon.stub(cql, 'Client').returns(pool);
      instance.pools = {};
      instance.logger = {
        debug: sinon.stub(),
        error: sinon.stub()
      };
      var connectionFailedHandler = sinon.stub();
      instance.on('connectionFailed', connectionFailedHandler);

      // act
      instance.cql('cql', [], { consistency: consistency }, function (err, result) {
        // assert
        assert.instanceOf(err, Error);
        assert.isUndefined(result);
        assert.ok(instance.logger.error.calledOnce, 'error log is called once');
        expect(connectionFailedHandler).to.have.been.calledWithMatch(sinon.match.string, err);

        done();
      });
    });

    it('executes queued queries when connection completes', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().resolves();
      sinon.stub(cql, 'Client').returns(pool);
      instance.pools = {};

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function () {
        // assert
        assert.strictEqual(pool.isReady, true, 'pool should be set to true after connection completes');
        assert.strictEqual(pool.stream.called, true, 'cql statements should execute after connection completes');

        done();
      });
    });

    it('allows callback to be optional to support fire-and-forget scenarios', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var pool = getPoolStub(instance.config, true, null, []);
      instance.pools = { myKeySpace: pool };

      pool.stream = sinon.spy(function *(cql) {
        var call = pool.stream.getCall(0);

        // assert
        assert.strictEqual(call.args[0], cql, 'cql should be passed through');
        assert.deepEqual(call.args[1], params, 'params should be passed through');

        yield* [];
        done();
      });

      // act
      instance.cql(cqlQuery, params);
    });

    it('uses default consistency of ONE if no options are passed', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, []);
      pool.on = sinon.stub();
      pool.connect = sinon.stub().resolves();
      sinon.stub(cql, 'Client').returns(pool);
      instance.pools = {};

      // act
      instance.cql(cqlQuery, params, function () {
        var ctorCall = cql.Client.getCall(0);

        // assert
        assert.strictEqual(ctorCall.args[0].consistencyLevel, consistency, 'consistency should be ONE');

        done();
      });
    });

    it('executes CQL and returns the data', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3', new Buffer('param4')];
      var consistency = cql.types.consistencies.quorum;
      var err = null;
      var data = {
        rows: [
          {
            field1: '12345'
          }, {
            field1: null
          }, {
            field1: undefined
          }
        ]
      };
      var pool = getPoolStub(instance.config, true, err, data);
      instance.pools = { myKeySpace: pool };
      var completedHandler = sinon.stub();
      instance.on('queryCompleted', completedHandler);

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function (error, returnData) {
        var call = pool.stream.getCall(0);

        // assert
        assert.strictEqual(call.args[0], cqlQuery, 'cql should be passed through');
        assert.deepEqual(call.args[1], params, 'params should be passed through');
        assert.strictEqual(call.args[2].prepare, false, 'prepare option should be false');
        assert.strictEqual(call.args[2].consistency, consistency, 'consistency should be passed through');
        assert.isNull(error, 'error should be null');
        assert.deepEqual(returnData, [
          { field1: '12345' }, { field1: null }, { field1: undefined }
        ], 'data should match normalized cql output');
        expect(completedHandler).to.have.been.called;

        done();
      });
    });

    it('returns a `Promise` if no callback or stream is supplied', async () => {
      const rows = [
        { field1: '12345' },
        { field1: null },
        { field1: undefined }
      ];
      instance.pools = {
        myKeySpace: getPoolStub(instance.config, true, null, { rows })
      };

      const result = await instance.cql('SELECT * FROM foo', []);

      expect(result).to.deep.equal(rows);
    });

    describe('when the "iterable" option is set to true', () => {
      const rows = [
        { field1: '12345' },
        { field1: null },
        { field1: undefined }
      ];

      beforeEach(() => {
        instance = getDefaultInstance();
        instance.pools = {
          myKeySpace: getPoolStub(instance.config, true, null, { rows })
        };
      });

      it('returns an async iterable', async () => {
        const result = await iterateIntoArray(instance.cql('SELECT * FROM foo', [], { iterable: true }));
        expect(result).to.deep.equal(rows);
      });

      it('performs retries within the async iterable', async () => {
        instance._initDriverConfig({
          ...getDefaultConfig(),
          numRetries: 2,
          retryDelay: 1
        });

        const pool = instance.pools.myKeySpace;
        let callCount = 0;
        pool.stream = sinon.spy(() => {
          if (!callCount++) {
            throw new cql.errors.ResponseError(0x1200, 'error message');
          } else {
            return rows;
          }
        });

        const result = await iterateIntoArray(instance.cql('SELECT * FROM foo', [], { iterable: true }));

        expect(callCount).to.equal(2);
        expect(result).to.deep.equal(rows);
      });

      async function iterateIntoArray(iterable) {
        const result = [];
        for await (const row of iterable) {
          result.push(row);
        }
        return result;
      }
    });

    it('handles null parameters', function (done) {
      // arrange
      var cqlStatement = 'INSERT INTO foo (id, some_column) VALUES (?, ?)';
      var params = [1, null];
      var pool = getPoolStub(instance.config, true, null, null);
      instance.pools = { myKeySpace: pool };

      // act
      instance.cql(cqlStatement, params, { consistency: cql.types.consistencies.one }, function () {
        // assert
        expect(pool.stream).to.have.been.calledWithMatch(cqlStatement, sinon.match([1, 'null']));

        done();
      });
    });

    it('executes CQL as prepared statement and returns the data if "executeAsPrepared" option specified', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = [{ value: 'param1', hint: 'ascii' }, 'param2', 'param3', new Buffer('param4')];
      var consistency = cql.types.consistencies.quorum;
      var err = null;
      var data = {
        rows: [
          {
            columns: [
              { name: 'field1', types: [1, null] }
            ],
            field1: 'value1'
          }
        ]
      };
      var pool = getPoolStub(instance.config, true, err, data);
      instance.pools = { myKeySpace: pool };

      // act
      instance.cql(cqlQuery, params, {
        consistency: consistency,
        executeAsPrepared: true
      }, function (error, returnData) {
        var call = pool.stream.getCall(0);

        // assert
        assert.strictEqual(call.args[0], cqlQuery, 'cql should be passed through');
        assert.strictEqual(call.args[2].prepare, true, 'prepare option should be true');
        assert.strictEqual(call.args[2].consistency, consistency, 'consistency should be passed through');
        assert.isNull(error, 'error should be null');
        assert.deepEqual(returnData, [
          { field1: 'value1' }
        ], 'data should match normalized cql output');

        done();
      });
    });

    it('executes CQL with hint options if parameters provide type hints', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = [
        'param1',
        { value: 'param2', hint: null },
        { value: 'param3', hint: instance.dataType.ascii },
        { value: 'param4', hint: 'map<text,boolean>' },
        { value: 'param5', hint: 'int', isRoutingKey: true },
        new Buffer('param6')];
      var consistency = cql.types.consistencies.quorum;
      var err = null;
      var data = {
        rows: [
          {
            columns: [
              { name: 'field1', types: [1, null] }
            ],
            field1: 'value1'
          }
        ]
      };
      var pool = getPoolStub(instance.config, true, err, data);
      instance.pools = { myKeySpace: pool };

      // act
      instance.cql(cqlQuery, params, {
        consistency: consistency,
        executeAsPrepared: true
      }, function (error, returnData) {
        var call = pool.stream.getCall(0);

        // assert
        assert.strictEqual(call.args[0], cqlQuery, 'cql should be passed through');
        assert.deepEqual(call.args[1], [
          params[0],
          params[1].value,
          params[2].value,
          params[3].value,
          params[4].value,
          params[5]
        ], 'param values should be passed through');
        assert.strictEqual(call.args[2].prepare, true, 'prepare option should be true');
        assert.strictEqual(call.args[2].consistency, consistency, 'consistency should be passed through');
        assert.deepEqual(call.args[2].routingIndexes, [4], 'routingIndexes should be generated');
        var expectedHints = [];
        expectedHints[2] = instance.dataType.ascii;
        expectedHints[3] = {
          code: 33,
          info: [
            { code: 10, info: null },
            { code: 4, info: null }
          ]
        };
        expectedHints[4] = {
          code: 9,
          info: null
        };
        assert.deepEqual(call.args[2].hints, expectedHints, 'hints should be passed through');

        assert.isNull(error, 'error should be null');
        assert.deepEqual(returnData, [
          { field1: 'value1' }
        ], 'data should match normalized cql output');

        done();
      });
    });

    function testDataTypeTransformation(configValue, optionValue, nullOptions, shouldCoerce, done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.one;
      var err = null;
      var data = {
        rows: [
          {
            columns: [
              // Numeric types
              { name: 'field1', types: [2, null] },
              { name: 'field2', types: [6, null] },
              { name: 'field3', types: [14, null] },
              // String types
              { name: 'field4', types: [12, null] },
              { name: 'field5', types: [15, null] },
              { name: 'field6', types: [16, null] },
              // Map types
              { name: 'field7', types: [33, null] },
              { name: 'field8', types: [33, null] },
              { name: 'field9', types: [33, null] },
              { name: 'field10', types: [33, null] },
              { name: 'field11', types: [33, null] },
              { name: 'field17', types: [33, null] },
              // Set types
              { name: 'field12', types: [34, null] },
              { name: 'field13', types: [34, null] },
              { name: 'field14', types: [34, null] },
              { name: 'field15', types: [34, null] },
              { name: 'field16', types: [34, null] }
            ],
            field1: cql.types.Long.fromNumber(12345),
            field2: new cql.types.BigDecimal(12345, 2),
            field3: cql.types.Integer.fromNumber(54321),
            field4: cql.types.Uuid.random(),
            field5: cql.types.TimeUuid.now(),
            field6: cql.types.InetAddress.fromString('127.0.0.1'),
            field7: { nullKey: null, key: cql.types.Long.fromNumber(12345) },
            field8: { nullKey: null, key: cql.types.Uuid.random() },
            field9: { nullKey: null, key: 'value' },
            field10: {},
            field11: { nullKey: null },
            field12: [null, cql.types.Long.fromNumber(12345)],
            field13: [null, cql.types.Uuid.random()],
            field14: [null, 'value'],
            field15: [],
            field16: [null],
            field17: { nullKey: null, key1: true, key2: false }
          }
        ]
      };

      var pool = getPoolStub(instance.config, true, err, data);
      instance.pools = { myKeySpace: pool };
      instance.config.coerceDataStaxTypes = configValue;

      // act
      instance.cql(cqlQuery, params, nullOptions ? null : {
        consistency: consistency,
        coerceDataStaxTypes: optionValue,
        resultHint: {
          field1: instance.dataType.bigint,
          field2: instance.dataType.decimal,
          field3: instance.dataType.varint,
          field4: instance.dataType.uuid,
          field5: instance.dataType.timeuuid,
          field6: instance.dataType.inet,
          field7: instance.dataType.map,
          field8: instance.dataType.map,
          field9: instance.dataType.map,
          field10: instance.dataType.map,
          field11: instance.dataType.map,
          field12: instance.dataType.set,
          field13: instance.dataType.set,
          field14: instance.dataType.set,
          field15: instance.dataType.set,
          field16: instance.dataType.set,
          field17: instance.dataType.set
        }
      }, function (error, returnData) {
        if (error) { return void done(error); }

        var call = pool.stream.getCall(0);
        assert.strictEqual(call.args[0], cqlQuery, 'cql should be passed through');
        assert.deepEqual(call.args[1], params, 'params should be passed through');
        var record = returnData[0];
        if (shouldCoerce) {
          // Standard types
          assert.strictEqual(typeof record.field1, 'number', 'first field should be a number');
          assert.strictEqual(record.field1, 12345, 'first field value should match');
          assert.strictEqual(typeof record.field2, 'number', 'second field should be a number');
          assert.strictEqual(record.field2, 123.45, 'second field value should match');
          assert.strictEqual(typeof record.field3, 'number', 'third field should be a number');
          assert.strictEqual(record.field3, 54321, 'first field value should match');
          assert.strictEqual(typeof record.field4, 'string', 'fourth field should be an string');
          assert.strictEqual(record.field4, data.rows[0].field4.toString(), 'fourth field value should match');
          assert.strictEqual(typeof record.field5, 'string', 'fifth field should be a string');
          assert.strictEqual(record.field5, data.rows[0].field5.toString(), 'fifth field value should match');
          assert.strictEqual(typeof record.field6, 'string', 'sixth field should be a string');
          assert.strictEqual(record.field6, data.rows[0].field6.toString(), 'sixth field value should match');

          // Maps
          assert.strictEqual(typeof record.field7.key, 'number', 'seventh field key value should be a number');
          assert.strictEqual(record.field7.key, 12345, 'seventh field key value should match');
          assert.isNull(record.field7.nullKey, 'seventh field nullkey should be null');
          assert.strictEqual(typeof record.field8.key, 'string', 'eighth field key value should be a string');
          assert.strictEqual(record.field8.key, data.rows[0].field8.key.toString(), 'eighth field key value should match');
          assert.isNull(record.field8.nullKey, 'eighth field nullkey should be null');
          assert.strictEqual(typeof record.field9.key, 'string', 'ninth field key value should be a string');
          assert.strictEqual(record.field9.key, 'value', 'ninth field key value should match');
          assert.isNull(record.field9.nullKey, 'ninth field nullkey should be null');
          assert.isTrue(Object.keys(record.field10).length === 0, 'tenth field should be an empty object');
          assert.isTrue(Object.keys(record.field11).length === 1, 'eleventh field should contain only nullKey');
          assert.isNull(record.field11.nullKey, 'eleventh field nullkey should be null');
          assert.isNull(record.field17.nullKey, '17th field nullkey should be null');
          assert.strictEqual(record.field17.key1, true, '17th field1 should be true');
          assert.strictEqual(record.field17.key2, false, '17th field1 should be false');

          // Sets
          assert.isNull(record.field12[0], 'twelfth field null index should be null');
          assert.strictEqual(typeof record.field12[1], 'number', 'twelfth field index value should be a number');
          assert.strictEqual(record.field12[1], 12345, 'twelfth field index value should match');
          assert.isNull(record.field13[0], 'thirteenth field null index should be null');
          assert.strictEqual(typeof record.field13[1], 'string', 'thirteenth field index value should be a string');
          assert.strictEqual(record.field13[1], data.rows[0].field13[1].toString(), 'thirteenth field index value should match');
          assert.isNull(record.field14[0], 'thirteenth field null index should be null');
          assert.strictEqual(typeof record.field14[1], 'string', 'fourteenth field index value should be a string');
          assert.strictEqual(record.field14[1], 'value', 'fourteenth field index value should match');
          assert.isTrue(record.field15.length === 0, 'fifteenth field should be an empty array');
          assert.isTrue(record.field16.length === 1, 'sixteenth field should contain only null');
          assert.isNull(record.field16[0], 'sixteenth field null index should be null');
        } else {
          assert.isTrue(record.field1 instanceof cql.types.Long, 'first field should not be transformed');
          assert.isTrue(record.field2 instanceof cql.types.BigDecimal, 'second field should not be transformed');
          assert.isTrue(record.field3 instanceof cql.types.Integer, 'third field should not be transformed');
          assert.isTrue(record.field4 instanceof cql.types.Uuid, 'fourth field should not be transformed');
          assert.isTrue(record.field5 instanceof cql.types.TimeUuid, 'fifth field should not be transformed');
          assert.isTrue(record.field6 instanceof cql.types.InetAddress, 'sixth field should not be transformed');
          assert.isTrue(record.field7.key instanceof cql.types.Long, 'seventh field key value should not be transformed');
          assert.isTrue(record.field8.key instanceof cql.types.Uuid, 'eighth field key value should not be transformed');
          assert.isTrue(typeof record.field9.key === 'string', 'ninth field key value should not be transformed');
          assert.isTrue(Object.keys(record.field10).length === 0, 'tenth field should be an empty object');
          assert.isTrue(Object.keys(record.field11).length === 1, 'eleventh field should contain only nullKey');
          assert.isTrue(record.field12[1] instanceof cql.types.Long, 'twelfth field index value should not be transformed');
          assert.isTrue(record.field13[1] instanceof cql.types.Uuid, 'thirteenth field index value should not be transformed');
          assert.isTrue(typeof record.field14[1] === 'string', 'fourteenth field index value should not be transformed');
          assert.isTrue(record.field15.length === 0, 'fifteenth field should be an empty array');
          assert.isTrue(record.field16.length === 1, 'sixteenth field should not be transformed');
        }

        done();
      });
    }

    it('coerces DataStax custom types back to strings and numbers if coerceDataStaxTypes is not set in options or config', function (done) {
      testDataTypeTransformation(undefined, undefined, false, true, done);
    });

    it('coerces DataStax custom types back to strings and numbers if coerceDataStaxTypes is not set in config and options are not passed', function (done) {
      testDataTypeTransformation(undefined, undefined, true, true, done);
    });

    it('coerces DataStax custom types back to strings and numbers if coerceDataStaxTypes is set to true in options but not in config', function (done) {
      testDataTypeTransformation(undefined, true, false, true, done);
    });

    it('coerces DataStax custom types back to strings and numbers if coerceDataStaxTypes is set to true in config but not in options', function (done) {
      testDataTypeTransformation(true, undefined, false, true, done);
    });

    it('coerces DataStax custom types back to strings and numbers if coerceDataStaxTypes is set to true in options but false in config', function (done) {
      testDataTypeTransformation(false, true, false, true, done);
    });

    it('does not coerce DataStax custom types back to strings and numbers if coerceDataStaxTypes is set to false in options but not in config', function (done) {
      testDataTypeTransformation(undefined, false, false, false, done);
    });

    it('does not coerce DataStax custom types back to strings and numbers if coerceDataStaxTypes is set to false in config but not in options', function (done) {
      testDataTypeTransformation(false, undefined, false, false, done);
    });

    it('does not coerce DataStax custom types back to strings and numbers if coerceDataStaxTypes is set to false in options but true in config', function (done) {
      testDataTypeTransformation(true, false, false, false, done);
    });

    it('normalizes/deserializes the data in the resulting array', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.one;
      var err = null;
      var data = {
        rows: [
          {
            columns: [
              { name: 'field1', types: [1, null] },
              { name: 'field2', types: [1, null] },
              { name: 'field3', types: [1, null] },
              { name: 'field4', types: [1, null] },
              { name: 'field5', types: [1, null] },
              { name: 'field6', types: [1, null] },
              { name: 'field7', types: [1, null] }
            ],
            field1: 'value1',
            field2: 2,
            field3: '{ "subField1": "blah" }',
            field4: '[ 4, 3, 2, 1]',
            field5: '{ some invalid json }',
            field6: false,
            field7: '{ "jsonThat": "iDontWantToParse" }'
          }
        ]
      };

      var pool = getPoolStub(instance.config, true, err, data);
      instance.pools = { myKeySpace: pool };

      // act
      instance.cql(cqlQuery, params, {
        consistency: consistency,
        resultHint: {
          field1: instance.dataType.ascii,
          field2: instance.dataType.number,
          field3: instance.dataType.objectAscii,
          field4: instance.dataType.objectText,
          field5: instance.dataType.objectAscii,
          field6: instance.dataType.boolean
          // field7 intentionally omitted
        }
      }, function (error, returnData) {
        var call = pool.stream.getCall(0);

        // assert
        assert.strictEqual(call.args[0], cqlQuery, 'cql should be passed through');
        assert.deepEqual(call.args[1], params, 'params should be passed through');
        assert.isNull(error, 'error should be null');
        assert.strictEqual(returnData[0].field1, 'value1', 'first field should be a string');
        assert.strictEqual(returnData[0].field2, 2, 'second field should be a number');
        assert.deepEqual(returnData[0].field3, { subField1: 'blah' }, 'third field should be an object');
        assert.deepEqual(returnData[0].field4, [4, 3, 2, 1], 'fourth field should be an array');
        assert.deepEqual(returnData[0].field5, '{ some invalid json }', 'fifth field should be a string');
        assert.strictEqual(returnData[0].field6, false, 'sixth field should be false');
        assert.deepEqual(returnData[0].field7, '{ "jsonThat": "iDontWantToParse" }', 'seventh field should be a string');

        done();
      });
    });

    it('normalizes/deserializes the data in the resulting array by detecting JSON strings', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.one;
      var err = null;
      var data = {
        rows: [
          {
            columns: [
              { name: 'field1', types: [1, null] },
              { name: 'field2', types: [1, null] },
              { name: 'field3', types: [1, null] },
              { name: 'field4', types: [1, null] },
              { name: 'field5', types: [1, null] }
            ],
            field1: 'value1',
            field2: 2,
            field3: '{ "subField1": "blah" }',
            field4: '[ 4, 3, 2, 1]',
            field5: '{ some invalid json }'
          }
        ]
      };

      var pool = getPoolStub(instance.config, true, err, data);
      instance.pools = { myKeySpace: pool };

      // act
      instance.cql(cqlQuery, params, {
        consistency: consistency,
        deserializeJsonStrings: true
      }, function (error, returnData) {
        var call = pool.stream.getCall(0);

        // assert
        assert.strictEqual(call.args[0], cqlQuery, 'cql should be passed through');
        assert.deepEqual(call.args[1], params, 'params should be passed through');
        assert.isNull(error, 'error should be null');
        assert.strictEqual(returnData[0].field1, 'value1', 'first field should be a string');
        assert.strictEqual(returnData[0].field2, 2, 'second field should be a number');
        assert.deepEqual(returnData[0].field3, { subField1: 'blah' }, 'third field should be an object');
        assert.deepEqual(returnData[0].field4, [4, 3, 2, 1], 'fourth field should be an array');
        assert.deepEqual(returnData[0].field5, '{ some invalid json }', 'fifth field should be a string');

        done();
      });
    });

    function testErrorRetry(errorName, errorCode, numRetries, shouldRetry) {
      it(`${shouldRetry ? 'adds' : 'does not add'} error retry if error is "${errorName}", code "${errorCode}", and retries ${numRetries}`, function (done) {
        // arrange
        var cqlQuery = 'MyCqlStatement';
        var params = ['param1', 'param2', 'param3'];
        var consistency = cql.types.consistencies.one;
        var pool = getPoolStub(instance.config, true, null, {});
        var data = [];
        var callCount = 0;
        pool.stream = sinon.spy(async function *() {
          callCount++;
          if (callCount === 1) {
            throw new cql.errors[errorName](errorCode, 'error message');
          } else {
            yield* data;
          }
        });
        instance.pools = { myKeySpace: pool };
        instance._initDriverConfig({ numRetries, retryDelay: 1 });

        // act
        instance.cql(cqlQuery, params, { consistency: consistency }, function (error, returnData) {
          // assert
          if (shouldRetry) {
            var call1 = pool.stream.getCall(0);
            var call2 = pool.stream.getCall(1);
            assert.strictEqual(pool.stream.callCount, 2, 'execute should be called twice');
            assert.notEqual(call1.args[1], call2.args[1], 'parameters should be cloned');
            assert.deepEqual(call1.args[1], call2.args[1], 'parameters should be cloned');
            assert.deepEqual(returnData, data, 'data should match cql output');
          } else {
            assert.strictEqual(pool.stream.callCount, 1, 'execute should be called once');
            assert.instanceOf(error, Error, 'error is not populated');
            assert.isObject(error.query, 'query is not defined');
            assert.equal(error.query.cql, cqlQuery, 'query.cql does not match query');
            assert.instanceOf(error.query.params, Array, 'query.params is not an array');
            assert.deepEqual(error.query.options, {
              consistency: consistency,
              prepare: false
            }, 'query.options does not match query options');
            assert.isUndefined(returnData, 'returnData not defined');
          }

          done();
        });
      });
    }

    testErrorRetry('ResponseError', 0x1200, 0, false); // readTimeout
    testErrorRetry('ResponseError', 0x1200, 1, true);
    testErrorRetry('ResponseError', 0x2000, 1, false); // syntaxError
    testErrorRetry('NoHostAvailableError', null, 1, true);
    testErrorRetry('DriverInternalError', null, 1, true);
    testErrorRetry('AuthenticationError', 0x1200, 1, false);

    it('does not add error retry at consistency QUORUM when original consistency is ALL and enableConsistencyFailover is false', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.all;
      var pool = getPoolStub(instance.config, true, null, {});
      var data = [];
      var callCount = 0;
      pool.stream = sinon.spy(function *() {
        callCount++;
        if (callCount === 1) {
          throw new Error('throws error on ALL');
        } else {
          yield* data;
        }
      });
      instance.pools = { myKeySpace: pool };
      instance.config.retryDelay = 1;
      instance.config.enableConsistencyFailover = false;

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function (error, returnData) {
        // assert
        assert.strictEqual(pool.stream.callCount, 1, 'execute should be called once');
        assert.ok(error);
        assert.equal(error.query.cql, cqlQuery);
        assert.notOk(returnData);

        done();
      });
    });

    it('adds error retry at consistency QUORUM when original consistency is ALL', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.all;
      var pool = getPoolStub(instance.config, true, null, {});
      var data = [];
      var callCount = 0;
      pool.stream = sinon.spy(async function *() {
        callCount++;
        if (callCount === 1) {
          throw new cql.errors.ResponseError(0x1200, 'timeout on read');
        } else {
          yield* data;
        }
      });
      instance.pools = { myKeySpace: pool };
      instance._initDriverConfig({ retryDelay: 1 });

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function (error, returnData) {
        var call1 = pool.stream.getCall(0);
        var call2 = pool.stream.getCall(1);
        // assert
        assert.strictEqual(pool.stream.callCount, 2, 'cql should be called twice');
        assert.notEqual(call1.args[1], call2.args[1], 'parameters should be cloned');
        assert.deepEqual(call1.args[1], call2.args[1], 'parameters should be cloned');
        assert.deepEqual(returnData, data, 'data should match cql output');

        done();
      });
    });

    it('adds error retry at consistency LOCUM_QUORUM when original consistency is QUORUM', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.quorum;
      var pool = getPoolStub(instance.config, true, null, {});
      var data = [];
      var callCount = 0;
      pool.stream = sinon.spy(async function *() {
        callCount++;
        if (callCount === 1) {
          throw new cql.errors.ResponseError(0x1200, 'timeout on read');
        } else {
          yield* data;
        }
      });
      instance.pools = { myKeySpace: pool };
      instance.config.retryDelay = 1;

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function (error, returnData) {
        var call1 = pool.stream.getCall(0);
        var call2 = pool.stream.getCall(1);
        // assert
        assert.strictEqual(pool.stream.callCount, 2, 'cql should be called twice');
        assert.notEqual(call1.args[1], call2.args[1], 'parameters should be cloned');
        assert.deepEqual(call1.args[1], call2.args[1], 'parameters should be cloned');
        assert.deepEqual(returnData, data, 'data should match cql output');

        done();
      });
    });

    it('does not error retry at consistency LOCUM_QUORUM when original consistency is QUORUM if error is not retryable', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.quorum;
      var pool = getPoolStub(instance.config, true, null, {});
      var data = [];
      var callCount = 0;
      var err = new cql.errors.ResponseError(1234, 'something blew up');
      pool.stream = sinon.spy(async function *() {
        callCount++;
        if (callCount === 1) {
          throw err;
        } else {
          yield* data;
        }
      });
      instance.pools = { myKeySpace: pool };
      instance.config.retryDelay = 1;

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function (error) {
        // assert
        assert.strictEqual(pool.stream.callCount, 1, 'cql should be called once');
        assert.equal(error, err);
        assert.equal(error.query.cql, cqlQuery);
        done();
      });
    });

    it('emits a queryFailed event when a query fails', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.quorum;
      var pool = getPoolStub(instance.config, true, null, {});
      // eslint-disable-next-line require-yield
      pool.stream = sinon.spy(function *() {
        throw new Error('throws error on QUORUM');
      });
      instance.pools = { myKeySpace: pool };
      var failedHandler = sinon.stub();
      instance.on('queryFailed', failedHandler);

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function () {
        expect(failedHandler).to.have.been.called;
        done();
      });
    });

    it('captures metrics if metrics and queryName are provided', function (done) {
      // arrange
      var cqlQuery = 'MyCqlStatement';
      var queryName = 'MyQueryName';
      var params = ['param1', 'param2', 'param3'];
      var consistency = cql.types.consistencies.one;
      var err = null;
      var data = { field1: 'value1' };
      var pool = getPoolStub(instance.config, true, err, data);
      instance.pools = { myKeySpace: pool };
      instance.metrics = {
        measurement: sinon.stub()
      };

      // act
      instance.cql(cqlQuery, params, { consistency: consistency, queryName: queryName }, function () {
        var call = instance.metrics.measurement.getCall(0);

        // assert
        assert.ok(instance.metrics.measurement.calledOnce, 'measurement called once');
        assert.strictEqual(call.args[0], `query.${queryName}`, 'measurement called with appropriate query name');

        done();
      });
    });

    it('can apply transformations to the results', async () => {
      const rows = [
        { foo: 1 },
        { foo: 2 },
        { foo: 3 }
      ];
      instance.pools = {
        myKeySpace: getPoolStub(instance.config, true, null, { rows })
      };

      const result = await instance.cql('SELECT * FROM foo', [], {
        resultTransformers: [
          x => x.foo * 2,
          x => '*'.repeat(x)
        ]
      });

      expect(result).to.deep.equal([
        '**',
        '****',
        '******'
      ]);
    });

    describe('with connection resolver', function () {

      let logger;

      const resolverOptions = {
        config: {
          connectionResolverPath: '../test/stubs/fake-resolver',
          cqlVersion: '3.1.0'
        }
      };

      function getResolverInstance(context) {
        logger = {
          debug: sinon.stub(),
          info: sinon.stub(),
          warn: sinon.stub(),
          error: sinon.stub()
        };
        return new Driver({ config: getDefaultConfig(), logger: logger, ...context });
      }

      beforeEach(function () {
        instance = null;
        fakeResolver.resolveConnection = sinon.stub().yieldsAsync(null, {});
      });

      it('uses supplied connection resolver to override base config', function (done) {
        // arrange
        const cqlQuery = 'MyCqlStatement';
        const params = ['param1', 'param2', 'param3'];
        const consistency = cql.types.consistencies.one;
        instance = getResolverInstance({ connectionResolver: fakeResolver });
        const fakeConnectionInfo = {
          username: 'myResolvedUsername',
          password: 'myResolvedPassword',
          contactPoints: []
        };
        fakeResolver.resolveConnection = sinon.stub().yieldsAsync(null, fakeConnectionInfo);
        const pool = getPoolStub(instance.config, true, null, {});
        pool.on = sinon.stub();
        pool.connect = sinon.stub().resolves({});
        sinon.stub(cql, 'Client').returns(pool);
        instance.pools = {};

        // act
        instance.cql(cqlQuery, params, { consistency }, function () {
          // assert
          assert.strictEqual(pool.storeConfig.credentials.username, fakeConnectionInfo.username);
          assert.strictEqual(pool.storeConfig.credentials.password, fakeConnectionInfo.password);

          done();
        });
      });

      it('uses resolved connection resolver from path to override base config', function (done) {
        // arrange
        const cqlQuery = 'MyCqlStatement';
        const params = ['param1', 'param2', 'param3'];
        const consistency = cql.types.consistencies.one;
        instance = getResolverInstance(resolverOptions);
        const userName = 'myResolvedUsername';
        const fakeConnectionInfo = {
          username: userName,
          password: 'myResolvedPassword',
          contactPoints: ['123.456.789.012:1234']
        };
        instance.connectionResolver.resolveConnection = sinon.stub().yieldsAsync(null, _.cloneDeep(fakeConnectionInfo));
        var pool = getPoolStub(instance.config, true, null, {});
        pool.on = sinon.stub();
        pool.connect = sinon.stub().resolves({});
        sinon.stub(cql, 'Client').returns(pool);
        instance.pools = {};

        // act
        instance.cql(cqlQuery, params, { consistency }, function () {
          // assert
          assert.strictEqual(pool.storeConfig.credentials.username, userName);
          assert.strictEqual(pool.storeConfig.credentials.password, fakeConnectionInfo.password);
          assert.deepEqual(pool.storeConfig.contactPoints, fakeConnectionInfo.contactPoints);

          done();
        });
      });

      it('applies port remapping to resolved connection information if specified', function (done) {
        // arrange
        const cqlQuery = 'MyCqlStatement';
        const params = ['param1', 'param2', 'param3'];
        const consistency = cql.types.consistencies.one;
        instance = getResolverInstance(resolverOptions);
        instance.config.connectionResolverPortMap = {
          from: '1234',
          to: '2345'
        };
        instance.poolConfig.connectionResolverPortMap = instance.config.connectionResolverPortMap;
        const userName = 'myResolvedUsername';
        const fakeConnectionInfo = {
          user: userName,
          password: 'myResolvedPassword',
          contactPoints: ['123.456.789.012:1234', '234.567.890.123', '235.235.4.3:8888']
        };
        instance.connectionResolver.resolveConnection = sinon.stub().yieldsAsync(null, _.cloneDeep(fakeConnectionInfo));
        const pool = getPoolStub(instance.config, true, null, {});
        pool.on = sinon.stub();
        pool.connect = sinon.stub().resolves({});
        sinon.stub(cql, 'Client').returns(pool);
        instance.pools = {};

        // act
        instance.cql(cqlQuery, params, { consistency }, function () {
          // assert
          assert.strictEqual(pool.storeConfig.credentials.username, userName);
          assert.strictEqual(pool.storeConfig.credentials.password, fakeConnectionInfo.password);
          assert.deepEqual(
            pool.storeConfig.contactPoints,
            ['123.456.789.012:2345', '234.567.890.123', '235.235.4.3:8888'],
            'contactPoints were applied with remapped ports');
          done();
        });
      });

      it('logs and returns error if connection resolver throws error', function (done) {
        // arrange
        var cqlQuery = 'MyCqlStatement';
        var params = ['param1', 'param2', 'param3'];
        var consistency = cql.types.consistencies.one;
        instance = getResolverInstance({ connectionResolver: fakeResolver });
        fakeResolver.resolveConnection = sinon.stub().yieldsAsync(new Error('connection resolution failed'));
        var pool = getPoolStub(instance.config, true, null, {});
        pool.on = sinon.stub();
        pool.connect = sinon.stub().yieldsAsync(null, {});
        sinon.stub(cql, 'Client').returns(pool);
        instance.pools = {};
        var connectionResolvedErrorHandler = sinon.stub();
        instance.on('connectionResolvedError', connectionResolvedErrorHandler);

        // act
        instance.cql(cqlQuery, params, { consistency: consistency }, function (err, result) {
          // assert
          assert.instanceOf(err, Error);
          assert.isUndefined(result);
          assert.ok(logger.error.calledOnce, 'error log is called once');
          expect(connectionResolvedErrorHandler).to.have.been.calledWithMatch(sinon.match.string, err);

          //                    setTimeout(function () {
          //                        assert.notOk(logger.error.calledTwice, 'error logger should only be called once');
          //
          //                        done();
          //                    }, 5);
          done();
        });
      });

      it('returns data and logs error if connection resolver throws error on lazy fetch', function (done) {
        // arrange
        const cqlQuery = 'MyCqlStatement';
        const params = ['param1', 'param2', 'param3'];
        const consistency = cql.types.consistencies.one;
        instance = getResolverInstance({ connectionResolver: fakeResolver });
        const fakeConnectionInfo = {
          username: 'myResolvedUsername',
          password: 'myResolvedPassword',
          contactPoints: []
        };
        const fetchError = new Error('lazy fetch error');
        fakeResolver.resolveConnection = function (data, cb) {
          fakeResolver.on.getCall(1).args[1](fetchError);
          cb(null, fakeConnectionInfo);
        };
        const pool = getPoolStub(instance.config, true, null, {});
        pool.on = sinon.stub();
        pool.connect = sinon.stub().resolves({});
        sinon.stub(cql, 'Client').returns(pool);
        instance.pools = {};
        const connectionOptionsErrorHandler = sinon.stub();
        instance.on('connectionOptionsError', connectionOptionsErrorHandler);

        // act
        instance.cql(cqlQuery, params, { consistency: consistency }, function () {
          // assert
          assert.strictEqual(pool.storeConfig.credentials.username, fakeConnectionInfo.username, 'username successfully updated');
          assert.strictEqual(pool.storeConfig.credentials.password, fakeConnectionInfo.password, 'password successfully updated');
          expect(connectionOptionsErrorHandler).to.have.been.calledWith(fetchError);
          done();
        });
      });

      it('returns data if connection resolver successfully performs a lazy fetch', function (done) {
        // arrange
        const cqlQuery = 'MyCqlStatement';
        const params = ['param1', 'param2', 'param3'];
        const consistency = cql.types.consistencies.one;
        instance = getResolverInstance({ connectionResolver: fakeResolver });
        const fakeConnectionInfo = {
          username: 'myResolvedUsername',
          password: 'myResolvedPassword',
          contactPoints: []
        };
        fakeResolver.resolveConnection = function (data, cb) {
          cb(null, fakeConnectionInfo);
          fakeResolver.on.getCall(1).args[1](null, { user: 'someOtherInfo', password: 'someOtherPassword' });
        };
        const pool = getPoolStub(instance.config, true, null, {});
        pool.on = sinon.stub();
        pool.connect = sinon.stub().resolves({});
        sinon.stub(cql, 'Client').returns(pool);
        instance.pools = {};
        const connOptionsFetchedHandler = sinon.stub();
        const connResolvedHandler = sinon.stub();
        instance.on('connectionOptionsFetched', connOptionsFetchedHandler);
        instance.on('connectionResolved', connResolvedHandler);

        // act
        instance.cql(cqlQuery, params, { consistency: consistency }, function () {
          // assert
          assert.strictEqual(pool.storeConfig.credentials.username, fakeConnectionInfo.username, 'username successfully updated');
          assert.strictEqual(pool.storeConfig.credentials.password, fakeConnectionInfo.password, 'password successfully updated');
          assert.notOk(logger.warn.called, 'warn logger should not be called');
          expect(connOptionsFetchedHandler).to.have.been.called;
          expect(connResolvedHandler).to.have.been.called;
          done();
        });
      });

      it('allows the connection resolver to not be an event emitter', () => {
        getResolverInstance({
          connectionResolver: {
            resolveConnection(cb) {
              cb(null, {
                username: 'bob',
                password: 'blah',
                contactPoints: ['1.2.3.4']
              });
            }
          }
        });
      });
    });
  });

  describe('crud wrappers', function () {

    var instance;
    beforeEach(function () {
      instance = getDefaultInstance();
      sinon.stub(instance, '_execCql').resolves({});
      sinon.stub(cql, 'Client').returns({});
    });

    afterEach(function () {
      instance._execCql.restore();
      cql.Client.restore();
    });

    function validateWrapperCall(method, consistencyLevel) {
      describe(`DatastaxDriver#${method}()`, function () {
        it('normalizes the parameter list if it is an array', function (done) {
          // arrange
          var dt = new Date();
          var buffer = new Buffer(4096);
          var hinted = { value: 'bar', hint: 1 /* ascii*/ };
          buffer.write('This is a string buffer', 'utf-8');
          var params = [1, 'myString', dt, [1, 2, 3, 4], { myObjectKey: 'value' }, buffer, hinted];

          // act
          instance[method]('cql', params, {}, function () {
            var call = instance._execCql.getCall(0);
            var normalized = call.args[1];

            // assert
            assert.strictEqual(normalized[0], params[0], '1st parameter should be a number');
            assert.strictEqual(normalized[1], params[1], '2nd parameter should be a string');
            assert.strictEqual(normalized[2], JSON.stringify(dt), '3rd parameter should be date converted to string');
            assert.strictEqual(normalized[3], '[1,2,3,4]', '4th parameter should be array converted to JSON');
            assert.strictEqual(normalized[4], '{"myObjectKey":"value"}', '5th parameter should be object converted to JSON');
            assert.ok(Buffer.isBuffer(normalized[5]), '6th parameter should be buffer');
            assert.deepEqual(normalized[6], hinted, '7th parameter should remain hinted object parameter');

            done();
          });
        });

        it('normalizes the timestamp parameters appropriately', function (done) {
          // arrange
          var timeStampType = 11;
          var hintedDateTimestamp = { value: new Date(8675309), hint: timeStampType };
          var hintedNumberTimestamp = { value: 8675309, hint: timeStampType };
          var hintedNumberStringTimestamp = { value: '8675309', hint: timeStampType };
          var hintedIsoStringTimestamp = { value: '1970-01-01T02:24:35.309Z', hint: timeStampType };
          var hintedNullTimestamp = { value: null, hint: timeStampType };
          var params = [
            hintedDateTimestamp,
            hintedNumberTimestamp,
            hintedNumberStringTimestamp,
            hintedIsoStringTimestamp,
            hintedNullTimestamp
          ];

          // act
          instance[method]('cql', params, {}, function () {
            var call = instance._execCql.getCall(0);
            var normalized = call.args[1];

            // assert
            assert.deepEqual(normalized[0], hintedDateTimestamp, 'Date parameter should remain hinted Date parameter');
            assert.deepEqual(normalized[1], hintedDateTimestamp, 'Number parameter should be converted to hinted Date parameter');
            assert.deepEqual(normalized[2], hintedDateTimestamp, 'Numeric String parameter should be converted to hinted Date parameter');
            assert.deepEqual(normalized[3], hintedDateTimestamp, 'ISO Date String parameter should be converted to hinted Date parameter');
            assert.deepEqual(normalized[4], hintedNullTimestamp, 'Null parameter should remain hinted Null parameter');

            done();
          });
        });

        it('leaves object parameter untouched', function (done) {
          // arrange
          var params = { foo: 'bar' };

          // act
          instance[method]('cql', params, {}, function () {
            var call = instance._execCql.getCall(0);
            var normalized = call.args[1];

            // assert
            assert.strictEqual(normalized, params, 'normalized params should match original');

            done();
          });
        });

        it('turns empty parameter into empty array', function (done) {
          // arrange
          var params = null;

          // act
          instance[method]('cql', params, {}, function () {
            var call = instance._execCql.getCall(0);
            var normalized = call.args[1];

            // assert
            assert.deepEqual(normalized, [], 'normalized params should be empty array');

            done();
          });
        });

        it('skips debug log if "suppressDebugLog" set to true', function (done) {
          // arrange
          var cqlQuery = 'SELECT * FROM users;';
          var params = [];
          var options = { suppressDebugLog: true };

          // act
          instance[method](cqlQuery, params, options, function () {
            // assert
            assert.notOk(instance.logger.debug.calledOnce, 'cql is logged');

            done();
          });
        });

        it(`executes cql with default ConsistencyLevel.${consistencyLevel} if consistency not provided.`, function (done) {
          // arrange
          var cqlQuery = 'SELECT * FROM users;';
          var params = [];
          var options = {};

          // act
          instance[method](cqlQuery, params, options, function () {
            var call = instance._execCql.getCall(0);

            // assert
            assert.ok(instance.logger.debug.calledOnce, 'cql is logged');
            assert.equal(call.args[0], cqlQuery, 'cql should be passed through');
            assert.equal(call.args[1], params, 'params should be passed through');
            assert.isObject(call.args[2], 'options should be populated');
            assert.strictEqual(call.args[2].consistency, cql.types.consistencies[consistencyLevel], `options.consistency should be ${consistencyLevel}`);

            done();
          });
        });

        it(`executes cql with default ConsistencyLevel.${consistencyLevel} if options is null.`, function (done) {
          // arrange
          var cqlQuery = 'SELECT * FROM users;';
          var params = [];
          var options = null;

          // act
          instance[method](cqlQuery, params, options, function () {
            var call = instance._execCql.getCall(0);

            // assert
            assert.equal(call.args[0], cqlQuery, 'cql should be passed through');
            assert.equal(call.args[1], params, 'params should be passed through');
            assert.isObject(call.args[2], 'options should be populated');
            assert.strictEqual(call.args[2].consistency, cql.types.consistencies[consistencyLevel], `options.consistency should be ${consistencyLevel}`);

            done();
          });
        });

        it(`executes cql with default ConsistencyLevel.${consistencyLevel} if options not provided.`, function (done) {
          // arrange
          var cqlQuery = 'SELECT * FROM users;';
          var params = [];

          // act
          instance[method](cqlQuery, params, function () {
            var call = instance._execCql.getCall(0);

            // assert
            assert.equal(call.args[0], cqlQuery, 'cql should be passed through');
            assert.equal(call.args[1], params, 'params should be passed through');
            assert.isObject(call.args[2], 'options should be populated');
            assert.strictEqual(call.args[2].consistency, cql.types.consistencies[consistencyLevel], `options.consistency should be ${consistencyLevel}`);

            done();
          });
        });

        it('executes cql with provided consistency.', function (done) {
          // arrange
          var cqlQuery = 'SELECT * FROM users;';
          var params = [];
          var consistency = cql.types.consistencies.quorum;
          var options = { consistency: consistency };

          // act
          instance[method](cqlQuery, params, options, function () {
            var call = instance._execCql.getCall(0);

            // assert
            assert.equal(call.args[0], cqlQuery, 'cql should be passed through');
            assert.equal(call.args[1], params, 'params should be passed through');
            assert.isObject(call.args[2], 'options should be populated');
            assert.strictEqual(call.args[2].consistency, consistency, 'options.consistency should be passed through');

            done();
          });
        });
      });
    }

    validateWrapperCall('select', 'one');
    validateWrapperCall('insert', 'localQuorum');
    validateWrapperCall('update', 'localQuorum');
    validateWrapperCall('delete', 'localQuorum');

  });

  describe('DatastaxDriver#beginQuery()', function () {

    var pool, instance;
    beforeEach(function () {
      instance = getDefaultInstance();
      pool = getPoolStub(instance.config, true, null, {});
      instance.pools = { myKeySpace: pool };
      instance._execCql = sinon.stub().resolves({});
      instance.getConnectionPool = sinon.stub().yields(null, pool);
    });

    function validateQueryCalls(asPromise) {
      it(`#execute() executes cql ${asPromise ? 'with promise syntax' : 'with callback syntax'}`, function (done) {
        // arrange
        var cqlQuery = 'SELECT * FROM users WHERE name = ?;';

        // act
        var query = instance
          .beginQuery()
          .query(cqlQuery)
          .param('name', 'text');

        if (asPromise) {
          query
            .execute()
            .then(function () {
              asserts(done);
            });
        } else {
          query.execute(function () {
            asserts(done);
          });
        }

        function asserts(done) {
          var call = instance._execCql.getCall(0);

          // assert
          assert.equal(call.args[0], cqlQuery, 'cql should be passed through');
          assert.equal(call.args[1], query.context.params, 'params should be passed through');
          assert.equal(call.args[2], query.context.options, 'options should be passed through');

          done();
        }

      });
    }

    validateQueryCalls(false);
    validateQueryCalls(true);

  });

  describe('DatastaxDriver#namedQuery()', function () {
    var pool, instance;
    beforeEach(function () {
      instance = getNamedQueryInstance();
      pool = getPoolStub(instance.config, true, null, {});
      instance.pools = { myKeySpace: pool };
      instance._execCql = sinon.stub().resolves({});
      instance._getConnectionPool = sinon.stub().resolves(pool);
    });

    function getNamedQueryInstance() {
      var config = getDefaultConfig();
      config.queryDirectory = path.join(__dirname, '../stubs/cql');
      return new Driver({ config: config });
    }

    it('executes the CQL specified by the named query', function (done) {
      // arrange
      var queryName = 'myFakeCql';
      var params = [];
      var consistency = cql.types.consistencies.one;

      // act
      instance.namedQuery(queryName, params, { consistency: consistency }, function () {
        var call = instance._execCql.getCall(0),
          opts = call.args[2];

        // assert
        assert.strictEqual(call.args[0], instance.queryCache.fileCache[queryName], 'cql should be read from query cache');
        assert.deepEqual(call.args[1], params, 'params should be passed through');
        assert.strictEqual(opts.executeAsPrepared, true, 'executeAsPrepared should be set to true');
        assert.strictEqual(opts.queryName, queryName, 'queryName should be set to the named query name');

        done();
      });
    });

    it('returns a Promise if no callback or stream is supplied', async () => {
      // arrange
      const rows = [
        { foo: 1, bar: 2 },
        { foo: 3, bar: 4 }
      ];
      instance._execCql.resolves(rows);

      // act
      const result = await instance.namedQuery('myFakeCql', [], { consistency: cql.types.consistencies.one });

      // assert
      expect(result).to.deep.equal(rows);
    });

    it('handles CQL files containing a BOM', function (done) {
      // arrange
      var queryName = 'cqlFileWithBOM';
      var params = [];
      var consistency = cql.types.consistencies.one;

      // act
      instance.namedQuery(queryName, params, { consistency: consistency }, function () {
        expect(instance._execCql.firstCall.args[0]).to.equal('SELECT * FROM "myColumnFamily"');
        done();
      });
    });

    it('allows caller to disable prepared statement', function (done) {
      // arrange
      var queryName = 'myFakeCql';
      var params = [];
      var consistency = cql.types.consistencies.one;

      // act
      instance.namedQuery(queryName, params, { consistency: consistency, executeAsPrepared: false }, function () {
        var call = instance._execCql.getCall(0),
          opts = call.args[2];

        // assert
        assert.strictEqual(opts.executeAsPrepared, false, 'executeAsPrepared should be set to true');

        done();
      });
    });

    it('allows callback to be optional to support fire-and-forget scenarios', function (done) {
      // arrange
      var queryName = 'myFakeCql';
      var params = [];
      var consistency = cql.types.consistencies.one;

      // act/assert
      expect(function () {
        instance.namedQuery(queryName, params, { consistency: consistency });
      }).not.to.throw(Error);

      done();
    });

    it('throws an error if named query does not exist', async () => {
      // arrange
      var queryName = 'idontexist';

      // act
      let error;
      try {
        await instance.namedQuery(queryName, []);
      } catch (err) {
        error = err;
      }

      // assert
      assert.instanceOf(error, Error, 'error is populated');
    });

    it('throws error if queryDirectory not provided in constructor', async () => {
      // arrange
      var defInstance = getDefaultInstance();
      var queryName = 'myFakeCql';

      // act
      let error;
      try {
        await defInstance.namedQuery(queryName, [], {});
      } catch (err) {
        error = err;
      }

      // assert
      expect(error).to.be.instanceOf(Error);
    });

  });
});
