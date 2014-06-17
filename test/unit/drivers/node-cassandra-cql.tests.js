"use strict";

var sinon = require("sinon"),
  chai = require("chai"),
  util = require("util"),
  assert = chai.assert,
  expect = chai.expect,
  FakeResolver = require("../../stubs/fakeResolver"),
  _ = require("lodash"),
  path = require("path");
chai.use(require('sinon-chai'));

var cql = require("node-cassandra-cql");
var Driver = require("../../../lib/drivers/node-cassandra-cql");

describe("lib/drivers/node-cassandra-cql.js", function () {

  function getDefaultConfig() {
    return {
      hosts: ["123.456.789.012:9042"],
      keyspace: "myKeySpace",
      timeout: 12345
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
    return Driver({
      config: getDefaultConfig(),
      logger: getDefaultLogger()
    });
  }

  describe("interface", function () {

    var instance = getDefaultInstance();

    function validateFunctionExists(name, argCount) {
      // arrange
      // act
      // assert
      assert.strictEqual(typeof instance[name], "function");
      assert.strictEqual(instance[name].length, argCount, name + " takes " + argCount + " arguments");
    }

    it("is a constructor function", function () {
      assert.strictEqual(typeof Driver, "function", "exports a constructor function");
    });
    it("instance provides a cql function", function () {
      validateFunctionExists("cql", 4);
    });
    it("instance provides a namedQuery function", function () {
      validateFunctionExists("namedQuery", 4);
    });
    it("instance provides consistencyLevel object", function () {
      assert.isDefined(instance.consistencyLevel);
    });
    it("instance provides a select function", function () {
      validateFunctionExists("select", 4);
    });
    it("instance provides a insert function", function () {
      validateFunctionExists("insert", 4);
    });
    it("instance provides a update function", function () {
      validateFunctionExists("update", 4);
    });
    it("instance provides a delete function", function () {
      validateFunctionExists("delete", 4);
    });
    it("instance provides a close function", function () {
      validateFunctionExists("close", 1);
    });
  });

  describe("NodeCassandraDriver#constructor", function () {
    it("should throw exception if context is missing", function () {
      // arrange
      // act, assert
      expect(function () {
        new Driver();
      }).to.throw(Error, /missing context /i);
    });

    it("should throw exception if config is missing from context", function () {
      // arrange
      // act, assert
      expect(function () {
        new Driver({ });
      }).to.throw(Error, /missing context.config /i);
    });
    it("sets default pool configuration", function () {
      // arrange
      var config = _.extend({ }, getDefaultConfig());
      var configCopy = _.extend({ }, config);
      var cqlVersion = "3.0.0";
      var consistencyLevel = cql.types.consistencies.one;

      // act
      var instance = new Driver({ config: config });

      // assert
      assert.deepEqual(instance.poolConfig.hosts, configCopy.hosts, "hosts should be passed through");
      assert.strictEqual(instance.poolConfig.keyspace, configCopy.keyspace, "keyspace should be passed through");
      assert.strictEqual(instance.poolConfig.getAConnectionTimeout, configCopy.timeout, "timeout should be passed through");
      assert.strictEqual(instance.poolConfig.version, cqlVersion, "version should default to 3.0.0");
      assert.strictEqual(instance.poolConfig.consistencyLevel, consistencyLevel, "consistencyLevel should default to ONE");
    });

    it("should override default pool config with additional store options", function () {
      // arrange
      var config = _.extend({}, getDefaultConfig());
      var configCopy = _.extend({ }, config);
      var cqlVersion = "2.0.0";
      var consistencyLevel = cql.types.consistencies.any;
      config.cqlVersion = cqlVersion;
      config.consistencyLevel = consistencyLevel;

      // act
      var instance = new Driver({ config: config });

      // assert
      assert.deepEqual(instance.poolConfig.hosts, configCopy.hosts, "hosts should be passed through");
      assert.strictEqual(instance.poolConfig.keyspace, configCopy.keyspace, "keyspace should be passed through");
      assert.strictEqual(instance.poolConfig.getAConnectionTimeout, configCopy.timeout, "timeout should be passed through");
      assert.strictEqual(instance.poolConfig.version, cqlVersion, "cqlVersion should be overridden");
      assert.strictEqual(instance.poolConfig.consistencyLevel, consistencyLevel, "consistencyLevel should be overridden");
    });
  });

  function getPoolStub(config, isReady, err, data) {
    return {
      storeConfig: _.extend({ consistencyLevel: 1, version: "3.0.0"}, config),
      isReady: isReady,
      execute: sinon.stub().yields(err, data),
      executeAsPrepared: sinon.stub().yields(err, data),
      shutdown: sinon.spy(function (callback) {
        if (callback) {
          process.nextTick(function () {
            callback(null, null);
          });
        }
      })
    };
  }

  describe("NodeCassandraDriver#close()", function () {

    var instance = getDefaultInstance();

    it("closes the connection pool if it exists", function (done) {
      // arrange
      var pool = getPoolStub(instance.config, true, null, {});
      instance.pools = { default: pool };
      var closedHandler = sinon.stub();
      instance.on('connectionClosed', closedHandler);

      // act
      instance.close(function () {
        // assert
        assert.strictEqual(pool.isClosed, true, "pool should be set to closed");
        assert.ok(pool.shutdown.called, "pool shutdown should be called");
        expect(closedHandler).to.have.been.called;

        done();
      });
    });

    it("just calls callback if pool does not yet exist", function (done) {
      // arrange
      instance.pools = {};
      var closedHandler = sinon.stub();
      instance.on('connectionClosed', closedHandler);

      // act
      instance.close(function () {
        // assert
        expect(closedHandler).to.have.not.been.called;

        done();
      });
    });

  });

  describe("NodeCassandraDriver#cql()", function () {

    var instance = null,
      fakeResolver = null;

    beforeEach(function () {
      fakeResolver = new FakeResolver();
      instance = getDefaultInstance();
    });

    afterEach(function () {
      if (cql.Client.restore) {
        cql.Client.restore();
      }
    });

    it("emits a connectionRequested event at the beginning of a query", function () {
      var eventHandler = sinon.stub();
      instance.on('connectionRequested', eventHandler);

      instance.cql("select foo from bar");

      expect(eventHandler).to.have.been.called;
    });

    it("creates a new connection pool if one does not exist", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().yieldsAsync(null, {});
      sinon.stub(cql, "Client").returns(pool);
      instance.pools = {};
      var connectionOpeningHandler = sinon.stub(),
        connectionOpenedHandler = sinon.stub(),
        queryStartedHandler = sinon.stub();
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
        assert.equal(instance.pools.default, pool, "pool should be cached");
        assert.strictEqual(pool.waiters.length, 0, "waiters should be executed after connection completes");
        assert.strictEqual(pool.execute.called, true, "cql statements should execute after connection completes");

        done();
      });

      // before callback asserts
      assert.strictEqual(pool.execute.called, false, "cql statements should wait to execute until after connection completes");
    });

    it("creates a new connection pool if pool is closed", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().yieldsAsync(null, {});
      sinon.stub(cql, "Client").returns(pool);
      var existingPool = getPoolStub(instance.config, true, null, {});
      existingPool.isClosed = true;
      instance.pools = { default: existingPool };

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function () {
        // assert
        assert.notOk(existingPool.execute.called, "existing pool should not be called");
        assert.ok(pool.execute.called, "new pool should be called");

        done();
      });
    });

    it("adds a new connection pool if keyspace is different", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(_.extend(_.extend({}, instance.config), { keyspace: "myNewKeyspace" }), true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().yieldsAsync(null, {});
      sinon.stub(cql, "Client").returns(pool);

      var existingPool = getPoolStub(_.extend({}, instance.config), true, null, {});
      instance.pools = { default: existingPool };

      // act
      instance.cql(cqlQuery, params, { consistency: consistency, keyspace: "myNewKeyspace" }, function () {
        // assert
        assert.notOk(existingPool.execute.called, "existing pool should not be called");
        assert.ok(pool.execute.called, "new pool should be called");

        done();
      });
    });

    it("uses default connection pool if supplied keyspace matches default", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(_.extend(_.extend({}, instance.config), { keyspace: instance.config.keyspace }), true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().yieldsAsync(null, {});
      sinon.stub(cql, "Client").returns(pool);
      var existingPool = getPoolStub(_.extend({}, instance.config), true, null, {});
      instance.pools = { default: existingPool };
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
        assert.notOk(pool.execute.called, "new pool should not be called");
        assert.ok(existingPool.execute.called, "existing pool should be called");

        done();
      });
    });

    it("creates a new connection pool if config no longer matches", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub({ different: "config", keyspace: instance.config.keyspace}, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().yieldsAsync(null, {});
      sinon.stub(cql, "Client").returns(pool);
      var existingPool = getPoolStub({ keyspace: instance.config.keyspace }, true, null, {});
      instance.pools = { default: existingPool };

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function () {
        // assert
        assert.notEqual(instance.pools.default, existingPool, "existing pool should be replaced");
        assert.equal(instance.pools.default, pool, "pool should be cached");
        assert.strictEqual(pool.waiters.length, 0, "waiters should be executed after connection completes");
        assert.strictEqual(pool.execute.called, true, "cql statements should execute after connection completes");

        done();
      });

      // before callback asserts
      assert.strictEqual(pool.execute.called, false, "cql statements should wait to execute until after connection completes");
    });

    function testLogEvent(logLevel, expectedLevel, done) {
      // setup arrange
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().yieldsAsync(null, {});
      sinon.stub(cql, "Client").returns(pool);
      instance.pools = {};
      var logEventHandler = sinon.stub();
      instance.on('connectionLogged', logEventHandler);

      // setup act
      instance.cql("cql", [], { consistency: consistency }, function () {
        // setup assert
        assert.ok(pool.on.calledWith("log", sinon.match.func), "log handler should be wired up");

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
        var message = "something blew up",
          metaData = { some: "metadata" };
        errorCb(logLevel, message, metaData);

        // handler assert
        expect(logEventHandler).to.have.been.calledWith(logLevel, message, metaData);
        if (expectedLevel) {
          assert.ok(instance.logger[expectedLevel].calledWithMatch("priam.Driver: " + message, { data: metaData }));
        }
        else {
          assert.notOk(instance.logger.info.called);
          assert.notOk(instance.logger.warn.called);
          assert.notOk(instance.logger.error.called);
        }
      });

      done();
    }

    it("sets up a global trace handler for the connection pool - ignores debug level", function (done) {
      testLogEvent("debug", null, done);
    });

    it("sets up a global trace handler for the connection pool - ignores info level", function (done) {
      testLogEvent("info", null, done);
    });

    it("sets up a global error handler for the connection pool - logs warn as warn", function (done) {
      testLogEvent("warn", "warn", done);
    });

    it("sets up a global error handler for the connection pool - logs error as warn", function (done) {
      testLogEvent("error", "warn", done);
    });

    it("sets up a global error handler for the connection pool - logs critical as critical", function (done) {
      testLogEvent("critical", "critical", done);
    });

    it("sets up an error handler for pool.connect", function (done) {
      // arrange
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().yieldsAsync(new Error("Connection pool failed to connect"));
      sinon.stub(cql, "Client").returns(pool);
      instance.pools = {};
      instance.logger = {
        debug: sinon.stub(),
        error: sinon.stub()
      };
      var connectionFailedHandler = sinon.stub();
      instance.on('connectionFailed', connectionFailedHandler);

      // act
      instance.cql("cql", [], { consistency: consistency }, function (err, result) {
        // assert
        assert.instanceOf(err, Error);
        assert.isUndefined(result);
        assert.ok(instance.logger.error.calledOnce, "error log is called once");
        expect(connectionFailedHandler).to.have.been.calledWithMatch(sinon.match.string, err);

        done();
      });
    });

    it("executes queued queries when connection completes", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, {});
      pool.on = sinon.stub();
      pool.connect = sinon.stub().yieldsAsync(null);
      sinon.stub(cql, "Client").returns(pool);
      instance.pools = {};

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function () {
        // assert
        assert.strictEqual(pool.isReady, true, "pool should be set to true after connection completes");
        assert.strictEqual(pool.execute.called, true, "cql statements should execute after connection completes");

        done();
      });
    });

    it("allows callback to be optional to support fire-and-forget scenarios", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var pool = getPoolStub(instance.config, true, null, []);
      instance.pools = { default: pool };

      pool.execute = sinon.spy(function (cql, data, consist, cb) {
        var call = pool.execute.getCall(0);

        // assert
        assert.strictEqual(call.args[0], cql, "cql should be passed through");
        assert.deepEqual(call.args[1], params, "params should be passed through");

        cb(null, []);
        done();
      });

      // act
      instance.cql(cqlQuery, params);
    });

    it("uses default consistency of ONE if no options are passed", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, []);
      pool.on = sinon.stub();
      pool.connect = sinon.stub().yieldsAsync(null);
      sinon.stub(cql, "Client").returns(pool);
      instance.pools = {};

      // act
      instance.cql(cqlQuery, params, function () {
        var ctorCall = cql.Client.getCall(0);

        // assert
        assert.strictEqual(ctorCall.args[0].consistencyLevel, consistency, "consistency should be ONE");

        done();
      });
    });

    it("executes CQL and returns the data", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3", new Buffer("param4")];
      var consistency = cql.types.consistencies.one;
      var err = null;
      var data = {
        rows: [
          {
            columns: [
              { name: "field1", types: [1, null] }
            ],
            field1: "value1" }
        ]
      };
      var pool = getPoolStub(instance.config, true, err, data);
      instance.pools = { default: pool };
      var completedHandler = sinon.stub();
      instance.on('queryCompleted', completedHandler);

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function (error, returnData) {
        var call = pool.execute.getCall(0);

        // assert
        assert.strictEqual(call.args[0], cqlQuery, "cql should be passed through");
        assert.deepEqual(call.args[1], params, "params should be passed through");
        assert.isNull(error, "error should be null");
        assert.deepEqual(returnData, [
          { field1: "value1" }
        ], "data should match normalized cql output");
        expect(completedHandler).to.have.been.called;

        done();
      });
    });

    it('handles null parameters', function (done) {
      // arrange
      var cqlStatement = 'INSERT INTO foo (id, some_column) VALUES (?, ?)';
      var params = [1, null];
      var pool = getPoolStub(instance.config, true, null, null);
      instance.pools = { default: pool };

      // act
      instance.cql(cqlStatement, params, { consistency: cql.types.consistencies.one }, function () {
        // assert
        expect(pool.execute).to.have.been.calledWithMatch(cqlStatement, sinon.match([1, "null"]));

        done();
      });
    });

    it("executes CQL as prepared statement and returns the data if 'executeAsPrepared' option specified", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3", new Buffer("param4")];
      var consistency = cql.types.consistencies.one;
      var err = null;
      var data = {
        rows: [
          {
            columns: [
              { name: "field1", types: [1, null] }
            ],
            field1: "value1" }
        ]
      };
      var pool = getPoolStub(instance.config, true, err, data);
      instance.pools = { default: pool };

      // act
      instance.cql(cqlQuery, params, { consistency: consistency, executeAsPrepared: true }, function (error, returnData) {
        var call = pool.executeAsPrepared.getCall(0);

        // assert
        assert.strictEqual(call.args[0], cqlQuery, "cql should be passed through");
        assert.deepEqual(call.args[1], params, "params should be passed through");
        assert.isNull(error, "error should be null");
        assert.deepEqual(returnData, [
          { field1: "value1" }
        ], "data should match normalized cql output");

        done();
      });
    });

    it("normalizes/deserializes the data in the resulting array", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.one;
      var err = null;
      var data = {
        rows: [
          {
            columns: [
              { name: "field1", types: [1, null] },
              { name: "field2", types: [1, null] },
              { name: "field3", types: [1, null] },
              { name: "field4", types: [1, null] },
              { name: "field5", types: [1, null] },
              { name: "field6", types: [1, null] }
            ],
            field1: "value1",
            field2: 2,
            field3: "{ \"subField1\": \"blah\" }",
            field4: "[ 4, 3, 2, 1]",
            field5: "{ some invalid json }",
            field6: "{ \"jsonThat\": \"iDontWantToParse\" }"
          }
        ]
      };

      var pool = getPoolStub(instance.config, true, err, data);
      instance.pools = { default: pool };

      // act
      instance.cql(cqlQuery, params, {
        consistency: consistency,
        resultHint: {
          field1: instance.dataType.ascii,
          field2: instance.dataType.number,
          field3: instance.dataType.objectAscii,
          field4: instance.dataType.objectText,
          field5: instance.dataType.objectAscii
          //field6 intentionally omitted
        }
      }, function (error, returnData) {
        var call = pool.execute.getCall(0);

        // assert
        assert.strictEqual(call.args[0], cqlQuery, "cql should be passed through");
        assert.deepEqual(call.args[1], params, "params should be passed through");
        assert.isNull(error, "error should be null");
        assert.strictEqual(returnData[0].field1, "value1", "first field should be a string");
        assert.strictEqual(returnData[0].field2, 2, "second field should be a number");
        assert.deepEqual(returnData[0].field3, { subField1: 'blah' }, "third field should be an object");
        assert.deepEqual(returnData[0].field4, [ 4, 3, 2, 1], "fourth field should be an array");
        assert.deepEqual(returnData[0].field5, "{ some invalid json }", "fifth field should be a string");
        assert.deepEqual(returnData[0].field6, "{ \"jsonThat\": \"iDontWantToParse\" }", "sixth field should be a string");

        done();
      });
    });

    it("normalizes/deserializes the data in the resulting array by detecting JSON strings", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.one;
      var err = null;
      var data = {
        rows: [
          {
            columns: [
              { name: "field1", types: [1, null] },
              { name: "field2", types: [1, null] },
              { name: "field3", types: [1, null] },
              { name: "field4", types: [1, null] },
              { name: "field5", types: [1, null] }
            ],
            field1: "value1",
            field2: 2,
            field3: "{ \"subField1\": \"blah\" }",
            field4: "[ 4, 3, 2, 1]",
            field5: "{ some invalid json }"
          }
        ]
      };

      var pool = getPoolStub(instance.config, true, err, data);
      instance.pools = { default: pool };

      // act
      instance.cql(cqlQuery, params, {
        consistency: consistency,
        deserializeJsonStrings: true
      }, function (error, returnData) {
        var call = pool.execute.getCall(0);

        // assert
        assert.strictEqual(call.args[0], cqlQuery, "cql should be passed through");
        assert.deepEqual(call.args[1], params, "params should be passed through");
        assert.isNull(error, "error should be null");
        assert.strictEqual(returnData[0].field1, "value1", "first field should be a string");
        assert.strictEqual(returnData[0].field2, 2, "second field should be a number");
        assert.deepEqual(returnData[0].field3, { subField1: 'blah' }, "third field should be an object");
        assert.deepEqual(returnData[0].field4, [ 4, 3, 2, 1], "fourth field should be an array");
        assert.deepEqual(returnData[0].field5, "{ some invalid json }", "fifth field should be a string");

        done();
      });
    });

    function testErrorRetry(errorName, errorCode, numRetries, shouldRetry) {
      it((shouldRetry ? "adds" : "does not add") + " error retry if error is '" + errorName + "', code '" + errorCode + "', and retries " + numRetries, function (done) {
        // arrange
        var cqlQuery = "MyCqlStatement";
        var params = ["param1", "param2", "param3"];
        var consistency = cql.types.consistencies.one;
        var pool = getPoolStub(instance.config, true, null, {});
        var data = [];
        var callCount = 0;
        var queryRetriedHandler = sinon.stub();
        instance.on('queryRetried', queryRetriedHandler);
        pool.execute = sinon.spy(function (c, d, con, cb) {
          callCount++;
          if (callCount === 1) {
            var e = new Error("some connection error");
            e.name = errorName;
            e.code = errorCode;
            cb(e);
          }
          else {
            cb(null, data);
          }
        });
        instance.pools = { default: pool };
        instance.config.numRetries = numRetries;
        instance.config.retryDelay = 1;

        // act
        instance.cql(cqlQuery, params, { consistency: consistency }, function (error, returnData) {
          // assert
          if (shouldRetry) {
            var call1 = pool.execute.getCall(0);
            var call2 = pool.execute.getCall(1);
            assert.strictEqual(pool.execute.callCount, 2, "cql should be called twice");
            assert.notEqual(call1.args[1], call2.args[1], "parameters should be cloned");
            assert.deepEqual(call1.args[1], call2.args[1], "parameters should be cloned");
            assert.deepEqual(returnData, data, "data should match cql output");
            expect(queryRetriedHandler.getCalls()).to.have.lengthOf(numRetries);
          }
          else {
            assert.strictEqual(pool.execute.callCount, 1, "cql should be called once");
            expect(queryRetriedHandler).to.have.not.been.called;
          }

          done();
        });
      });
    }

    testErrorRetry("DriverError", null, 0, false);
    testErrorRetry("DriverError", null, 1, true);
    testErrorRetry("PoolConnectionError", null, 1, true);
    testErrorRetry("ResponseError", null, 1, false);
    testErrorRetry("Error", "ECONNRESET", 1, true);
    testErrorRetry("Error", "ECONNREFUSED", 1, true);
    testErrorRetry("Error", "ENOTFOUND", 1, true);
    testErrorRetry("Error", "SomethingElse", 1, false);

    it("does not add error retry at consistency QUORUM when original consistency is ALL and enableConsistencyFailover is false", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.all;
      var pool = getPoolStub(instance.config, true, null, {});
      var data = [];
      var callCount = 0;
      pool.execute = sinon.spy(function (c, d, con, cb) {
        callCount++;
        if (callCount === 1) {
          cb(new Error("throws error on ALL"));
        }
        else {
          cb(null, data);
        }
      });
      instance.pools = { default: pool };
      instance.config.retryDelay = 1;
      instance.config.enableConsistencyFailover = false;

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function (error, returnData) {
        // assert
        assert.strictEqual(pool.execute.callCount, 1, "execute should be called once");
        assert.ok(error);
        assert.notOk(returnData);

        done();
      });
    });

    it("adds error retry at consistency QUORUM when original consistency is ALL", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.all;
      var pool = getPoolStub(instance.config, true, null, {});
      var data = [];
      var callCount = 0;
      pool.execute = sinon.spy(function (c, d, con, cb) {
        callCount++;
        if (callCount === 1) {
          cb(new Error("throws error on ALL"));
        }
        else {
          cb(null, data);
        }
      });
      instance.pools = { default: pool };
      instance.config.retryDelay = 1;

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function (error, returnData) {
        var call1 = pool.execute.getCall(0);
        var call2 = pool.execute.getCall(1);
        // assert
        assert.strictEqual(pool.execute.callCount, 2, "cql should be called twice");
        assert.notEqual(call1.args[1], call2.args[1], "parameters should be cloned");
        assert.deepEqual(call1.args[1], call2.args[1], "parameters should be cloned");
        assert.deepEqual(returnData, data, "data should match cql output");

        done();
      });
    });

    it("adds error retry at consistency LOCUM_QUORUM when original consistency is QUORUM", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.quorum;
      var pool = getPoolStub(instance.config, true, null, {});
      var data = [];
      var callCount = 0;
      pool.execute = sinon.spy(function (c, d, con, cb) {
        callCount++;
        if (callCount === 1) {
          cb(new Error("throws error on QUORUM"));
        }
        else {
          cb(null, data);
        }
      });
      instance.pools = { default: pool };
      instance.config.retryDelay = 1;

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function (error, returnData) {
        var call1 = pool.execute.getCall(0);
        var call2 = pool.execute.getCall(1);
        // assert
        assert.strictEqual(pool.execute.callCount, 2, "cql should be called twice");
        assert.notEqual(call1.args[1], call2.args[1], "parameters should be cloned");
        assert.deepEqual(call1.args[1], call2.args[1], "parameters should be cloned");
        assert.deepEqual(returnData, data, "data should match cql output");

        done();
      });
    });

    it("emits a queryFailed event when a query fails", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.quorum;
      var pool = getPoolStub(instance.config, true, null, {});
      pool.execute = sinon.stub().yields(new Error("throws error on QUORUM"));
      instance.pools = { default: pool };
      var failedHandler = sinon.stub();
      instance.on('queryFailed', failedHandler);

      // act
      instance.cql(cqlQuery, params, { consistency: consistency }, function () {
        expect(failedHandler).to.have.been.called;
        done();
      });
    });

    it("captures metrics if metrics and queryName are provided", function (done) {
      // arrange
      var cqlQuery = "MyCqlStatement";
      var queryName = "MyQueryName";
      var params = ["param1", "param2", "param3"];
      var consistency = cql.types.consistencies.one;
      var err = null;
      var data = { field1: "value1" };
      var pool = getPoolStub(instance.config, true, err, data);
      instance.pools = { default: pool };
      instance.metrics = {
        measurement: sinon.stub()
      };

      // act
      instance.cql(cqlQuery, params, { consistency: consistency, queryName: queryName }, function () {
        var call = instance.metrics.measurement.getCall(0);

        // assert
        assert.ok(instance.metrics.measurement.calledOnce, "measurement called once");
        assert.strictEqual(call.args[0], "query." + queryName, "measurement called with appropriate query name");

        done();
      });
    });

    describe("with connection resolver", function () {

      var logger = null;

      function getResolverInstance(context) {
        logger = {
          debug: sinon.stub(),
          info: sinon.stub(),
          warn: sinon.stub(),
          error: sinon.stub()
        };
        return new Driver(_.extend({ config: getDefaultConfig(), logger: logger }, context));
      }

      beforeEach(function () {
        instance = null;
        fakeResolver.resolveConnection = sinon.stub().yieldsAsync(null, {});
      });


      it("uses supplied connection resolver to override base config", function (done) {
        // arrange
        var cqlQuery = "MyCqlStatement";
        var params = ["param1", "param2", "param3"];
        var consistency = cql.types.consistencies.one;
        instance = getResolverInstance({ connectionResolver: fakeResolver });
        var fakeConnectionInfo = {
          username: "myResolvedUsername",
          password: "myResolvedPassword",
          hosts: []
        };
        fakeResolver.resolveConnection = sinon.stub().yieldsAsync(null, fakeConnectionInfo);
        var pool = getPoolStub(instance.config, true, null, {});
        pool.on = sinon.stub();
        pool.connect = sinon.stub().yieldsAsync(null, {});
        sinon.stub(cql, "Client").returns(pool);
        instance.pools = {};

        // act
        instance.cql(cqlQuery, params, { consistency: consistency }, function () {
          // assert
          assert.strictEqual(pool.storeConfig.username, fakeConnectionInfo.username);
          assert.strictEqual(pool.storeConfig.password, fakeConnectionInfo.password);

          done();
        });
      });

      it("uses resolved connection resolver from path to override base config", function (done) {
        // arrange
        var cqlQuery = "MyCqlStatement";
        var params = ["param1", "param2", "param3"];
        var consistency = cql.types.consistencies.one;
        instance = getResolverInstance({ config: { connectionResolverPath: "../../test/stubs/fakeResolver" } });
        var userName = "myResolvedUsername";
        var fakeConnectionInfo = {
          user: userName,
          password: "myResolvedPassword",
          hosts: ["123.456.789.012:1234"]
        };
        instance.connectionResolver.resolveConnection = sinon.stub().yieldsAsync(null, fakeConnectionInfo);
        var pool = getPoolStub(instance.config, true, null, {});
        pool.on = sinon.stub();
        pool.connect = sinon.stub().yieldsAsync(null, {});
        sinon.stub(cql, "Client").returns(pool);
        instance.pools = {};

        // act
        instance.cql(cqlQuery, params, { consistency: consistency }, function () {
          // assert
          assert.strictEqual(pool.storeConfig.username, userName);
          assert.strictEqual(pool.storeConfig.password, fakeConnectionInfo.password);
          assert.deepEqual(pool.storeConfig.hosts, fakeConnectionInfo.hosts);

          done();
        });
      });

      it("applies port remapping to resolved connection information if specified", function (done) {
        // arrange
        var cqlQuery = "MyCqlStatement";
        var params = ["param1", "param2", "param3"];
        var consistency = cql.types.consistencies.one;
        instance = getResolverInstance({ config: { connectionResolverPath: "../../test/stubs/fakeResolver" } });
        instance.config.connectionResolverPortMap = {
          from: "1234",
          to: "2345"
        };
        instance.poolConfig.connectionResolverPortMap = instance.config.connectionResolverPortMap;
        var userName = "myResolvedUsername";
        var fakeConnectionInfo = {
          user: userName,
          password: "myResolvedPassword",
          hosts: ["123.456.789.012:1234", "234.567.890.123"]
        };
        instance.connectionResolver.resolveConnection = sinon.stub().yieldsAsync(null, fakeConnectionInfo);
        var pool = getPoolStub(instance.config, true, null, {});
        pool.on = sinon.stub();
        pool.connect = sinon.stub().yieldsAsync(null, {});
        sinon.stub(cql, "Client").returns(pool);
        instance.pools = {};

        // act
        instance.cql(cqlQuery, params, { consistency: consistency }, function () {
          // assert
          assert.strictEqual(pool.storeConfig.username, userName);
          assert.strictEqual(pool.storeConfig.password, fakeConnectionInfo.password);
          assert.deepEqual(pool.storeConfig.hosts, ["123.456.789.012:2345", "234.567.890.123"], "hosts were applied with remapped ports");

          done();
        });
      });

      it("logs and returns error if connection resolver throws error", function (done) {
        // arrange
        var cqlQuery = "MyCqlStatement";
        var params = ["param1", "param2", "param3"];
        var consistency = cql.types.consistencies.one;
        instance = getResolverInstance({ connectionResolver: fakeResolver });
        fakeResolver.resolveConnection = sinon.stub().yieldsAsync(new Error("connection resolution failed"));
        var pool = getPoolStub(instance.config, true, null, {});
        pool.on = sinon.stub();
        pool.connect = sinon.stub().yieldsAsync(null, {});
        sinon.stub(cql, "Client").returns(pool);
        instance.pools = {};
        var connectionResolvedErrorHandler = sinon.stub();
        instance.on('connectionResolvedError', connectionResolvedErrorHandler);

        // act
        instance.cql(cqlQuery, params, { consistency: consistency }, function (err, result) {
          // assert
          assert.instanceOf(err, Error);
          assert.isUndefined(result);
          assert.ok(logger.error.calledOnce, "error log is called once");
          expect(connectionResolvedErrorHandler).to.have.been.calledWithMatch(sinon.match.string, err);

//                    setTimeout(function () {
//                        assert.notOk(logger.error.calledTwice, "error logger should only be called once");
//
//                        done();
//                    }, 5);
          done();
        });
      });

      it("logs error and updates connection if connection resolver returns error AND data", function (done) {
        // arrange
        var cqlQuery = "MyCqlStatement";
        var params = ["param1", "param2", "param3"];
        var consistency = cql.types.consistencies.one;
        instance = getResolverInstance({ connectionResolver: fakeResolver });
        var fakeConnectionInfo = {
          username: "myResolvedUsername",
          password: "myResolvedPassword",
          hosts: []
        };
        var resolutionError = new Error("connection resolution failed");
        fakeResolver.resolveConnection = sinon.stub().yieldsAsync(resolutionError, fakeConnectionInfo);
        var pool = getPoolStub(instance.config, true, null, {});
        pool.on = sinon.stub();
        pool.connect = sinon.stub().yieldsAsync(null, {});
        sinon.stub(cql, "Client").returns(pool);
        instance.pools = {};
        var resolutionErrorHandler = sinon.stub();
        instance.on('connectionResolvedError', resolutionErrorHandler);

        // act
        instance.cql(cqlQuery, params, { consistency: consistency }, function (err) {
          // assert
          assert.isNull(err);
          assert.strictEqual(pool.storeConfig.username, fakeConnectionInfo.username, "username successfully updated");
          assert.strictEqual(pool.storeConfig.password, fakeConnectionInfo.password, "password successfully updated");
          assert.ok(logger.error.called, "error log is called");
          expect(resolutionErrorHandler).to.have.been.calledWithMatch(sinon.match.string, resolutionError);
          done();
        });
      });

      it("returns data and logs error if connection resolver throws error on lazy fetch", function (done) {
        // arrange
        var cqlQuery = "MyCqlStatement";
        var params = ["param1", "param2", "param3"];
        var consistency = cql.types.consistencies.one;
        instance = getResolverInstance({ connectionResolver: fakeResolver });
        var fakeConnectionInfo = {
          username: "myResolvedUsername",
          password: "myResolvedPassword",
          hosts: []
        };
        var fetchError = new Error("lazy fetch error");
        fakeResolver.resolveConnection = function (data, cb) {
          fakeResolver.on.getCall(1).args[1](fetchError);
          cb(null, fakeConnectionInfo);
        };
        var pool = getPoolStub(instance.config, true, null, {});
        pool.on = sinon.stub();
        pool.connect = sinon.stub().yieldsAsync(null, {});
        sinon.stub(cql, "Client").returns(pool);
        instance.pools = {};
        var connectionOptionsErrorHandler = sinon.stub();
        instance.on('connectionOptionsError', connectionOptionsErrorHandler);

        // act
        instance.cql(cqlQuery, params, { consistency: consistency }, function () {
          // assert
          assert.strictEqual(pool.storeConfig.username, fakeConnectionInfo.username, "username successfully updated");
          assert.strictEqual(pool.storeConfig.password, fakeConnectionInfo.password, "password successfully updated");
          expect(connectionOptionsErrorHandler).to.have.been.calledWith(fetchError);
          done();
        });
      });

      it("returns data if connection resolver successfully performs a lazy fetch", function (done) {
        // arrange
        var cqlQuery = "MyCqlStatement";
        var params = ["param1", "param2", "param3"];
        var consistency = cql.types.consistencies.one;
        instance = getResolverInstance({ connectionResolver: fakeResolver });
        var fakeConnectionInfo = {
          username: "myResolvedUsername",
          password: "myResolvedPassword",
          hosts: []
        };
        fakeResolver.resolveConnection = function (data, cb) {
          cb(null, fakeConnectionInfo);
          fakeResolver.on.getCall(1).args[1](null, { user: "someOtherInfo", password: "someOtherPassword" });
        };
        var pool = getPoolStub(instance.config, true, null, {});
        pool.on = sinon.stub();
        pool.connect = sinon.stub().yieldsAsync(null, {});
        sinon.stub(cql, "Client").returns(pool);
        instance.pools = {};
        var connectionOptionsFetchedHandler = sinon.stub();
        var connectionResolvedHandler = sinon.stub();
        instance.on('connectionOptionsFetched', connectionOptionsFetchedHandler);
        instance.on('connectionResolved', connectionResolvedHandler);

        // act
        instance.cql(cqlQuery, params, { consistency: consistency }, function () {
          // assert
          assert.strictEqual(pool.storeConfig.username, fakeConnectionInfo.username, "username successfully updated");
          assert.strictEqual(pool.storeConfig.password, fakeConnectionInfo.password, "password successfully updated");
          assert.notOk(logger.warn.called, "warn logger should not be called");
          expect(connectionOptionsFetchedHandler).to.have.been.called;
          expect(connectionResolvedHandler).to.have.been.called;
          done();
        });
      });
    });
  });

  describe("crud wrappers", function () {

    var instance;
    beforeEach(function () {
      instance = getDefaultInstance();
      sinon.stub(instance, "execCql").yields(null, {});
    });

    afterEach(function () {
      instance.execCql.restore();
    });

    function validateWrapperCall(method, consistencyLevel) {
      describe("NodeCassandraDriver#" + method + "()", function () {
        it("normalizes the parameter list if it is an array", function (done) {
          // arrange
          var dt = new Date();
          var buffer = new Buffer(4096);
          var hinted = { value: "bar", hint: 1 /*ascii*/ };
          buffer.write("This is a string buffer", "utf-8");
          var params = [ 1, "myString", dt, [1, 2, 3, 4], { myObjectKey: "value"}, buffer, hinted ];

          // act
          instance[method]("cql", params, {}, function () {
            var call = instance.execCql.getCall(0);
            var normalized = call.args[1];

            // assert
            assert.strictEqual(normalized[0], params[0], "1st parameter should be a number");
            assert.strictEqual(normalized[1], params[1], "2nd parameter should be a string");
            assert.strictEqual(normalized[2], JSON.stringify(dt), "3rd parameter should be date converted to string");
            assert.strictEqual(normalized[3], "[1,2,3,4]", "4th parameter should be array converted to JSON");
            assert.strictEqual(normalized[4], "{\"myObjectKey\":\"value\"}", "5th parameter should be object converted to JSON");
            assert.ok(Buffer.isBuffer(normalized[5]), "6th parameter should be buffer");
            assert.deepEqual(normalized[6], hinted, "7th parameter should remain hinted object parameter");

            done();
          });
        });

        it("leaves object parameter untouched", function (done) {
          // arrange
          var params = { foo: "bar" };

          // act
          instance[method]("cql", params, {}, function () {
            var call = instance.execCql.getCall(0);
            var normalized = call.args[1];

            // assert
            assert.strictEqual(normalized, params, "normalized params should match original");

            done();
          });
        });

        it("turns empty parameter into empty array", function (done) {
          // arrange
          var params = null;

          // act
          instance[method]("cql", params, {}, function () {
            var call = instance.execCql.getCall(0);
            var normalized = call.args[1];

            // assert
            assert.deepEqual(normalized, [], "normalized params should be empty array");

            done();
          });
        });

        it("skips debug log if 'suppressDebugLog' set to true", function (done) {
          // arrange
          var cqlQuery = "SELECT * FROM users;";
          var params = [];
          var options = { suppressDebugLog: true };

          // act
          instance[method](cqlQuery, params, options, function () {
            // assert
            assert.notOk(instance.logger.debug.calledOnce, "cql is logged");

            done();
          });
        });

        it("executes cql with default ConsistencyLevel." + consistencyLevel + " if consistency not provided.", function (done) {
          // arrange
          var cqlQuery = "SELECT * FROM users;";
          var params = [];
          var options = {};

          // act
          instance[method](cqlQuery, params, options, function () {
            var call = instance.execCql.getCall(0);

            // assert
            assert.ok(instance.logger.debug.calledOnce, "cql is logged");
            assert.equal(call.args[0], cqlQuery, "cql should be passed through");
            assert.equal(call.args[1], params, "params should be passed through");
            assert.isObject(call.args[2], "options should be populated");
            assert.strictEqual(call.args[2].consistency, cql.types.consistencies[consistencyLevel], "options.consistency should be " + consistencyLevel);

            done();
          });
        });

        it("executes cql with default ConsistencyLevel." + consistencyLevel + " if options is null.", function (done) {
          // arrange
          var cqlQuery = "SELECT * FROM users;";
          var params = [];
          var options = null;

          // act
          instance[method](cqlQuery, params, options, function () {
            var call = instance.execCql.getCall(0);

            // assert
            assert.equal(call.args[0], cqlQuery, "cql should be passed through");
            assert.equal(call.args[1], params, "params should be passed through");
            assert.isObject(call.args[2], "options should be populated");
            assert.strictEqual(call.args[2].consistency, cql.types.consistencies[consistencyLevel], "options.consistency should be " + consistencyLevel);

            done();
          });
        });

        it("executes cql with default ConsistencyLevel." + consistencyLevel + " if options not provided.", function (done) {
          // arrange
          var cqlQuery = "SELECT * FROM users;";
          var params = [];

          // act
          instance[method](cqlQuery, params, function () {
            var call = instance.execCql.getCall(0);

            // assert
            assert.equal(call.args[0], cqlQuery, "cql should be passed through");
            assert.equal(call.args[1], params, "params should be passed through");
            assert.isObject(call.args[2], "options should be populated");
            assert.strictEqual(call.args[2].consistency, cql.types.consistencies[consistencyLevel], "options.consistency should be " + consistencyLevel);

            done();
          });
        });

        it("executes cql with provided consistency.", function (done) {
          // arrange
          var cqlQuery = "SELECT * FROM users;";
          var params = [];
          var consistency = cql.types.consistencies.quorum;
          var options = { consistency: consistency };

          // act
          instance[method](cqlQuery, params, options, function () {
            var call = instance.execCql.getCall(0);

            // assert
            assert.equal(call.args[0], cqlQuery, "cql should be passed through");
            assert.equal(call.args[1], params, "params should be passed through");
            assert.isObject(call.args[2], "options should be populated");
            assert.strictEqual(call.args[2].consistency, consistency, "options.consistency should be passed through");

            done();
          });
        });
      });
    }

    validateWrapperCall("select", "one");
    validateWrapperCall("insert", "localQuorum");
    validateWrapperCall("update", "localQuorum");
    validateWrapperCall("delete", "localQuorum");

  });

  describe("NodeCassandraDriver#beginQuery()", function () {

    var instance;
    beforeEach(function () {
      instance = getDefaultInstance();
      sinon.stub(instance, "execCql").yields(null, {});
    });

    afterEach(function () {
      if (instance.execCql.restore) {
        instance.execCql.restore();
      }
    });

    function validateQueryCalls(asPromise) {
      it("#execute() executes cq " +
          (asPromise ? "with promise syntax" : "with callback syntax"),
        function (done) {
          // arrange
          var cqlQuery = "SELECT * FROM users WHERE name = ?;";

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
          }
          else {
            query.execute(function () {
              asserts(done);
            });
          }

          function asserts(done) {
            var call = instance.execCql.getCall(0);

            // assert
            assert.equal(call.args[0], cqlQuery, "cql should be passed through");
            assert.equal(call.args[1], query.context.params, "params should be passed through");
            assert.equal(call.args[2], query.context.options, "options should be passed through");

            done();
          }

        });
    }

    validateQueryCalls(false);
    validateQueryCalls(true);

  });

  describe("NodeCassandraDriver#namedQuery()", function () {

    var instance = getNamedQueryInstance();
    beforeEach(function () {
      sinon.stub(instance, "execCql").yields(null, {});
    });

    afterEach(function () {
      instance.execCql.restore();
    });

    function getNamedQueryInstance() {
      var config = getDefaultConfig();
      config.queryDirectory = path.join(__dirname, "../../stubs/cql");
      return new Driver({ config: config });
    }

    it("executes the CQL specified by the named query", function (done) {
      // arrange
      var queryName = "myFakeCql";
      var params = [];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, {});
      instance.pools = { default: pool };

      // act
      instance.namedQuery(queryName, params, { consistency: consistency }, function () {
        var call = instance.execCql.getCall(0),
          opts = call.args[2];

        // assert
        assert.strictEqual(call.args[0], instance.queryCache.fileCache[queryName], "cql should be read from query cache");
        assert.deepEqual(call.args[1], params, "params should be passed through");
        assert.strictEqual(opts.executeAsPrepared, true, "executeAsPrepared should be set to true");
        assert.strictEqual(opts.queryName, queryName, "queryName should be set to the named query name");

        done();
      });
    });

    it("allows caller to disable prepared statement", function (done) {
      // arrange
      var queryName = "myFakeCql";
      var params = [];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, {});
      instance.pools = { default: pool };

      // act
      instance.namedQuery(queryName, params, { consistency: consistency, executeAsPrepared: false }, function () {
        var call = instance.execCql.getCall(0),
          opts = call.args[2];

        // assert
        assert.strictEqual(opts.executeAsPrepared, false, "executeAsPrepared should be set to true");

        done();
      });
    });

    it("allows callback to be optional to support fire-and-forget scenarios", function (done) {
      // arrange
      var queryName = "myFakeCql";
      var params = [];
      var consistency = cql.types.consistencies.one;
      var pool = getPoolStub(instance.config, true, null, {});
      instance.pools = { default: pool };

      // act/assert
      expect(function () {
        instance.namedQuery(queryName, params, { consistency: consistency });
      }).not.to.throw(Error);

      done();
    });

    it("yields error if named query does not exist", function (done) {
      // arrange
      var queryName = "idontexist";

      // act
      instance.namedQuery(queryName, [], function (error, returnData) {
        // assert
        assert.instanceOf(error, Error, "error is populated");
        assert.isUndefined(returnData, "returnData not defined");

        done();
      });
    });

    it("throws error if queryDirectory not provided in constructor", function (done) {
      // arrange
      var defInstance = getDefaultInstance();
      var queryName = "myFakeCql";

      // act
      expect(function () {
        defInstance.namedQuery(queryName, [], {}, sinon.stub());
      }).to.throw(Error);

      done();
    });

  });
});
