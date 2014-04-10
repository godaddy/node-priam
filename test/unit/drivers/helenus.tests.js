"use strict";

var sinon = require("sinon"),
    chai = require("chai"),
    util = require("util"),
    assert = chai.assert,
    expect = chai.expect,
    FakeResolver = require("../../stubs/fakeResolver"),
    _ = require("lodash"),
    path = require("path");

var helenus = require("helenus");
var Driver = require("../../../lib/drivers/helenus");

describe("lib/drivers/helenus.js", function () {

    function getDefaultLogger() {
        return {
            debug: sinon.stub(),
            info: sinon.stub(),
            warn: sinon.stub(),
            error: sinon.stub(),
            critical: sinon.stub()
        };
    }
    function getDefaultConfig() {
        return {
            hosts: ["123.456.789.012:9160"],
            keyspace: "myKeySpace",
            timeout: 12345
        };
    }
    function getDefaultInstance() {
        var instance = Driver({
            config: getDefaultConfig(),
            logger: getDefaultLogger()
        });
        return instance;
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

    describe("HelenusDriver#constructor", function () {
        it ("should throw exception if context is missing", function () {
            // arrange
            // act
            expect(function () {
                var instance = new Driver();
            })
                // assert
                .to.throw(Error, /missing context /i);
        });

        it ("should throw exception if config is missing from context", function () {
            // arrange
            // act
            expect(function () {
                var instance = new Driver({ });
            })
                // assert
                .to.throw(Error, /missing context.config /i);
        });
        it ("sets default pool configuration", function () {
            // arrange
            var config = _.extend({ }, getDefaultConfig());
            var cqlVersion = "3.0.0";
            var consistencyLevel = helenus.ConsistencyLevel.ONE;

            // act
            var instance = new Driver({ config: config });

            // assert
            assert.deepEqual(instance.poolConfig.hosts, config.hosts, "hosts should be passed through");
            assert.strictEqual(instance.poolConfig.keyspace, config.keyspace, "keyspace should be passed through");
            assert.strictEqual(instance.poolConfig.timeout, config.timeout, "timeout should be passed through");
            assert.strictEqual(instance.poolConfig.cqlVersion, cqlVersion, "cqlVersion should default to 3.0.0");
            assert.strictEqual(instance.poolConfig.consistencyLevel, consistencyLevel, "consistencyLevel should default to ONE");
        });

        it ("should override default pool config with additional store options", function () {
            // arrange
            var config = _.extend({}, getDefaultConfig());
            var cqlVersion = "2.0.0";
            var consistencyLevel = helenus.ConsistencyLevel.ANY;
            config.cqlVersion = cqlVersion;
            config.consistencyLevel = consistencyLevel;

            // act
            var instance = new Driver({ config: config });

            // assert
            assert.deepEqual(instance.poolConfig.hosts, config.hosts, "hosts should be passed through");
            assert.strictEqual(instance.poolConfig.keyspace, config.keyspace, "keyspace should be passed through");
            assert.strictEqual(instance.poolConfig.timeout, config.timeout, "timeout should be passed through");
            assert.strictEqual(instance.poolConfig.cqlVersion, cqlVersion, "cqlVersion should be overridden");
            assert.strictEqual(instance.poolConfig.consistencyLevel, consistencyLevel, "consistencyLevel should be overridden");
        });
    });

    function getPoolStub(config, isReady, err, data) {
        var pool = {
            storeConfig: _.extend({ consistencyLevel: 1, cqlVersion: "3.0.0"}, config),
            isReady: isReady,
            cql: sinon.stub().yields(err, data),
            monitorConnections: sinon.stub(),
            close: sinon.spy(function (callback) {
                if (callback) {
                    process.nextTick(function () {
                        callback(null, null);
                    });
                }
            }),
            once: sinon.spy(function (event, callback) {
                if (callback) {
                    process.nextTick(function () {
                        callback(null, null);
                    });
                }
            })
        };
        return pool;
    }

    describe("HelenusDriver#close()", function () {

        var instance = getDefaultInstance();

        it("closes the connection pool if it exists", function (done) {
            // arrange
            var pool = getPoolStub(instance.config, true, null, {});
            instance.pools = { default: pool };

            // act
            instance.close(function () {
                // assert
                assert.strictEqual(pool.isClosed, true, "pool should be set to closed");
                assert.ok(pool.close.called, "pool close should be called");

                done();
            });
        });

        it("just calls callback if pool does not yet exist", function (done) {
            // arrange
            instance.pools = {};

            // act
            instance.close(function () {
                // assert

                done();
            });
        });

    });

    describe("HelenusDriver#cql()", function () {

        var instance = null,
            fakeResolver = null;

        beforeEach(function () {
            fakeResolver = new FakeResolver();
            instance = getDefaultInstance();
        });

        afterEach(function () {
            if (helenus.ConnectionPool.restore) {
                helenus.ConnectionPool.restore();
            }
        });

        it("creates a new connection pool if one does not exist", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var params = ["param1", "param2", "param3"];
            var consistency = helenus.ConsistencyLevel.ONE;
            var pool = getPoolStub(instance.config, true, null, {});
            pool.on = sinon.stub();
            pool.connect = sinon.stub().yieldsAsync(null, {});
            sinon.stub(helenus, "ConnectionPool").returns(pool);
            instance.pools = {};

            // act
            instance.cql(cql, params, { consistency: consistency }, function () {
                // assert
                assert.equal(instance.pools.default, pool, "pool should be cached");
                assert.strictEqual(pool.waiters.length, 0, "waiters should be executed after connection completes");
                assert.strictEqual(pool.cql.called, true, "cql statements should execute after connection completes");

                done();
            });

            // before callback asserts
            assert.strictEqual(pool.cql.called, false, "cql statements should wait to execute until after connection completes");
        });

        it("creates a new connection pool if config no longer matches", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var params = ["param1", "param2", "param3"];
            var consistency = helenus.ConsistencyLevel.ONE;
            var pool = getPoolStub({ different: "config", keyspace: instance.config.keyspace }, true, null, {});
            pool.on = sinon.stub();
            pool.connect = sinon.stub().yieldsAsync(null, {});
            sinon.stub(helenus, "ConnectionPool").returns(pool);
            var existingPool = getPoolStub({ keyspace: instance.config.keyspace }, true, null, {});
            instance.pools = { default: existingPool };

            // act
            instance.cql(cql, params, { consistency: consistency }, function () {
                // assert
                assert.notEqual(instance.pools.default, existingPool, "existing pool should be replaced");
                assert.equal(instance.pools.default, pool, "pool should be cached");
                assert.strictEqual(pool.waiters.length, 0, "waiters should be executed after connection completes");
                assert.strictEqual(pool.cql.called, true, "cql statements should execute after connection completes");

                done();
            });

            // before callback asserts
            assert.strictEqual(pool.cql.called, false, "cql statements should wait to execute until after connection completes");
        });

        it("creates a new connection pool if pool is closed", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var params = ["param1", "param2", "param3"];
            var consistency = helenus.ConsistencyLevel.ONE;
            var pool = getPoolStub(instance.config, true, null, {});
            pool.on = sinon.stub();
            pool.connect = sinon.stub().yieldsAsync(null, {});
            sinon.stub(helenus, "ConnectionPool").returns(pool);
            var existingPool = getPoolStub(instance.config, true, null, {});
            existingPool.isClosed = true;
            instance.pools = { default: existingPool };

            // act
            instance.cql(cql, params, { consistency: consistency }, function () {
                // assert
                assert.notOk(existingPool.cql.called, "existing pool should not be called");
                assert.ok(pool.cql.called, "new pool should be called");

                done();
            });

            assert.strictEqual(pool.cql.called, false, "cql statements should wait to execute until after connection completes");
        });

        it("adds a new connection pool if keyspace is different", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var params = ["param1", "param2", "param3"];
            var consistency = helenus.ConsistencyLevel.ONE;
            var pool = getPoolStub(_.extend(_.extend({}, instance.config), { keyspace: "myNewKeyspace" }), true, null, {});
            pool.on = sinon.stub();
            pool.connect = sinon.stub().yieldsAsync(null, {});
            sinon.stub(helenus, "ConnectionPool").returns(pool);

            var existingPool = getPoolStub(_.extend({}, instance.config), true, null, {});
            instance.pools = { default: existingPool };

            // act
            instance.cql(cql, params, { consistency: consistency, keyspace: "myNewKeyspace" }, function () {
                // assert
                assert.notOk(existingPool.cql.called, "existing pool should not be called");
                assert.ok(pool.cql.called, "new pool should be called");

                done();
            });
        });

        it("uses default connection pool if supplied keyspace matches default", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var params = ["param1", "param2", "param3"];
            var consistency = helenus.ConsistencyLevel.ONE;
            var pool = getPoolStub(_.extend(_.extend({}, instance.config), { keyspace: instance.config.keyspace }), true, null, {});
            pool.on = sinon.stub();
            pool.connect = sinon.stub().yieldsAsync(null, {});
            sinon.stub(helenus, "ConnectionPool").returns(pool);

            var existingPool = getPoolStub(_.extend({}, instance.config), true, null, {});
            instance.pools = { default: existingPool };

            // act
            instance.cql(cql, params, { consistency: consistency, keyspace: instance.config.keyspace }, function () {
                // assert
                assert.notOk(pool.cql.called, "new pool should not be called");
                assert.ok(existingPool.cql.called, "existing pool should be called");

                done();
            });
        });

        it("sets up a global error handler for the connection pool", function (done) {
            // setup arrange
            var consistency = helenus.ConsistencyLevel.ONE;
            var pool = getPoolStub(instance.config, true, null, {});
            var errorCb = null;
            pool.on = sinon.stub();
            pool.connect = sinon.stub().yieldsAsync(null, {});
            sinon.stub(helenus, "ConnectionPool").returns(pool);
            instance.pools = {};

            // setup act
            instance.cql("cql", [], { consistency: consistency }, function () {
                var ctorCall = helenus.ConnectionPool.getCall(0);

                // setup assert
                assert.ok(pool.on.calledWith("error", sinon.match.func), "error handler should be wired up");

                // handler arrange
                var errorCb = pool.on.getCall(0).args[1];
                var error = new Error("the connection blew up");
                instance.logger = getDefaultLogger();

                // handler act
                errorCb(error);

                // handler assert
                assert.ok(instance.logger.error.calledOnce);

                done();
            });
        });

        it("sets up an error handler for pool.connect", function (done) {
            // arrange
            var consistency = helenus.ConsistencyLevel.ONE;
            var pool = getPoolStub(instance.config, true, null, {});
            var errorCb = null;
            pool.on = sinon.stub();
            pool.connect = sinon.stub().yieldsAsync(new Error("Connection pool failed to connect"));
            sinon.stub(helenus, "ConnectionPool").returns(pool);
            instance.pools = {};
            instance.logger = getDefaultLogger();

            // act
            instance.cql("cql", [], { consistency: consistency }, function (err, result) {
                var ctorCall = helenus.ConnectionPool.getCall(0);

                // assert
                assert.instanceOf(err, Error);
                assert.isUndefined(result);
                assert.ok(instance.logger.error.calledOnce, "error log is called once");

                done();
            });
        });

        it("executes queued queries when connection completes", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var params = ["param1", "param2", "param3"];
            var consistency = helenus.ConsistencyLevel.ONE;
            var pool = getPoolStub(instance.config, true, null, {});
            pool.on = sinon.stub();
            pool.connect = sinon.stub().yieldsAsync(null, "myKeySpace");
            sinon.stub(helenus, "ConnectionPool").returns(pool);
            instance.pools = {};

            // act
            instance.cql(cql, params, { consistency: consistency }, function () {
                var ctorCall = helenus.ConnectionPool.getCall(0);

                // assert
                assert.strictEqual(pool.isReady, true, "pool should be set to true after connection completes");
                assert.strictEqual(pool.cql.called, true, "cql statements should execute after connection completes");

                done();
            });
        });

        it("allows callback to be optional to support fire-and-forget scenarios", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var params = ["param1", "param2", "param3"];
            var consistency = helenus.ConsistencyLevel.ONE;
            var pool = getPoolStub(instance.config, true, null, []);
            instance.pools = { default: pool };

            pool.cql = sinon.spy(function (cql, data, cb) {
                var call = pool.cql.getCall(0);

                // assert
                assert.strictEqual(call.args[0], cql, "cql should be passed through");
                assert.deepEqual(call.args[1], params, "params should be passed through");

                cb(null, []);
                done();
            });

            // act
            instance.cql(cql, params);
        });


        it("uses default consistency of ONE if no options are passed", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var params = ["param1", "param2", "param3"];
            var consistency = helenus.ConsistencyLevel.ONE;
            var pool = getPoolStub(instance.config, true, null, []);
            pool.on = sinon.stub();
            pool.connect = sinon.stub().yieldsAsync(null, "myKeySpace");
            sinon.stub(helenus, "ConnectionPool").returns(pool);
            instance.pools = {};

            // act
            instance.cql(cql, params, function (error, returnData) {
                var ctorCall = helenus.ConnectionPool.getCall(0);

                // assert
                assert.strictEqual(ctorCall.args[0].consistencyLevel, consistency, "consistency should be ONE");

                done();
            });
        });

        it("executes CQL and returns the data", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var params = ["param1", "param2", "param3", new Buffer("param4")];
            var consistency = helenus.ConsistencyLevel.ONE;
            var err = null;
            var data = { field1: "value1" };
            var pool = getPoolStub(instance.config, true, err, data);
            instance.pools = { default: pool };

            // act
            instance.cql(cql, params, { consistency: consistency }, function (error, returnData) {
                var call = pool.cql.getCall(0);

                // assert
                assert.strictEqual(call.args[0], cql, "cql should be passed through");
                assert.deepEqual(call.args[1], params, "params should be passed through");
                assert.isNull(error, "error should be null");
                assert.equal(returnData, data, "data should match cql output");

                done();
            });
        });

        it("converts parameters with a UUID hint to a UUID object", function (done) {
            // arrange
            var cql = "INSERT INTO table (key) VALUES (?)";
            var origUUID = '8bf58bc0-5d52-4066-b521-83e93fc7a2c5';
            var params = [{ value: origUUID, hint: instance.dataType.uuid }];
            var consistency = helenus.ConsistencyLevel.ONE;
            var err = null;
            var data = { field1: "value1" };
            var pool = getPoolStub(instance.config, true, err, data);
            instance.pools = { default: pool };

            // act
            instance.cql(cql, params, { consistency: consistency }, function (error, returnData) {
                var call = pool.cql.getCall(0);
                var passedParams = call.args[1];

                // assert
                assert.instanceOf(passedParams[0], helenus.UUID);

                done();
            });
        });

        it("normalizes/deserializes the data in the resulting array", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var params = ["param1", "param2", "param3"];
            var consistency = helenus.ConsistencyLevel.ONE;
            var err = null;
            var data = [{
                _map: { field1: true, field2: true, field3: true, field4: true, field5: true },
                get: sinon.stub()
            }];
            data[0].get.withArgs("field1").returns({ value: "value1" });
            data[0].get.withArgs("field2").returns({ value: 2 });
            data[0].get.withArgs("field3").returns({ value: "{ \"subField1\": \"blah\" }" });
            data[0].get.withArgs("field4").returns({ value: "[ 4, 3, 2, 1]" });
            data[0].get.withArgs("field5").returns({ value: "{ string that looks like it could be json }" });

            var pool = getPoolStub(instance.config, true, err, data);
            instance.pools = { default: pool };

            // act
            instance.cql(cql, params, { consistency: consistency }, function (error, returnData) {
                var call = pool.cql.getCall(0);

                // assert
                assert.strictEqual(call.args[0], cql, "cql should be passed through");
                assert.deepEqual(call.args[1], params, "params should be passed through");
                assert.isNull(error, "error should be null");
                assert.strictEqual(returnData[0].field1, "value1", "first field should be a string");
                assert.strictEqual(returnData[0].field2, 2, "second field should be a number");
                assert.deepEqual(returnData[0].field3, { subField1: 'blah' }, "third field should be an object");
                assert.deepEqual(returnData[0].field4, [ 4, 3, 2, 1], "fourth field should be an array");
                assert.deepEqual(returnData[0].field5, "{ string that looks like it could be json }", "fourth field should be an array");

                done();
            });
        });

        function testErrorRetry(errorName, errorCode, numRetries, shouldRetry) {
            it((shouldRetry ? "adds" : "does not add") + " error retry if error is '" + errorName + "', code '" + errorCode + "', and retries " + numRetries, function (done) {
                // arrange
                var cql = "MyCqlStatement";
                var params = ["param1", "param2", "param3"];
                var consistency = helenus.ConsistencyLevel.ONE;
                var pool = getPoolStub(instance.config, true, null, {});
                var data = [];
                var callCount = 0;
                pool.cql = sinon.spy(function (c, d, cb) {
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
                instance.cql(cql, params, { consistency: consistency }, function (error, returnData) {
                    // assert
                    if (shouldRetry) {
                        var call1 = pool.cql.getCall(0);
                        var call2 = pool.cql.getCall(1);
                        assert.strictEqual(pool.cql.callCount, 2, "cql should be called twice");
                        assert.notEqual(call1.args[1], call2.args[1], "parameters should be cloned");
                        assert.deepEqual(call1.args[1], call2.args[1], "parameters should be cloned");
                        assert.deepEqual(returnData, data, "data should match cql output");
                    }
                    else {
                        assert.strictEqual(pool.cql.callCount, 1, "cql should be called once");
                    }

                    done();
                });
            });
        }
        testErrorRetry("HelenusPoolError", null, 0, false);
        testErrorRetry("HelenusPoolError", null, 1, true);
        testErrorRetry("HelenusInvalidNameError", null, 1, false);
        testErrorRetry("HelenusInvalidRequestException", null, 1, false);
        testErrorRetry("Error", "ECONNRESET", 1, true);
        testErrorRetry("Error", "ECONNREFUSED", 1, true);
        testErrorRetry("Error", "ENOTFOUND", 1, true);

        it("does not add error retry at consistency QUORUM when original consistency is ALL and enableConsistencyFailover is false", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var params = ["param1", "param2", "param3"];
            var consistency = helenus.ConsistencyLevel.ALL;
            var pool = getPoolStub(instance.config, true, null, {});
            var data = [];
            var callCount = 0;
            pool.cql = sinon.spy(function (c, d, cb) {
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
            instance.cql(cql, params, { consistency: consistency }, function (error, returnData) {
                // assert
                assert.strictEqual(pool.cql.callCount, 1, "cql should be called once");
                assert.ok(error);
                assert.notOk(returnData);

                done();
            });
        });

        it("adds error retry at consistency QUORUM when original consistency is ALL", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var params = ["param1", "param2", "param3"];
            var consistency = helenus.ConsistencyLevel.ALL;
            var pool = getPoolStub(instance.config, true, null, {});
            var data = [];
            var callCount = 0;
            pool.cql = sinon.spy(function (c, d, cb) {
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
            instance.cql(cql, params, { consistency: consistency }, function (error, returnData) {
                var call1 = pool.cql.getCall(0);
                var call2 = pool.cql.getCall(1);
                // assert
                assert.strictEqual(pool.cql.callCount, 2, "cql should be called twice");
                assert.notEqual(call1.args[1], call2.args[1], "parameters should be cloned");
                assert.deepEqual(call1.args[1], call2.args[1], "parameters should be cloned");
                assert.deepEqual(returnData, data, "data should match cql output");

                done();
            });
        });

        it("adds error retry at consistency LOCUM_QUORUM when original consistency is QUORUM", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var params = ["param1", "param2", "param3"];
            var consistency = helenus.ConsistencyLevel.QUORUM;
            var pool = getPoolStub(instance.config, true, null, {});
            var data = [];
            var callCount = 0;
            pool.cql = sinon.spy(function (c, d, cb) {
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
            instance.cql(cql, params, { consistency: consistency }, function (error, returnData) {
                var call1 = pool.cql.getCall(0);
                var call2 = pool.cql.getCall(1);
                // assert
                assert.strictEqual(pool.cql.callCount, 2, "cql should be called twice");
                assert.notEqual(call1.args[1], call2.args[1], "parameters should be cloned");
                assert.deepEqual(call1.args[1], call2.args[1], "parameters should be cloned");
                assert.deepEqual(returnData, data, "data should match cql output");

                done();
            });
        });


        it("captures metrics if metrics and queryName are provided", function (done) {
            // arrange
            var cql = "MyCqlStatement";
            var queryName = "MyQueryName";
            var params = ["param1", "param2", "param3"];
            var consistency = helenus.ConsistencyLevel.ONE;
            var err = null;
            var data = { field1: "value1" };
            var pool = getPoolStub(instance.config, true, err, data);
            instance.pools = { default: pool };
            instance.metrics = {
                measurement: sinon.stub()
            };

            // act
            instance.cql(cql, params, { consistency: consistency, queryName: queryName }, function (error, returnData) {
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
                logger = getDefaultLogger();
                var instance = new Driver(_.extend({ config: getDefaultConfig(), logger: logger }, context));
                return instance;
            }

            beforeEach(function () {
                instance = null;
                fakeResolver.resolveConnection = sinon.stub().yieldsAsync(null, {});
            });


            it("uses supplied connection resolver to override base config", function (done) {
                // arrange
                var cql = "MyCqlStatement";
                var params = ["param1", "param2", "param3"];
                var consistency = helenus.ConsistencyLevel.ONE;
                instance = getResolverInstance({ connectionResolver: fakeResolver });
                var fakeConnectionInfo = {
                    username: "myResolvedUsername",
                    password: "myResolvedPassword",
                    hosts: ["123.456.789.012:1234"]
                };
                fakeResolver.resolveConnection = sinon.stub().yieldsAsync(null, fakeConnectionInfo);
                var pool = getPoolStub(instance.config, true, null, {});
                pool.on = sinon.stub();
                pool.connect = sinon.stub().yieldsAsync(null, {});
                sinon.stub(helenus, "ConnectionPool").returns(pool);
                instance.pools = {};

                // act
                instance.cql(cql, params, { consistency: consistency }, function () {
                    // assert
                    assert.strictEqual(pool.storeConfig.username, fakeConnectionInfo.username);
                    assert.strictEqual(pool.storeConfig.password, fakeConnectionInfo.password);
                    assert.deepEqual(pool.storeConfig.hosts, fakeConnectionInfo.hosts, "hosts successfully updated");

                    done();
                });
            });

            it("uses resolved connection resolver from path to override base config", function (done) {
                // arrange
                var cql = "MyCqlStatement";
                var params = ["param1", "param2", "param3"];
                var consistency = helenus.ConsistencyLevel.ONE;
                instance = getResolverInstance({ config: { connectionResolverPath: "../../test/stubs/fakeResolver" } });
                var fakeConnectionInfo = {
                    username: "myResolvedUsername",
                    password: "myResolvedPassword",
                    hosts: ["123.456.789.012:1234"]
                };
                instance.connectionResolver.resolveConnection = sinon.stub().yieldsAsync(null, fakeConnectionInfo);
                var pool = getPoolStub(instance.config, true, null, {});
                pool.on = sinon.stub();
                pool.connect = sinon.stub().yieldsAsync(null, {});
                sinon.stub(helenus, "ConnectionPool").returns(pool);
                instance.pools = {};

                // act
                instance.cql(cql, params, { consistency: consistency }, function () {
                    // assert
                    assert.strictEqual(pool.storeConfig.username, fakeConnectionInfo.username);
                    assert.strictEqual(pool.storeConfig.password, fakeConnectionInfo.password);
                    assert.deepEqual(pool.storeConfig.hosts, fakeConnectionInfo.hosts, "hosts successfully updated");

                    done();
                });
            });

            it("applies port remapping to resolved connection information if specified", function (done) {
                // arrange
                var cql = "MyCqlStatement";
                var params = ["param1", "param2", "param3"];
                var consistency = helenus.ConsistencyLevel.ONE;
                instance = getResolverInstance({ config: { connectionResolverPath: "../../test/stubs/fakeResolver" } });
                instance.config.connectionResolverPortMap = {
                    from: "1234",
                    to: "2345"
                };
                instance.poolConfig.connectionResolverPortMap = instance.config.connectionResolverPortMap;
                var fakeConnectionInfo = {
                    username: "myResolvedUsername",
                    password: "myResolvedPassword",
                    hosts: ["123.456.789.012:1234", "234.567.890.123"]
                };
                instance.connectionResolver.resolveConnection = sinon.stub().yieldsAsync(null, fakeConnectionInfo);
                var pool = getPoolStub(instance.config, true, null, {});
                pool.on = sinon.stub();
                pool.connect = sinon.stub().yieldsAsync(null, {});
                sinon.stub(helenus, "ConnectionPool").returns(pool);
                instance.pools = {};

                // act
                instance.cql(cql, params, { consistency: consistency }, function () {
                    // assert
                    assert.strictEqual(pool.storeConfig.username, fakeConnectionInfo.username);
                    assert.strictEqual(pool.storeConfig.password, fakeConnectionInfo.password);
                    assert.deepEqual(pool.storeConfig.hosts, ["123.456.789.012:2345", "234.567.890.123"], "hosts were applied with remapped ports");

                    done();
                });
            });

            it("logs and returns error if connection resolver throws error", function (done) {
                // arrange
                var cql = "MyCqlStatement";
                var params = ["param1", "param2", "param3"];
                var consistency = helenus.ConsistencyLevel.ONE;
                instance = getResolverInstance({ connectionResolver: fakeResolver });
                fakeResolver.resolveConnection = sinon.stub().yieldsAsync(new Error("connection resolution failed"));
                var pool = getPoolStub(instance.config, true, null, {});
                pool.on = sinon.stub();
                pool.connect = sinon.stub().yieldsAsync(null, {});
                sinon.stub(helenus, "ConnectionPool").returns(pool);
                instance.pools = {};

                // act
                instance.cql(cql, params, { consistency: consistency }, function (err, result) {
                    // assert
                    assert.instanceOf(err, Error);
                    assert.isUndefined(result);
                    assert.ok(logger.error.calledOnce, "error log is called once");
                    done();
                });
            });

            it("logs error and updates connection if connection resolver returns error AND data", function (done) {
                // arrange
                var cql = "MyCqlStatement";
                var params = ["param1", "param2", "param3"];
                var consistency = helenus.ConsistencyLevel.ONE;
                instance = getResolverInstance({ connectionResolver: fakeResolver });
                var fakeConnectionInfo = {
                    username: "myResolvedUsername",
                    password: "myResolvedPassword",
                    hosts: ["123.456.789.012:1234"]
                };
                fakeResolver.resolveConnection = sinon.stub().yieldsAsync(new Error("connection resolution failed"), fakeConnectionInfo);
                var pool = getPoolStub(instance.config, true, null, {});
                pool.on = sinon.stub();
                pool.connect = sinon.stub().yieldsAsync(null, {});
                sinon.stub(helenus, "ConnectionPool").returns(pool);
                instance.pools = {};

                // act
                instance.cql(cql, params, { consistency: consistency }, function (err, result) {
                    // assert
                    assert.isNull(err);
                    assert.strictEqual(pool.storeConfig.username, fakeConnectionInfo.username, "username successfully updated");
                    assert.strictEqual(pool.storeConfig.password, fakeConnectionInfo.password, "password successfully updated");
                    assert.deepEqual(pool.storeConfig.hosts, fakeConnectionInfo.hosts, "hosts successfully updated");
                    assert.ok(logger.error.called, "error log is called");
                    done();
                });
            });

            it("returns data and logs error if connection resolver throws error on lazy fetch", function (done) {
                // arrange
                var cql = "MyCqlStatement";
                var params = ["param1", "param2", "param3"];
                var consistency = helenus.ConsistencyLevel.ONE;
                instance = getResolverInstance({ connectionResolver: fakeResolver });
                var fakeConnectionInfo = {
                    username: "myResolvedUsername",
                    password: "myResolvedPassword",
                    hosts: ["123.456.789.012:1234"]
                };
                fakeResolver.resolveConnection = function (data, cb, lazyCb) {
                    cb(null, fakeConnectionInfo);
                    fakeResolver.on.getCall(1).args[1](new Error("lazy fetch error"));
                };
                var pool = getPoolStub(instance.config, true, null, {});
                pool.on = sinon.stub();
                pool.connect = sinon.stub().yieldsAsync(null, {});
                sinon.stub(helenus, "ConnectionPool").returns(pool);
                instance.pools = {};

                // act
                instance.cql(cql, params, { consistency: consistency }, function (err, result) {
                    // assert
                    assert.strictEqual(pool.storeConfig.username, fakeConnectionInfo.username, "username successfully updated");
                    assert.strictEqual(pool.storeConfig.password, fakeConnectionInfo.password, "password successfully updated");
                    assert.deepEqual(pool.storeConfig.hosts, fakeConnectionInfo.hosts, "hosts successfully updated");
                    assert.ok(logger.warn.calledOnce, "logger should only be called once");
                    done();
                });
            });

            it("returns data if connection resolver successfully performs a lazy fetch", function (done) {
                // arrange
                var cql = "MyCqlStatement";
                var params = ["param1", "param2", "param3"];
                var consistency = helenus.ConsistencyLevel.ONE;
                instance = getResolverInstance({ connectionResolver: fakeResolver });
                var fakeConnectionInfo = {
                    username: "myResolvedUsername",
                    password: "myResolvedPassword",
                    hosts: ["123.456.789.012:1234"]
                };
                fakeResolver.resolveConnection = function (data, cb, lazyCb) {
                    cb(null, fakeConnectionInfo);
                    fakeResolver.on.getCall(1).args[1](null, { user: "someOtherInfo", password: "someOtherPassword" });
                };
                var pool = getPoolStub(instance.config, true, null, {});
                pool.on = sinon.stub();
                pool.connect = sinon.stub().yieldsAsync(null, {});
                sinon.stub(helenus, "ConnectionPool").returns(pool);
                instance.pools = {};

                // act
                instance.cql(cql, params, { consistency: consistency }, function (err, result) {
                    // assert
                    assert.strictEqual(pool.storeConfig.username, fakeConnectionInfo.username, "username successfully updated");
                    assert.strictEqual(pool.storeConfig.password, fakeConnectionInfo.password, "password successfully updated");
                    assert.deepEqual(pool.storeConfig.hosts, fakeConnectionInfo.hosts, "hosts successfully updated");
                    assert.notOk(logger.warn.called, "warn logger should not be called");
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
            if (instance.execCql.restore) {
                instance.execCql.restore();
            }
        });

        function validateWrapperCall(method, consistencyLevel) {
            describe("HelenusDriver#" + method + "()", function () {
                it("normalizes the parameter list if it is an array", function (done) {
                    // arrange
                    var dt = new Date();
                    var buffer = new Buffer(4096);
                    var hinted = { value: "bar", hint: 1 };
                    buffer.write("This is a string buffer", "utf-8");
                    var params = [ 1, "myString", dt, [1, 2, 3, 4], { myObjectKey: "value"}, buffer, hinted ];

                    // act
                    instance[method]("cql", params, {}, function () {
                        var call = instance.execCql.getCall(0);
                        var normalized = call.args[1];

                        // assert
                        assert.strictEqual(normalized[0], params[0], "1st parameter should be a number");
                        assert.strictEqual(normalized[1], params[1], "2nd parameter should be a string");
                        assert.strictEqual(normalized[2], dt.getTime(), "3rd parameter should be date converted to ticks");
                        assert.strictEqual(normalized[3], "[1,2,3,4]", "4th parameter should be array converted to JSON");
                        assert.strictEqual(normalized[4], "{\"myObjectKey\":\"value\"}", "5th parameter should be object converted to JSON");
                        assert.strictEqual(normalized[5], buffer.toString("hex"), "6th parameter should be buffer converted to hex");
                        assert.strictEqual(normalized[6], hinted.value, "7th parameter should be the hinted object value");

                        done();
                    });
                });

                it("leaves object parameter untouched", function (done) {
                    // arrange
                    var dt = new Date();
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
                    var dt = new Date();
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
                    var cql = "SELECT * FROM users;";
                    var params = [];
                    var options = { suppressDebugLog: true };

                    // act
                    instance[method](cql, params, options, function () {
                        var call = instance.execCql.getCall(0);

                        // assert
                        assert.notOk(instance.logger.debug.calledOnce, "cql is logged");

                        done();
                    });
                });

                it("executes cql with default ConsistencyLevel." + consistencyLevel + " if consistency not provided.", function (done) {
                    // arrange
                    var cql = "SELECT * FROM users;";
                    var params = [];
                    var options = {};

                    // act
                    instance[method](cql, params, options, function () {
                        var call = instance.execCql.getCall(0);

                        // assert
                        assert.ok(instance.logger.debug.calledOnce, "cql is logged");
                        assert.equal(call.args[0], cql, "cql should be passed through");
                        assert.equal(call.args[1], params, "params should be passed through");
                        assert.isObject(call.args[2], "options should be populated");
                        assert.strictEqual(call.args[2].consistency, helenus.ConsistencyLevel[consistencyLevel], "options.consistency should be " + consistencyLevel);

                        done();
                    });
                });

                it("executes cql with default ConsistencyLevel." + consistencyLevel + " if no options are provided.", function (done) {
                    // arrange
                    var cql = "SELECT * FROM users;";
                    var params = [];
                    var options = null;

                    // act
                    instance[method](cql, params, options, function () {
                        var call = instance.execCql.getCall(0);

                        // assert
                        assert.equal(call.args[0], cql, "cql should be passed through");
                        assert.equal(call.args[1], params, "params should be passed through");
                        assert.isObject(call.args[2], "options should be populated");
                        assert.strictEqual(call.args[2].consistency, helenus.ConsistencyLevel[consistencyLevel], "options.consistency should be " + consistencyLevel);

                        done();
                    });
                });

                it("executes cql with default ConsistencyLevel." + consistencyLevel + " if options not provided.", function (done) {
                    // arrange
                    var cql = "SELECT * FROM users;";
                    var params = [];

                    // act
                    instance[method](cql, params, function () {
                        var call = instance.execCql.getCall(0);

                        // assert
                        assert.equal(call.args[0], cql, "cql should be passed through");
                        assert.equal(call.args[1], params, "params should be passed through");
                        assert.isObject(call.args[2], "options should be populated");
                        assert.strictEqual(call.args[2].consistency, helenus.ConsistencyLevel[consistencyLevel], "options.consistency should be " + consistencyLevel);

                        done();
                    });
                });

                it("executes cql with provided consistency.", function (done) {
                    // arrange
                    var cql = "SELECT * FROM users;";
                    var params = [];
                    var consistency = helenus.ConsistencyLevel.QUORUM;
                    var options = { consistency: consistency };

                    // act
                    instance[method](cql, params, options, function () {
                        var call = instance.execCql.getCall(0);

                        // assert
                        assert.equal(call.args[0], cql, "cql should be passed through");
                        assert.equal(call.args[1], params, "params should be passed through");
                        assert.isObject(call.args[2], "options should be populated");
                        assert.strictEqual(call.args[2].consistency, consistency, "options.consistency should be passed through");

                        done();
                    });
                });
            });
        }

        validateWrapperCall("select", "ONE");
        validateWrapperCall("insert", "LOCAL_QUORUM");
        validateWrapperCall("update", "LOCAL_QUORUM");
        validateWrapperCall("delete", "LOCAL_QUORUM");
    });

    describe("HelenusDriver#beginQuery()", function () {

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
                            .then(function (data) {
                                asserts(done);
                            });
                    }
                    else {
                        query.execute(function () {
                            asserts(done);
                        });
                    }

                    function asserts (done) {
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

    describe("HelenusDriver#namedQuery()", function () {

        var instance = getNamedQueryInstance();
        beforeEach(function () {
            sinon.stub(instance, "execCql").yields(null, {});
        });

        afterEach(function () {
            if (instance.execCql.restore) {
                instance.execCql.restore();
            }
        });

        function getNamedQueryInstance() {
            var config = getDefaultConfig();
            config.queryDirectory = path.join(__dirname, "../../stubs/cql");
            var instance = new Driver({
                config: config
            });
            return instance;
        }

        it("executes the CQL specified by the named query", function (done) {
            // arrange
            var queryName = "myFakeCql";
            var params = [];
            var consistency = helenus.ConsistencyLevel.ONE;
            var pool = getPoolStub(instance.config, true, null, {});
            instance.pools = { default: pool };

            // act
            instance.namedQuery(queryName, params, { consistency: consistency }, function (error, returnData) {
                var call = instance.execCql.getCall(0);

                // assert
                assert.strictEqual(call.args[0], instance.queryCache.fileCache[queryName], "cql should be read from query cache");
                assert.deepEqual(call.args[1], params, "params should be passed through");

                done();
            });
        });

        it("allows callback to be optional to support fire-and-forget scenarios", function (done) {
            // arrange
            var queryName = "myFakeCql";
            var params = [];
            var consistency = helenus.ConsistencyLevel.ONE;
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
            var consistency = helenus.ConsistencyLevel.ONE;

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
